package main

// Enhanced Transaction Aggregation with Multiple Metrics aggregator
// Scaling to Handle 10,000 Transactions per Second

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"runtime"
	"runtime/debug"
)

// Configuration constants
const (
	MaxWorkers        = 100    // Number of concurrent processors
	QueueSize         = 100000 // Buffer size for incoming transactions
	StatsFlushSeconds = 1     // How often to flush stats to persistent storage
)

// Transaction represents a single transaction record
type Transaction struct {
	ID         string  `json:"id"`
	CardNumber string  `json:"card_number"`
	Amount     float64 `json:"amount"`
	Timestamp  string  `json:"timestamp"`
}

// CardStats holds all aggregation metrics for a card
type CardStats struct {
	Count uint64  `json:"count"`
	Total float64 `json:"total"`
	Max   float64 `json:"max"`
	Min   float64 `json:"min"`
	Avg   float64 `json:"avg"`
}

// Aggregator manages the aggregation of transactions by card number
type Aggregator struct {
	stats     sync.Map // Using sync.Map for better concurrent read performance
	totalTxns uint64   // Atomic counter for total transactions processed
	queue     chan Transaction
	shutdown  chan struct{}
	wg        sync.WaitGroup
}

// NewAggregator creates a new Aggregator instance
func NewAggregator() *Aggregator {
	agg := &Aggregator{
		queue:    make(chan Transaction, QueueSize),
		shutdown: make(chan struct{}),
	}

	// Start worker pool
	for i := 0; i < MaxWorkers; i++ {
		agg.wg.Add(1)
		go agg.worker()
	}

	// Start stats flusher
	go agg.statsFlusher()

	return agg
}

// worker processes transactions from the queue
func (a *Aggregator) worker() {
	defer a.wg.Done()

	for {
		select {
		case t := <-a.queue:
			a.processTransaction(t)
		case <-a.shutdown:
			return
		}
	}
}

// processTransaction handles the actual aggregation logic
func (a *Aggregator) processTransaction(t Transaction) {
	// Using LoadOrStore pattern for concurrent updates
	value, _ := a.stats.LoadOrStore(t.CardNumber, &CardStats{
		Min: t.Amount,
		Max: t.Amount,
	})
	stats := value.(*CardStats)

	// Using atomic operations for counters
	atomic.AddUint64(&stats.Count, 1)

	// Need mutex for the non-atomic operations
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()

	stats.Total += t.Amount
	if t.Amount > stats.Max {
		stats.Max = t.Amount
	}
	if t.Amount < stats.Min {
		stats.Min = t.Amount
	}
	stats.Avg = stats.Total / float64(stats.Count)

	// Increment total transactions counter
	atomic.AddUint64(&a.totalTxns, 1)
}

// statsFlusher periodically persists statistics
func (a *Aggregator) statsFlusher() {
	ticker := time.NewTicker(StatsFlushSeconds * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// In a real implementation, this would persist to disk/database
			log.Printf("Processed %d transactions total", atomic.LoadUint64(&a.totalTxns))
		case <-a.shutdown:
			return
		}
	}
}

// AddTransaction queues a transaction for processing
func (a *Aggregator) AddTransaction(t Transaction) error {
	select {
	case a.queue <- t:
		return nil
	default:
		return fmt.Errorf("transaction queue full")
	}
}

// GetStats returns the current aggregation statistics for all cards
func (a *Aggregator) GetStats() map[string]CardStats {
	result := make(map[string]CardStats)

	a.stats.Range(func(key, value interface{}) bool {
		card := key.(string)
		stats := *value.(*CardStats)
		result[card] = stats
		return true
	})

	return result
}

// Shutdown gracefully stops the aggregator
func (a *Aggregator) Shutdown() {
	close(a.shutdown)
	a.wg.Wait()
}

// Server handles HTTP requests and processes transactions
type Server struct {
	aggregator *Aggregator
}

// NewServer creates a new Server instance
func NewServer(aggregator *Aggregator) *Server {
	return &Server{
		aggregator: aggregator,
	}
}

// handleTransaction processes incoming transaction POST requests
func (s *Server) handleTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var t Transaction
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		http.Error(w, "Invalid transaction data", http.StatusBadRequest)
		return
	}

	if err := s.aggregator.AddTransaction(t); err != nil {
		http.Error(w, "System busy, please retry", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// handleGetStats returns the current aggregation statistics for all cards
func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := s.aggregator.GetStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func main() {

	maxProcs := runtime.GOMAXPROCS(0) // returns current GOMAXPROCS
	log.Printf("MaxProcs: %d", maxProcs)

	debug.SetMaxThreads(20000)
	// debug.SetGCPercent(10)


	// Initialize components
	aggregator := NewAggregator()
	defer aggregator.Shutdown()
	server := NewServer(aggregator)

	// Set up HTTP routes
	http.HandleFunc("/transaction", server.handleTransaction)
	http.HandleFunc("/stats", server.handleGetStats)

	// Configure HTTP server
	srv := &http.Server{
		Addr:         ":8080",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	// Start the server
	log.Printf("Server starting on port %s", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Could not start server: %v", err)
	}
}
