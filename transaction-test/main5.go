package main

// Enhanced Transaction Aggregation with Comprehensive Filtering

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Configuration constants
const (
	MaxWorkers        = 10
	QueueSize         = 100000
	StatsFlushSeconds = 10
)

// Transaction represents a complete transaction record
type Transaction struct {
	ID          string  `json:"id"`
	CardNumber  string  `json:"card_number"`
	Amount      float64 `json:"amount"`
	Timestamp   string  `json:"timestamp"`
	TransactionType        string  `json:"transaction_type"`
	TerminalType string `json:"terminal_type"`
	MTI         string  `json:"mti"`
	TransactionStatus      string  `json:"transaction_status"`
	MerchantID  string  `json:"merchant_id"`
	ActionCode string `json:"action_code"`
}

// FilterConditions defines multiple filtering criteria
type FilterConditions struct {
	TransactionTypes        []string `json:"transaction_types"`
	TerminalTypes []string `json:"terminal_types"`
	MTIs         []string `json:"mtis"`
	Statuses     []string `json:"statuses"`
	MinAmount    float64  `json:"min_amount"`
	MaxAmount    float64  `json:"max_amount"`
}

// CardStats holds complete statistics for filtered and unfiltered transactions
type CardStats struct {
	// Filtered statistics
	FilteredCount uint64  `json:"filtered_count"`
	FilteredSum   float64 `json:"filtered_sum"`
	FilteredMax   float64 `json:"filtered_max"`
	FilteredMin   float64 `json:"filtered_min"`
	FilteredAvg   float64 `json:"filtered_avg"`

	// Unfiltered statistics
	TotalCount uint64  `json:"total_count"`
	TotalSum   float64 `json:"total_sum"`
	TotalMax   float64 `json:"total_max"`
	TotalMin   float64 `json:"total_min"`
	TotalAvg   float64 `json:"total_avg"`

	// Mutex for non-atomic operations
	mu sync.Mutex
}

// Aggregator manages the comprehensive aggregation
type Aggregator struct {
	stats      sync.Map
	queue      chan Transaction
	shutdown   chan struct{}
	wg         sync.WaitGroup
	conditions FilterConditions
}

// NewAggregator creates a new Aggregator with filter conditions
func NewAggregator(conditions FilterConditions) *Aggregator {
	agg := &Aggregator{
		queue:      make(chan Transaction, QueueSize),
		shutdown:   make(chan struct{}),
		conditions: conditions,
	}

	for i := 0; i < MaxWorkers; i++ {
		agg.wg.Add(1)
		go agg.worker()
	}

	go agg.statsFlusher()
	return agg
}

// contains checks if a string exists in a slice
func contains(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

// matchesConditions checks if transaction meets all filter criteria
func (a *Aggregator) matchesConditions(t Transaction) bool {
	// Check type if conditions specify types
	if len(a.conditions.TransactionTypes) > 0 && !contains(a.conditions.TransactionTypes, t.TransactionType) {
		return false
	}

	// Check terminal type if conditions specify terminal types
	if len(a.conditions.TerminalTypes) > 0 && !contains(a.conditions.TerminalTypes, t.TerminalType) {
		return false
	}

	// Check MTI if conditions specify MTIs
	if len(a.conditions.MTIs) > 0 && !contains(a.conditions.MTIs, t.MTI) {
		return false
	}

	// Check status if conditions specify statuses
	// if len(a.conditions.Statuses) > 0 && !contains(a.conditions.Statuses, t.TransactionStatus) {
	// 	return false
	// }

	// Check amount range
	if t.Amount < a.conditions.MinAmount || t.Amount > a.conditions.MaxAmount {
		return false
	}

	return true
}

// worker processes transactions with comprehensive aggregation
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

// processTransaction handles the complete aggregation logic
func (a *Aggregator) processTransaction(t Transaction) {
	value, _ := a.stats.LoadOrStore(t.CardNumber, &CardStats{})
	stats := value.(*CardStats)

	// Always update total statistics
	atomic.AddUint64(&stats.TotalCount, 1)

	stats.mu.Lock()
	stats.TotalSum += t.Amount
	if t.Amount > stats.TotalMax || stats.TotalCount == 1 {
		stats.TotalMax = t.Amount
	}
	if t.Amount < stats.TotalMin || stats.TotalCount == 1 {
		stats.TotalMin = t.Amount
	}
	stats.TotalAvg = stats.TotalSum / float64(stats.TotalCount)
	stats.mu.Unlock()

	// Update filtered statistics if transaction matches conditions
	if a.matchesConditions(t) {
		atomic.AddUint64(&stats.FilteredCount, 1)

		stats.mu.Lock()
		stats.FilteredSum += t.Amount
		if t.Amount > stats.FilteredMax || stats.FilteredCount == 1 {
			stats.FilteredMax = t.Amount
		}
		if t.Amount < stats.FilteredMin || stats.FilteredCount == 1 {
			stats.FilteredMin = t.Amount
		}
		stats.FilteredAvg = stats.FilteredSum / float64(stats.FilteredCount)
		stats.mu.Unlock()
	}
}

// statsFlusher periodically logs statistics
func (a *Aggregator) statsFlusher() {
	ticker := time.NewTicker(StatsFlushSeconds * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var totalTxns, filteredTxns uint64
			a.stats.Range(func(_, value interface{}) bool {
				stats := value.(*CardStats)
				totalTxns += atomic.LoadUint64(&stats.TotalCount)
				filteredTxns += atomic.LoadUint64(&stats.FilteredCount)
				return true
			})
			log.Printf("Stats: Total=%d, Filtered=%d", totalTxns, filteredTxns)
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

// GetStats returns complete statistics
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

// GetFilteredStats returns only filtered statistics
func (a *Aggregator) GetFilteredStats() map[string]CardStats {
	result := make(map[string]CardStats)

	a.stats.Range(func(key, value interface{}) bool {
		card := key.(string)
		stats := value.(*CardStats)
		result[card] = CardStats{
			FilteredCount: stats.FilteredCount,
			FilteredSum:   stats.FilteredSum,
			FilteredMax:   stats.FilteredMax,
			FilteredMin:   stats.FilteredMin,
			FilteredAvg:   stats.FilteredAvg,
		}
		return true
	})

	return result
}

// Shutdown gracefully stops the aggregator
func (a *Aggregator) Shutdown() {
	close(a.shutdown)
	a.wg.Wait()
}

// Server handles HTTP requests
type Server struct {
	aggregator *Aggregator
}

// NewServer creates a new Server instance
func NewServer(aggregator *Aggregator) *Server {
	return &Server{
		aggregator: aggregator,
	}
}

// handleTransaction processes incoming transactions
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

// handleGetStats returns complete statistics
func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := s.aggregator.GetStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// handleGetFilteredStats returns only filtered statistics
func (s *Server) handleGetFilteredStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := s.aggregator.GetFilteredStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func main() {
	// Define comprehensive filter conditions
	conditions := FilterConditions{
		TransactionTypes:        []string{"01", "02"},
		TerminalTypes: []string{"14", "15"},
		MTIs:         []string{"200", "210"},
		Statuses:     []string{"approved", "completed"},
		MinAmount:    0.01,
		MaxAmount:    10000.00,
	}

	// Initialize components with conditions
	aggregator := NewAggregator(conditions)
	defer aggregator.Shutdown()
	server := NewServer(aggregator)

	// Configure HTTP server
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      http.DefaultServeMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	// Set up routes
	http.HandleFunc("/transaction", server.handleTransaction)
	http.HandleFunc("/stats", server.handleGetStats)
	http.HandleFunc("/stats/filtered", server.handleGetFilteredStats)

	// Start server
	log.Printf("Server starting with conditions: %+v", conditions)
	log.Fatal(srv.ListenAndServe())
}