package main

// Enhanced Transaction Aggregation with Multiple Metrics aggregator

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
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
	Count int     `json:"count"`
	Total float64 `json:"total"`
	Max   float64 `json:"max"`
	Min   float64 `json:"min"`
	Avg   float64 `json:"avg"`
}

// Aggregator manages the aggregation of transactions by card number
type Aggregator struct {
	mu    sync.RWMutex
	stats map[string]*CardStats // card number -> statistics
}

// NewAggregator creates a new Aggregator instance
func NewAggregator() *Aggregator {
	return &Aggregator{
		stats: make(map[string]*CardStats),
	}
}

// AddTransaction processes a transaction and updates all metrics
func (a *Aggregator) AddTransaction(t Transaction) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Get or create stats for this card
	stats, exists := a.stats[t.CardNumber]
	if !exists {
		stats = &CardStats{
			Min: t.Amount,
			Max: t.Amount,
		}
		a.stats[t.CardNumber] = stats
	}

	// Update all metrics
	stats.Count++
	stats.Total += t.Amount

	if t.Amount > stats.Max {
		stats.Max = t.Amount
	}
	if t.Amount < stats.Min {
		stats.Min = t.Amount
	}
	stats.Avg = stats.Total / float64(stats.Count)
}

// GetStats returns the current aggregation statistics for all cards
func (a *Aggregator) GetStats() map[string]CardStats {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Create a copy of the map to avoid external modifications
	result := make(map[string]CardStats)
	for k, v := range a.stats {
		result[k] = *v
	}
	return result
}

// GetCardStats returns statistics for a specific card
func (a *Aggregator) GetCardStats(cardNumber string) (CardStats, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	stats, exists := a.stats[cardNumber]
	if !exists {
		return CardStats{}, false
	}
	return *stats, true
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
	err := json.NewDecoder(r.Body).Decode(&t)
	if err != nil {
		http.Error(w, "Invalid transaction data", http.StatusBadRequest)
		return
	}

	s.aggregator.AddTransaction(t)
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "Transaction processed for card %s\n", t.CardNumber)
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

// handleGetCardStats returns statistics for a specific card
func (s *Server) handleGetCardStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cardNumber := r.URL.Query().Get("card_number")
	if cardNumber == "" {
		http.Error(w, "Card number parameter is required", http.StatusBadRequest)
		return
	}

	stats, exists := s.aggregator.GetCardStats(cardNumber)
	if !exists {
		http.Error(w, "Card not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func main() {
	// Initialize components
	aggregator := NewAggregator()
	server := NewServer(aggregator)

	// Set up HTTP routes
	http.HandleFunc("/transaction", server.handleTransaction)
	http.HandleFunc("/stats", server.handleGetStats)
	http.HandleFunc("/stats/card", server.handleGetCardStats)

	// Start the server
	port := ":8080"
	log.Printf("Server starting on port %s", port)
	log.Fatal(http.ListenAndServe(port, nil))
}

// curl -X POST -H "Content-Type: application/json" -d '{"id": "1", "card_number": "1234-5678-9012-3456", "amount": 100.0, "timestamp": "2021-01-01T00:00:00Z"}' http://localhost:8080/transaction
// curl http://localhost:8080/stats
// curl http://localhost:8080/stats/card?card_number=1234-5678-9012-3456
