package main

// Simple Transaction Aggregation with Running Total

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

// Aggregator manages the aggregation of transactions by card number
type Aggregator struct {
	mu     sync.Mutex
	totals map[string]float64 // card number -> running total
}

// NewAggregator creates a new Aggregator instance
func NewAggregator() *Aggregator {
	return &Aggregator{
		totals: make(map[string]float64),
	}
}

// AddTransaction processes a transaction and updates the running total
func (a *Aggregator) AddTransaction(t Transaction) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.totals[t.CardNumber] += t.Amount
}

// GetTotals returns the current aggregation totals
func (a *Aggregator) GetTotals() map[string]float64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Create a copy of the map to avoid external modifications
	result := make(map[string]float64)
	for k, v := range a.totals {
		result[k] = v
	}
	return result
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
	fmt.Println("Transaction data decoded", t)
	if err != nil {
		http.Error(w, "Invalid transaction data", http.StatusBadRequest)
		return
	}

	s.aggregator.AddTransaction(t)
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "Transaction processed for card %s\n", t.CardNumber)
}

// handleGetTotals returns the current aggregation totals
func (s *Server) handleGetTotals(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	totals := s.aggregator.GetTotals()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(totals)
}

func main() {
	// Initialize components
	aggregator := NewAggregator()
	server := NewServer(aggregator)

	// Set up HTTP routes
	http.HandleFunc("/transaction", server.handleTransaction)
	http.HandleFunc("/totals", server.handleGetTotals)

	// Start the server
	port := ":8080"
	log.Printf("Server starting on port %s", port)
	log.Fatal(http.ListenAndServe(port, nil))
}
