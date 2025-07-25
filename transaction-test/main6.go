package main

// Enhanced Transaction Aggregation with Dynamic Rules

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

// Configuration constants
const (
	MaxWorkers        = 10
	QueueSize         = 100000
	StatsFlushSeconds = 10
	ConfigFile        = "aggregation_rules.json"
)

// Transaction represents a complete transaction record
type Transaction struct {
	ActionCode        string                 `json:"action_code"`
	ID                string                 `json:"id"`
	CardNumber        string                 `json:"card_number"`
	Amount            float64                `json:"amount"`
	Timestamp         string                 `json:"timestamp"`
	TransactionType   string                 `json:"transaction_type"`
	TerminalType      string                 `json:"terminal_type"`
	MTI               string                 `json:"mti"`
	TransactionStatus string                 `json:"transaction_status"`
	MerchantID        string                 `json:"merchant_id"`
	// Additional        map[string]interface{} `json:"additional"` // For dynamic fields
}

// Condition represents a single filter condition
type Condition struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"` // "eq", "neq", "gt", "gte", "lt", "lte", "in", "nin"
	Value    interface{} `json:"value"`
}

// AggregationRule defines a dynamic aggregation rule
type AggregationRule struct {
	Name        string      `json:"name"`
	GroupBy     string      `json:"group_by"`  // Field to group by (e.g., "card_number")
	AggType     string      `json:"agg_type"`  // "sum", "count", "avg", "max", "min", "distinct_count"
	AggField    string      `json:"agg_field"` // Field to aggregate (e.g., "amount")
	Conditions  []Condition `json:"conditions"`
	Description string      `json:"description"`
}

// AggregationResult holds the result of a dynamic aggregation
type AggregationResult struct {
	Key   interface{} `json:"key"`
	Value interface{} `json:"value"`
}

// AggregationStats holds all statistics for a rule
type AggregationStats struct {
	Results map[interface{}]interface{} // Grouped results
	mu      sync.RWMutex
}

type avgTracker struct {
	sum   float64
	count int
}

type distinctTracker struct {
	values map[interface{}]struct{}
}

type CardStats struct {
	CardNumber        string                 `json:"card_number"`
	Sum               float64                `json:"sum"`
	Count             int                    `json:"count"`
	Avg               float64                `json:"avg"`
	Min               float64                `json:"min"`
	Max               float64                `json:"max"`
	DistinctMerchants int                    `json:"distinct_merchants"`
	// Additional        map[string]interface{} `json:"additional"` // For dynamic fields
}

// Aggregator manages all dynamic aggregations
type Aggregator struct {
	rules       map[string]*AggregationStats // rule name -> stats
	queue       chan Transaction
	shutdown    chan struct{}
	wg          sync.WaitGroup
	config      []AggregationRule
	cardStats   map[string]*CardStats
	cardStatsMu sync.RWMutex
}

// NewAggregator creates a new dynamic aggregator
func NewAggregator(configFile string) (*Aggregator, error) {
	// Load configuration from file
	config, err := loadConfig(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %v", err)
	}

	agg := &Aggregator{
		rules:     make(map[string]*AggregationStats),
		queue:     make(chan Transaction, QueueSize),
		shutdown:  make(chan struct{}),
		config:    config,
		cardStats: make(map[string]*CardStats),
	}

	// Initialize rules
	for _, rule := range config {
		agg.rules[rule.Name] = &AggregationStats{
			Results: make(map[interface{}]interface{}),
		}
	}

	// Start worker pool
	for i := 0; i < MaxWorkers; i++ {
		agg.wg.Add(1)
		go agg.worker()
	}

	go agg.statsFlusher()
	return agg, nil
}

// loadConfig reads aggregation rules from file
func loadConfig(filename string) ([]AggregationRule, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var rules []AggregationRule
	if err := json.Unmarshal(bytes, &rules); err != nil {
		return nil, err
	}

	return rules, nil
}

// evaluateCondition checks if a transaction meets a condition
func evaluateCondition(t Transaction, cond Condition) bool {
	var fieldValue interface{}

	// Get field value from transaction
	switch cond.Field {
	case "transaction_type":
		fieldValue = t.TransactionType
	case "terminal_type":
		fieldValue = t.TerminalType
	case "mti":
		fieldValue = t.MTI
	case "transaction_status":
		fieldValue = t.TransactionStatus
	case "amount":
		fieldValue = t.Amount
	case "card_number":
		fieldValue = t.CardNumber
	case "merchant_id":
		fieldValue = t.MerchantID
	default:
		return false
		// Check additional fields
		// if val, ok := t.Additional[cond.Field]; ok {
		// 	fieldValue = val
		// } else {
		// 	return false
		// }
	}

	// Evaluate condition based on operator
	switch cond.Operator {
	case "eq":
		return fieldValue == cond.Value
	case "neq":
		return fieldValue != cond.Value
	case "gt":
		if f, ok := fieldValue.(float64); ok {
			if v, ok := cond.Value.(float64); ok {
				return f > v
			}
		}
	case "gte":
		if f, ok := fieldValue.(float64); ok {
			if v, ok := cond.Value.(float64); ok {
				return f >= v
			}
		}
	case "lt":
		if f, ok := fieldValue.(float64); ok {
			if v, ok := cond.Value.(float64); ok {
				return f < v
			}
		}
	case "lte":
		if f, ok := fieldValue.(float64); ok {
			if v, ok := cond.Value.(float64); ok {
				return f <= v
			}
		}
	case "in":
		if values, ok := cond.Value.([]interface{}); ok {
			for _, v := range values {
				if v == fieldValue {
					return true
				}
			}
		}
	case "nin":
		if values, ok := cond.Value.([]interface{}); ok {
			for _, v := range values {
				if v == fieldValue {
					return false
				}
			}
			return true
		}
	}

	return false
}

// matchesAllConditions checks if transaction meets all conditions
func matchesAllConditions(t Transaction, conditions []Condition) bool {
	for _, cond := range conditions {
		if !evaluateCondition(t, cond) {
			return false
		}
	}
	return true
}

// getGroupKey extracts the group by field value
func getGroupKey(t Transaction, field string) interface{} {
	switch field {
	case "transaction_type":
		return t.TransactionType
	case "terminal_type":
		return t.TerminalType
	case "mti":
		return t.MTI
	case "transaction_status":
		return t.TransactionStatus
	case "amount":
		return t.Amount
	case "card_number":
		return t.CardNumber
	case "merchant_id":
		return t.MerchantID
	default:
		return nil
		// if val, ok := t.Additional[field]; ok {
		// 	return val
		// }
		// return nil
	}
}

// getAggFieldValue extracts the field value to aggregate
func getAggFieldValue(t Transaction, field string) float64 {
	if field == "amount" {
		return t.Amount
	}
	// For other numeric fields in Additional
	// if val, ok := t.Additional[field].(float64); ok {
	// 	return val
	// }
	// return 0
	return 0
}

// worker processes transactions with dynamic aggregation
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

// processTransaction applies all aggregation rules to a transaction
func (a *Aggregator) processTransaction(t Transaction) {
	matchedCardRule := false
	for _, rule := range a.config {
		if !matchesAllConditions(t, rule.Conditions) {
			continue
		}

		groupKey := getGroupKey(t, rule.GroupBy)
		if groupKey == nil {
			continue
		}

		aggValue := getAggFieldValue(t, rule.AggField)
		stats := a.rules[rule.Name]

		stats.mu.Lock()
		defer stats.mu.Unlock()

		switch rule.AggType {
		case "sum":
			if current, ok := stats.Results[groupKey].(float64); ok {
				stats.Results[groupKey] = current + aggValue
			} else {
				stats.Results[groupKey] = aggValue
			}
		case "count":
			if current, ok := stats.Results[groupKey].(int); ok {
				stats.Results[groupKey] = current + 1
			} else {
				stats.Results[groupKey] = 1
			}
		case "avg":
			if current, ok := stats.Results[groupKey].(avgTracker); ok {
				current.sum += aggValue
				current.count++
				stats.Results[groupKey] = current
			} else {
				stats.Results[groupKey] = avgTracker{
					sum:   aggValue,
					count: 1,
				}
			}
		case "max":
			if current, ok := stats.Results[groupKey].(float64); ok {
				if aggValue > current {
					stats.Results[groupKey] = aggValue
				}
			} else {
				stats.Results[groupKey] = aggValue
			}
		case "min":
			if current, ok := stats.Results[groupKey].(float64); ok {
				if aggValue < current {
					stats.Results[groupKey] = aggValue
				}
			} else {
				stats.Results[groupKey] = aggValue
			}
		case "distinct_count":
			if current, ok := stats.Results[groupKey].(distinctTracker); ok {
				current.values[aggValue] = struct{}{}
			} else {
				stats.Results[groupKey] = distinctTracker{
					values: map[interface{}]struct{}{aggValue: {}},
				}
			}
		}

		// If this rule groups by card_number, mark for per-card aggregation
		if rule.GroupBy == "card_number" && groupKey == t.CardNumber {
			matchedCardRule = true
		}
	}

	if matchedCardRule {
		a.cardStatsMu.Lock()
		defer a.cardStatsMu.Unlock()
		cs, exists := a.cardStats[t.CardNumber]
		if !exists {
			cs = &CardStats{
				CardNumber: t.CardNumber,
				Min:        t.Amount,
				Max:        t.Amount,
			}
			a.cardStats[t.CardNumber] = cs
		}
		cs.Sum += t.Amount
		cs.Count++
		if t.Amount < cs.Min {
			cs.Min = t.Amount
		}
		if t.Amount > cs.Max {
			cs.Max = t.Amount
		}
		cs.Avg = cs.Sum / float64(cs.Count)
		cs.DistinctMerchants = 1
		// Track distinct merchants for this card	
		// if csMerchants, ok := cs.Additional["merchants"].(map[string]struct{}); ok {
		// 	csMerchants[t.MerchantID] = struct{}{}
		// 	cs.Additional["merchants"] = csMerchants
		// 	cs.DistinctMerchants = len(csMerchants)
		// } else {
		// 	csMerchants := map[string]struct{}{t.MerchantID: {}}
		// 	if cs.Additional == nil {
		// 		cs.Additional = make(map[string]interface{})
		// 	}
		// 	cs.Additional["merchants"] = csMerchants
		// 	cs.DistinctMerchants = 1
		// }
	}
}

// statsFlusher periodically logs statistics
func (a *Aggregator) statsFlusher() {
	ticker := time.NewTicker(StatsFlushSeconds * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for ruleName, stats := range a.rules {
				stats.mu.RLock()
				count := len(stats.Results)
				stats.mu.RUnlock()
				if count > 0 {
					log.Printf("Rule '%s': %d groups", ruleName, count)
				}
			}
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

// GetResults returns aggregation results for a specific rule
func (a *Aggregator) GetResults(ruleName string) ([]AggregationResult, error) {
	stats, exists := a.rules[ruleName]
	if !exists {
		return nil, fmt.Errorf("rule not found")
	}

	stats.mu.RLock()
	defer stats.mu.RUnlock()

	results := make([]AggregationResult, 0, len(stats.Results))
	for key, value := range stats.Results {
		// Handle special aggregation types
		switch v := value.(type) {
		case avgTracker:
			results = append(results, AggregationResult{
				Key:   key,
				Value: v.sum / float64(v.count),
			})
		case distinctTracker:
			results = append(results, AggregationResult{
				Key:   key,
				Value: len(v.values),
			})
		default:
			results = append(results, AggregationResult{
				Key:   key,
				Value: value,
			})
		}
	}

	return results, nil
}

// GetAllRules returns all configured aggregation rules
func (a *Aggregator) GetAllRules() []AggregationRule {
	return a.config
}

func (a *Aggregator) GetAllCardStats() []CardStats {
	a.cardStatsMu.RLock()
	defer a.cardStatsMu.RUnlock()
	stats := make([]CardStats, 0, len(a.cardStats))
	for _, cs := range a.cardStats {
		stats = append(stats, *cs)
	}
	return stats
}

func (a *Aggregator) GetCardStats(cardID string) (*CardStats, bool) {
	a.cardStatsMu.RLock()
	defer a.cardStatsMu.RUnlock()
	cs, exists := a.cardStats[cardID]
	return cs, exists
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

// handleGetResults returns aggregation results
func (s *Server) handleGetResults(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ruleName := r.URL.Query().Get("rule")
	if ruleName == "" {
		http.Error(w, "Rule name is required", http.StatusBadRequest)
		return
	}

	results, err := s.aggregator.GetResults(ruleName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// handleGetRules returns all configured rules
func (s *Server) handleGetRules(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rules := s.aggregator.GetAllRules()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rules)
}

func (s *Server) handleGetAllCardStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	stats := s.aggregator.GetAllCardStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleGetCardStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cardID := r.URL.Query().Get("card_id")
	if cardID == "" {
		http.Error(w, "Missing card_id parameter", http.StatusBadRequest)
		return
	}
	cs, exists := s.aggregator.GetCardStats(cardID)
	if !exists {
		http.Error(w, "Card not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cs)
}

func main() {
	// Initialize dynamic aggregator
	aggregator, err := NewAggregator(ConfigFile)
	if err != nil {
		log.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown()

	// Create and start server
	server := NewServer(aggregator)
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      http.DefaultServeMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	// Set up routes
	http.HandleFunc("/transaction", server.handleTransaction)
	http.HandleFunc("/results", server.handleGetResults)
	http.HandleFunc("/rules", server.handleGetRules)
	http.HandleFunc("/card_aggs", server.handleGetAllCardStats)
	http.HandleFunc("/card_stats", server.handleGetCardStats)

	// Start server
	log.Printf("Server starting with dynamic aggregation rules")
	log.Fatal(srv.ListenAndServe())
}
