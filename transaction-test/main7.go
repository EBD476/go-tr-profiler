package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/dimiro1/banner"
	"github.com/fatih/color"
)

// Configuration constants
const (
	MaxWorkers        = 100
	QueueSize         = 100000
	StatsFlushSeconds = 10
	ConfigFile        = "aggregation_rules2.json"
)

// Transaction represents a complete transaction record
type Transaction struct {
	ID           string                 `json:"id"`
	CardNumber   string                 `json:"card_number"`
	Amount       float64                `json:"amount"`
	Timestamp    string                 `json:"timestamp"`
	Type         string                 `json:"type"`
	TerminalType string                 `json:"terminal_type"`
	MTI          string                 `json:"mti"`
	Status       string                 `json:"status"`
	MerchantID   string                 `json:"merchant_id"`
	Additional   map[string]interface{} `json:"additional"`
}

// Condition represents a condition with logical operators
type Condition struct {
	Operator   string      `json:"operator"` // "field", "and", "or", "not"
	Field      string      `json:"field,omitempty"`
	Comparison string      `json:"comparison,omitempty"` // "eq", "neq", "gt", etc.
	Value      interface{} `json:"value,omitempty"`
	Conditions []Condition `json:"conditions,omitempty"`
}

// AggregationRule defines a dynamic aggregation rule
type AggregationRule struct {
	Name        string    `json:"name"`
	GroupBy     string    `json:"group_by"`
	AggType     string    `json:"agg_type"`
	AggField    string    `json:"agg_field"`
	Condition   Condition `json:"condition"`
	Description string    `json:"description"`
}

// AggregationResult holds the result of a dynamic aggregation
type AggregationResult struct {
	Key   interface{} `json:"key"`
	Value interface{} `json:"value"`
}

// AggregationStats holds all statistics for a rule
type AggregationStats struct {
	Results map[interface{}]interface{}
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
	rules    map[string]*AggregationStats
	queue    chan Transaction
	shutdown chan struct{}
	wg       sync.WaitGroup
	config   []AggregationRule
	cardStats   map[string]*CardStats
	cardStatsMu sync.RWMutex
}

func NewAggregator(configFile string) (*Aggregator, error) {
	config, err := loadConfig(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %v", err)
	}

	agg := &Aggregator{
		rules:    make(map[string]*AggregationStats),
		queue:    make(chan Transaction, QueueSize),
		shutdown: make(chan struct{}),
		config:   config,
		cardStats: make(map[string]*CardStats),
	}

	for _, rule := range config {
		agg.rules[rule.Name] = &AggregationStats{
			Results: make(map[interface{}]interface{}),
		}
	}

	for i := 0; i < MaxWorkers; i++ {
		agg.wg.Add(1)
		go agg.worker()
	}

	go agg.statsFlusher()
	return agg, nil
}

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

func getFieldValue(t Transaction, field string) interface{} {
	switch field {
	case "type":
		return t.Type
	case "terminal_type":
		return t.TerminalType
	case "mti":
		return t.MTI
	case "status":
		return t.Status
	case "amount":
		return t.Amount
	case "card_number":
		return t.CardNumber
	case "merchant_id":
		return t.MerchantID
	case "timestamp":
		return t.Timestamp
	default:
		if val, ok := t.Additional[field]; ok {
			return val
		}
		return nil
	}
}

func compareValues(fieldValue interface{}, comparison string, conditionValue interface{}) bool {
	switch comparison {
	case "eq":
		return fieldValue == conditionValue
	case "neq":
		return fieldValue != conditionValue
	case "gt":
		if f, ok := fieldValue.(float64); ok {
			if v, ok := conditionValue.(float64); ok {
				return f > v
			}
		}
	case "gte":
		if f, ok := fieldValue.(float64); ok {
			if v, ok := conditionValue.(float64); ok {
				return f >= v
			}
		}
	case "lt":
		if f, ok := fieldValue.(float64); ok {
			if v, ok := conditionValue.(float64); ok {
				return f < v
			}
		}
	case "lte":
		if f, ok := fieldValue.(float64); ok {
			if v, ok := conditionValue.(float64); ok {
				return f <= v
			}
		}
	case "in":
		if values, ok := conditionValue.([]interface{}); ok {
			for _, v := range values {
				if v == fieldValue {
					return true
				}
			}
		}
	case "nin":
		if values, ok := conditionValue.([]interface{}); ok {
			for _, v := range values {
				if v == fieldValue {
					return false
				}
			}
			return true
		}
	case "contains":
		if str, ok := fieldValue.(string); ok {
			if substr, ok := conditionValue.(string); ok {
				return contains(str, substr)
			}
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr
}

func evaluateCondition(t Transaction, cond Condition) bool {
	switch cond.Operator {
	case "field":
		fieldValue := getFieldValue(t, cond.Field)
		return compareValues(fieldValue, cond.Comparison, cond.Value)
	case "and":
		for _, c := range cond.Conditions {
			if !evaluateCondition(t, c) {
				return false
			}
		}
		return true
	case "or":
		for _, c := range cond.Conditions {
			if evaluateCondition(t, c) {
				return true
			}
		}
		return len(cond.Conditions) == 0
	case "not":
		return !evaluateCondition(t, cond.Conditions[0])

	default:
		return false
	}
}

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

func (a *Aggregator) processTransaction(t Transaction) {
	matchedCardRule := false
	for _, rule := range a.config {
		if !evaluateCondition(t, rule.Condition) {
			continue
		}

		groupKey := getFieldValue(t, rule.GroupBy)
		if groupKey == nil {
			continue
		}

		aggValue := getFieldValue(t, rule.AggField)
		if aggValue == nil {
			continue
		}

		stats := a.rules[rule.Name]
		stats.mu.Lock()
		defer stats.mu.Unlock()

		switch rule.AggType {
		case "sum":
			if f, ok := aggValue.(float64); ok {
				if current, ok := stats.Results[groupKey].(float64); ok {
					stats.Results[groupKey] = current + f
				} else {
					stats.Results[groupKey] = f
				}
			}
		case "count":
			if current, ok := stats.Results[groupKey].(int); ok {
				stats.Results[groupKey] = current + 1
			} else {
				stats.Results[groupKey] = 1
			}
		case "avg":
			var current avgTracker
			if existing, ok := stats.Results[groupKey].(avgTracker); ok {
				current = existing
			}
			if f, ok := aggValue.(float64); ok {
				current.sum += f
				current.count++
				stats.Results[groupKey] = current
			}
		case "max":
			if f, ok := aggValue.(float64); ok {
				if current, ok := stats.Results[groupKey].(float64); ok {
					if f > current {
						stats.Results[groupKey] = f
					}
				} else {
					stats.Results[groupKey] = f
				}
			}
		case "min":
			if f, ok := aggValue.(float64); ok {
				if current, ok := stats.Results[groupKey].(float64); ok {
					if f < current {
						stats.Results[groupKey] = f
					}
				} else {
					stats.Results[groupKey] = f
				}
			}
		case "distinct_count":
			var current distinctTracker
			if existing, ok := stats.Results[groupKey].(distinctTracker); ok {
				current = existing
			} else {
				current.values = make(map[interface{}]struct{})
			}
			current.values[aggValue] = struct{}{}
			stats.Results[groupKey] = current
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
	}
}

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

func (a *Aggregator) AddTransaction(t Transaction) error {
	select {
	case a.queue <- t:
		return nil
	default:
		return fmt.Errorf("transaction queue full")
	}
}

func (a *Aggregator) GetResults(ruleName string) ([]AggregationResult, error) {
	stats, exists := a.rules[ruleName]
	if !exists {
		return nil, fmt.Errorf("rule not found")
	}

	stats.mu.RLock()
	defer stats.mu.RUnlock()

	results := make([]AggregationResult, 0, len(stats.Results))
	for key, value := range stats.Results {
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


func (a *Aggregator) Shutdown() {
	close(a.shutdown)
	a.wg.Wait()
}

type Server struct {
	aggregator *Aggregator
}

func NewServer(aggregator *Aggregator) *Server {
	return &Server{
		aggregator: aggregator,
	}
}

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

	// Banner
	c := color.New(color.FgCyan).Add(color.Underline)
	file, err := os.Open("banner.txt")
	if err != nil {
		log.Fatalf("Failed to open banner file: %v", err)
	}
	defer file.Close()

	banner.Init(os.Stdout, true, true, file)
	c.Println("Prints cyan text with an underline.")

	aggregator, err := NewAggregator(ConfigFile)
	if err != nil {
		log.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown()

	server := NewServer(aggregator)
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      http.DefaultServeMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	http.HandleFunc("/transaction", server.handleTransaction)
	http.HandleFunc("/results", server.handleGetResults)
	http.HandleFunc("/rules", server.handleGetRules)
	http.HandleFunc("/card_aggs", server.handleGetAllCardStats)
	http.HandleFunc("/card_stats", server.handleGetCardStats)

	log.Printf("Server starting with dynamic aggregation rules")
	log.Fatal(srv.ListenAndServe())
}
