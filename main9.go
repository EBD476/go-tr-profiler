package main

//enhanced implementation that adds time-based windowing to aggregations, with support for both sliding and tumbling windows:

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// Configuration constants
const (
	MaxWorkers        = 100
	QueueSize         = 100000
	StatsFlushSeconds = 10
	ConfigFile        = "aggregation_rules.json"
)

// TimeWindow defines the windowing configuration
type TimeWindow struct {
	Duration  time.Duration `json:"duration"`   // e.g., "1h", "24h"
	Slide     time.Duration `json:"slide"`      // for sliding windows (optional)
	StartTime time.Time     `json:"start_time"` // When aggregation should begin
}

// AggregationWindowType defines the windowing mode
type AggregationWindowType string

const (
	TumblingWindow AggregationWindowType = "tumbling"
	SlidingWindow  AggregationWindowType = "sliding"
	CollectorMode  AggregationWindowType = "collector"
)

// AggregationRule now includes windowing config
type AggregationRule struct {
	Name        string                `json:"name"`
	GroupBy     string                `json:"group_by"`
	AggType     string                `json:"agg_type"`
	AggField    string                `json:"agg_field"`
	Condition   Condition             `json:"condition"`
	WindowType  AggregationWindowType `json:"window_type"`
	Window      TimeWindow            `json:"window"`
	Description string                `json:"description"`
}

// WindowedAggregation tracks aggregations within time windows
type WindowedAggregation struct {
	Current   interface{} // Current window aggregation
	Previous  interface{} // Previous window aggregation (for sliding)
	WindowEnd time.Time   // When current window ends
	mu        sync.RWMutex
}

// Aggregator now manages windowed aggregations
type Aggregator struct {
	rules         map[string]*AggregationStats
	windowedRules map[string]*WindowedAggregation
	queue         chan Transaction
	shutdown      chan struct{}
	wg            sync.WaitGroup
	config        []AggregationRule
}

// NewAggregator initializes with windowed aggregation support
func NewAggregator(configFile string) (*Aggregator, error) {
	config, err := loadConfig(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %v", err)
	}

	agg := &Aggregator{
		rules:         make(map[string]*AggregationStats),
		windowedRules: make(map[string]*WindowedAggregation),
		queue:         make(chan Transaction, QueueSize),
		shutdown:      make(chan struct{}),
		config:        config,
	}

	// Initialize rules
	for _, rule := range config {
		if rule.WindowType != "" {
			// Initialize windowed aggregation
			agg.windowedRules[rule.Name] = &WindowedAggregation{
				WindowEnd: rule.Window.StartTime.Add(rule.Window.Duration),
			}
		} else {
			// Regular aggregation
			agg.rules[rule.Name] = &AggregationStats{
				Results: make(map[interface{}]interface{}),
			}
		}
	}

	// Start worker pool
	for i := 0; i < MaxWorkers; i++ {
		agg.wg.Add(1)
		go agg.worker()
	}

	// Start window manager
	go agg.windowManager()

	// Start stats flusher
	go agg.statsFlusher()

	return agg, nil
}

// windowManager handles window rotations
func (a *Aggregator) windowManager() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			for ruleName, rule := range a.config {
				if rule.WindowType == "" {
					continue
				}

				windowAgg := a.windowedRules[ruleName]
				windowAgg.mu.Lock()

				// Check if window should advance
				if now.After(windowAgg.WindowEnd) {
					switch rule.WindowType {
					case TumblingWindow:
						// For tumbling windows, reset completely
						windowAgg.Previous = windowAgg.Current
						windowAgg.Current = nil
						windowAgg.WindowEnd = windowAgg.WindowEnd.Add(rule.Window.Duration)
					case SlidingWindow:
						// For sliding windows, advance by slide duration
						windowAgg.Previous = windowAgg.Current
						windowAgg.Current = nil
						windowAgg.WindowEnd = windowAgg.WindowEnd.Add(rule.Window.Slide)
					case CollectorMode:
						// Collector just keeps growing until reset
						windowAgg.WindowEnd = windowAgg.WindowEnd.Add(rule.Window.Duration)
					}
				}
				windowAgg.mu.Unlock()
			}

		case <-a.shutdown:
			return
		}
	}
}

// processTransaction handles windowed aggregations
func (a *Aggregator) processTransaction(t Transaction) {
	txnTime, err := time.Parse(time.RFC3339, t.Timestamp)
	if err != nil {
		log.Printf("Invalid timestamp: %v", err)
		return
	}

	for _, rule := range a.config {
		if !evaluateCondition(t, rule.Condition) {
			continue
		}

		// Handle windowed aggregations
		if rule.WindowType != "" {
			windowAgg := a.windowedRules[rule.Name]
			windowAgg.mu.Lock()

			// Check if transaction falls within current window
			if txnTime.After(rule.Window.StartTime) && txnTime.Before(windowAgg.WindowEnd) {
				a.updateWindowedAggregation(windowAgg, t, rule)
			}
			windowAgg.mu.Unlock()
			continue
		}

		// Handle non-windowed aggregations (original logic)
		// ... [previous non-windowed processing logic] ...
	}
}

// updateWindowedAggregation updates window-specific aggregations
func (a *Aggregator) updateWindowedAggregation(agg *WindowedAggregation, t Transaction, rule AggregationRule) {
	groupKey := getFieldValue(t, rule.GroupBy)
	if groupKey == nil {
		return
	}

	aggValue := getFieldValue(t, rule.AggField)
	if aggValue == nil {
		return
	}

	// Initialize current window data structure if needed
	if agg.Current == nil {
		agg.Current = make(map[interface{}]interface{})
	}

	currentWindow := agg.Current.(map[interface{}]interface{})

	switch rule.AggType {
	case "sum":
		if f, ok := aggValue.(float64); ok {
			if current, ok := currentWindow[groupKey].(float64); ok {
				currentWindow[groupKey] = current + f
			} else {
				currentWindow[groupKey] = f
			}
		}
	case "count":
		if current, ok := currentWindow[groupKey].(int); ok {
			currentWindow[groupKey] = current + 1
		} else {
			currentWindow[groupKey] = 1
		}
		// ... other aggregation types ...
	}
}

// GetWindowedResults returns results for windowed aggregations
func (a *Aggregator) GetWindowedResults(ruleName string) (current, previous interface{}, err error) {
	agg, exists := a.windowedRules[ruleName]
	if !exists {
		return nil, nil, fmt.Errorf("windowed rule not found")
	}

	agg.mu.RLock()
	defer agg.mu.RUnlock()

	return agg.Current, agg.Previous, nil
}

// Server methods for windowed aggregations
func (s *Server) handleWindowedResults(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ruleName := r.URL.Query().Get("rule")
	if ruleName == "" {
		http.Error(w, "Rule name is required", http.StatusBadRequest)
		return
	}

	current, previous, err := s.aggregator.GetWindowedResults(ruleName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	response := map[string]interface{}{
		"current":  current,
		"previous": previous,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// ... [rest of the implementation remains similar] ...

//GET /windowed_results?rule=hourly_transactions
