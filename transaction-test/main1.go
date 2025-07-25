package main

// Simple Transaction Aggregation with File Persistence

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
)

// Transaction represents a single transaction
type Transaction struct {
	ID         string  `json:"id"`
	CardNumber string  `json:"card_number"`
	Amount     float64 `json:"amount"`
	Timestamp  string  `json:"timestamp"`
}

// CardAggregate holds aggregated data for a card
type CardAggregate struct {
	CardNumber string  `json:"card_number"`
	Sum        float64 `json:"sum"`
	Max        float64 `json:"max"`
	Min        float64 `json:"min"`
	Count      int     `json:"count"`
}

// Storage interface defines how we store and retrieve aggregates
type Storage interface {
	UpdateAggregate(transaction Transaction) error
	GetAggregate(cardNumber string) (CardAggregate, error)
	SaveToFile(filename string) error
	LoadFromFile(filename string) error
}

// FileStorage implements Storage using an in-memory map and file persistence
type FileStorage struct {
	aggregates map[string]CardAggregate
	mu         sync.RWMutex
}

// NewFileStorage creates a new FileStorage instance
func NewFileStorage() *FileStorage {
	return &FileStorage{
		aggregates: make(map[string]CardAggregate),
	}
}

// UpdateAggregate updates the aggregates for a card based on a new transaction
func (fs *FileStorage) UpdateAggregate(transaction Transaction) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	agg, exists := fs.aggregates[transaction.CardNumber]
	if !exists {
		agg = CardAggregate{
			CardNumber: transaction.CardNumber,
			Max:        transaction.Amount,
			Min:        transaction.Amount,
		}
	}

	agg.Sum += transaction.Amount
	agg.Count++
	if transaction.Amount > agg.Max {
		agg.Max = transaction.Amount
	}
	if transaction.Amount < agg.Min {
		agg.Min = transaction.Amount
	}

	fs.aggregates[transaction.CardNumber] = agg
	return nil
}

// GetAggregate retrieves aggregates for a specific card
func (fs *FileStorage) GetAggregate(cardNumber string) (CardAggregate, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	agg, exists := fs.aggregates[cardNumber]
	if !exists {
		return CardAggregate{}, fmt.Errorf("card number %s not found", cardNumber)
	}

	return agg, nil
}

// SaveToFile saves all aggregates to a JSON file
func (fs *FileStorage) SaveToFile(filename string) error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(fs.aggregates); err != nil {
		return fmt.Errorf("error encoding aggregates: %v", err)
	}

	return nil
}

// LoadFromFile loads aggregates from a JSON file
func (fs *FileStorage) LoadFromFile(filename string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet, that's okay
			return nil
		}
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("error reading file: %v", err)
	}

	if len(data) == 0 {
		// Empty file, that's okay
		return nil
	}

	if err := json.Unmarshal(data, &fs.aggregates); err != nil {
		return fmt.Errorf("error decoding aggregates: %v", err)
	}

	return nil
}

// APIClient handles fetching transactions from the REST API
type APIClient struct {
	baseURL string
}

// NewAPIClient creates a new APIClient instance
func NewAPIClient(baseURL string) *APIClient {
	return &APIClient{baseURL: baseURL}
}

// FetchTransactions retrieves transactions from the API
func (ac *APIClient) FetchTransactions() ([]Transaction, error) {
	resp, err := http.Get(ac.baseURL + "/transactions")
	if err != nil {
		return nil, fmt.Errorf("error fetching transactions: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var transactions []Transaction
	if err := json.NewDecoder(resp.Body).Decode(&transactions); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	return transactions, nil
}

// AggregationService handles the business logic
type AggregationService struct {
	storage   Storage
	apiClient *APIClient
}

// NewAggregationService creates a new AggregationService instance
func NewAggregationService(storage Storage, apiClient *APIClient) *AggregationService {
	return &AggregationService{
		storage:   storage,
		apiClient: apiClient,
	}
}

// ProcessTransactions fetches transactions and updates aggregates
func (as *AggregationService) ProcessTransactions() error {
	transactions, err := as.apiClient.FetchTransactions()
	fmt.Println("Transactions fetched", transactions)
	if err != nil {
		return err
	}

	for _, transaction := range transactions {
		if err := as.storage.UpdateAggregate(transaction); err != nil {
			return fmt.Errorf("error updating aggregate for transaction %s: %v", transaction.ID, err)
		}
	}

	return nil
}

// GetCardAggregate retrieves and displays aggregate data for a card
func (as *AggregationService) GetCardAggregate(cardNumber string) error {
	agg, err := as.storage.GetAggregate(cardNumber)
	if err != nil {
		return err
	}

	fmt.Printf("Aggregate data for card %s:\n", cardNumber)
	fmt.Printf("  Transaction count: %d\n", agg.Count)
	fmt.Printf("  Total amount: %.2f\n", agg.Sum)
	fmt.Printf("  Maximum amount: %.2f\n", agg.Max)
	fmt.Printf("  Minimum amount: %.2f\n", agg.Min)
	fmt.Printf("  Average amount: %.2f\n", agg.Sum/float64(agg.Count))

	return nil
}

// Add this handler function
func (as *AggregationService) AddTransactionHandler(w http.ResponseWriter, r *http.Request) {

	fmt.Println("AddTransactionHandler called")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var transaction Transaction
	if err := json.NewDecoder(r.Body).Decode(&transaction); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := as.storage.UpdateAggregate(transaction); err != nil {
		http.Error(w, fmt.Sprintf("Error processing transaction: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Println("Transaction processed")

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "transaction processed"})
}

func main1() {
	// Initialize components
	storage := NewFileStorage()

	apiClient := NewAPIClient("http://localhost:8080") // Point to our mock server
	service := NewAggregationService(storage, apiClient)

	// Load existing aggregates from file
	const dataFile = "aggregates.json"
	if err := storage.LoadFromFile(dataFile); err != nil {
		fmt.Printf("Warning: could not load data file: %v\n", err)
	}

	// Set up HTTP server to receive transactions
	http.HandleFunc("/transaction", service.AddTransactionHandler)
	go func() {
		fmt.Println("Transaction receiver running on http://localhost:8081")
		http.ListenAndServe(":8081", nil)
	}()

	// Process new transactions
	if err := service.ProcessTransactions(); err != nil {
		fmt.Printf("Error processing transactions: %v\n", err)
	}

	// Save aggregates to file
	if err := storage.SaveToFile(dataFile); err != nil {
		fmt.Printf("Error saving aggregates: %v\n", err)
	}

	// Example: Query aggregates for a specific card
	// cardNumber := "1234-5678-9012-3456" // Replace with actual card number
	// if err := service.GetCardAggregate(cardNumber); err != nil {
	// fmt.Printf("Error getting aggregate: %v\n", err)
	// }

	select {}
}
