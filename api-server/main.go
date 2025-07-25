// mock_api_server.go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Transaction struct {
	ID         string  `json:"id"`
	CardNumber string  `json:"card_number"`
	Amount     float64 `json:"amount"`
	Timestamp  string  `json:"timestamp"`
}

var transactions = []Transaction{
	{ID: "1", CardNumber: "1234-5678-9012-3456", Amount: 100.50, Timestamp: time.Now().Format(time.RFC3339)},
	{ID: "2", CardNumber: "1234-5678-9012-3456", Amount: 75.25, Timestamp: time.Now().Format(time.RFC3339)},
	{ID: "3", CardNumber: "9876-5432-1098-7654", Amount: 200.00, Timestamp: time.Now().Format(time.RFC3339)},
	{ID: "4", CardNumber: "1234-5678-9012-3456", Amount: 50.75, Timestamp: time.Now().Format(time.RFC3339)},
	{ID: "5", CardNumber: "9876-5432-1098-7654", Amount: 150.00, Timestamp: time.Now().Format(time.RFC3339)},
}

func transactionsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(transactions)
}

func main() {
	http.HandleFunc("/transactions", transactionsHandler)
	fmt.Println("Mock API server running on http://localhost:8080")
		http.ListenAndServe(":8080", nil)
}
