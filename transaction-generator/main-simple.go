package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"	
	"strconv"
	"time"
)

type Transaction struct {
	ID         string `json:"id"`
	CardNumber string `json:"card_number"`
	Amount     int    `json:"amount"`
	Timestamp  string `json:"timestamp"`
}

func main() {
	// Get number of transactions from user input
	fmt.Print("Enter number of transactions to send: ")
	var input string
	_, err := fmt.Scanln(&input)
	if err != nil {
		fmt.Println("Error reading input:", err)
		return
	}

	numTransactions, err := strconv.Atoi(input)
	if err != nil {
		fmt.Println("Invalid number:", err)
		return
	}

	if numTransactions <= 0 {
		fmt.Println("Number of transactions must be positive")
		return
	}

	// Send transactions
	for i := 1; i <= numTransactions; i++ {
		// Create transaction
		transaction := Transaction{
			ID:         strconv.Itoa(i),
			CardNumber: generateCardNumber(i),
			Amount:     (i % 9 + 1) * 1000, // Varying amounts between 1000-9000
			Timestamp:  time.Now().UTC().Format(time.RFC3339),
		}

		// Convert to JSON
		jsonData, err := json.Marshal(transaction)
		if err != nil {
			fmt.Printf("Error marshaling transaction %d: %v\n", i, err)
			continue
		}

		// Send HTTP POST request
		resp, err := http.Post("http://localhost:8080/transaction", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Printf("Error sending transaction %d: %v\n", i, err)
			continue
		}
		defer resp.Body.Close()

		// Read response
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("Error reading response for transaction %d: %v\n", i, err)
			continue
		}

		fmt.Printf("Transaction %d sent. Status: %s, Response: %s\n", i, resp.Status, string(body))
	}
}

// Helper function to generate card numbers based on index
func generateCardNumber(i int) string {
	base := "8989-5678-9012-"
	lastFour := fmt.Sprintf("%04d", i%10000)
	return base + lastFour
}	