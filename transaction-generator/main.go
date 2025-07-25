package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Transaction struct {
	ID                string `json:"id"`
	CardNumber        string `json:"card_number"`
	Amount            int    `json:"amount"`
	Timestamp         string `json:"timestamp"`
	TransactionType   string `json:"transaction_type"`
	TerminalType      string `json:"terminal_type"`
	MerchantID        string `json:"merchant_id"`
	TransactionStatus string `json:"transaction_status"`
	MTI               string `json:"mti"`
	ActionCode        string `json:"action_code"`
}

const (
	transactionsPerSecond = 10000
	workers               = 200 // Number of concurrent workers
)

var (
	successCount uint64
	failureCount uint64
	totalToSend  int
)

func main() {
	// Get total number of transactions from user input
	fmt.Print("Enter total number of transactions to send: ")
	_, err := fmt.Scan(&totalToSend)
	if err != nil {
		fmt.Println("Error reading input:", err)
		return
	}

	if totalToSend <= 0 {
		fmt.Println("Number of transactions must be positive")
		return
	}

	// Setup graceful shutdown
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// Create worker pool
	jobs := make(chan int, transactionsPerSecond)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker(jobs, &wg)
	}

	// Start monitoring
	go monitor()

	// Start generating transactions
	fmt.Printf("Sending %d transactions at %d/sec...\n", totalToSend, transactionsPerSecond)
	ticker := time.NewTicker(time.Second / time.Duration(transactionsPerSecond))
	defer ticker.Stop()

	counter := 1
	sent := 0
	for sent < totalToSend {
		select {
		case <-ticker.C:
			select {
			case jobs <- counter:
				counter++
				sent++
			default:
				// Queue full, increment failure count
				atomic.AddUint64(&failureCount, 1)
			}
		case <-interrupt:
			fmt.Println("\nShutting down gracefully...")
			close(jobs)
			wg.Wait()
			printFinalStats()
			return
		}
	}

	// All transactions sent, wait for completion
	close(jobs)
	wg.Wait()
	printFinalStats()
}

func worker(jobs <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 5 * time.Second,
	}

	for id := range jobs {
		sendTransaction(client, id)
	}
}

func sendTransaction(client *http.Client, id int) {
	// Randomly select TransactionType as "01", "02", "03", or "04"
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	transactionTypes := []string{"01", "02", "03", "04"}
	transactionType := transactionTypes[r.Intn(len(transactionTypes))]

	// Use math/rand for better randomization of TerminalType
	r = rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	terminalTypes := []string{"14", "15", "16", "17"}
	terminalType := terminalTypes[r.Intn(len(terminalTypes))]

	// Randomly select MTI as "200" or "100"
	r = rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	mtiTypes := []string{"200", "100"}
	mti := mtiTypes[r.Intn(len(mtiTypes))]

	// Randomly select MCC
	r = rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	mccTypes := []string{"5411", "5412", "5413", "5414", "5415", "5416", "5417", "5418", "5419", "5420"}
	mcc := mccTypes[r.Intn(len(mccTypes))]

	transaction := Transaction{
		ID:                strconv.Itoa(id),
		CardNumber:        generateCardNumber(id),
		Amount:            (id%9 + 1) * 1000,
		Timestamp:         time.Now().UTC().Format(time.RFC3339),
		TransactionType:   transactionType,
		TerminalType:      terminalType,
		MerchantID:        mcc,
		MTI:               mti,
		ActionCode:        "00",
		TransactionStatus: "success",
		// CardType: "debit",
		// CardBrand: "visa",
		// CardIssuer: "bank",
		// CardCountry: "US",
		// CardCurrency: "USD",
		// CardExpiry: "12/2025",
	}

	jsonData, err := json.Marshal(transaction)
	fmt.Println(string(jsonData))
	if err != nil {
		atomic.AddUint64(&failureCount, 1)
		return
	}

	resp, err := client.Post("http://localhost:8080/transaction", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		atomic.AddUint64(&failureCount, 1)
		return
	}
	defer resp.Body.Close()

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		atomic.AddUint64(&failureCount, 1)
		return
	}

	atomic.AddUint64(&successCount, 1)
}

func generateCardNumber(i int) string {
	base := "8989-5678-9012-"
	lastFour := fmt.Sprintf("%04d", i%10000)
	return base + lastFour
}

func monitor() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		success := atomic.LoadUint64(&successCount)
		failure := atomic.LoadUint64(&failureCount)
		total := success + failure
		var rate float64
		if total > 0 {
			rate = float64(success) / float64(total) * 100
		}

		// remaining := totalToSend - int(total)
		fmt.Printf("Progress: %d/%d (%.1f%%) | Success: %d | Failures: %d | Rate: %.2f%%\n",
			total, totalToSend, float64(total)/float64(totalToSend)*100,
			success, failure, rate)

		if total >= uint64(totalToSend) {
			return
		}
	}
}

func printFinalStats() {
	success := atomic.LoadUint64(&successCount)
	failure := atomic.LoadUint64(&failureCount)
	total := success + failure
	var rate float64
	if total > 0 {
		rate = float64(success) / float64(total) * 100
	}

	fmt.Printf("\nFinal Stats:\n")
	fmt.Printf("Requested Transactions: %d\n", totalToSend)
	fmt.Printf("Attempted Transactions: %d\n", total)
	fmt.Printf("Successful: %d\n", success)
	fmt.Printf("Failed: %d\n", failure)
	fmt.Printf("Success Rate: %.2f%%\n", rate)
}
