package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func getTableStats(client flight.Client, ticketStr string) (float64, int64, float64) {
	start := time.Now()

	ticket := &flight.Ticket{Ticket: []byte(ticketStr)}
	stream, err := client.DoGet(context.Background(), ticket)
	if err != nil {
		log.Fatalf("Greška za %s: %v", ticketStr, err)
	}

	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatalf("Greška pri kreiranju readera za %s: %v", ticketStr, err)
	}
	defer reader.Release()

	var totalSum float64
	var totalRows int64

	valueIdx := -1
	for i, field := range reader.Schema().Fields() {
		if field.Name == "value" {
			valueIdx = i
			break
		}
	}

	if valueIdx == -1 {
		log.Fatalf("Kolona 'value' nije pronađena u datasetu %s", ticketStr)
	}

	for reader.Next() {
		record := reader.Record()
		nRows := record.NumRows()
		totalRows += nRows

		colData := record.Column(valueIdx)
		values := colData.(*array.Float64)

		for i := 0; i < values.Len(); i++ {
			if values.IsValid(i) {
				totalSum += values.Value(i)
			}
		}
	}

	if err := reader.Err(); err != nil {
		log.Fatalf("Greška tokom čitanja strima %s: %v", ticketStr, err)
	}

	duration := time.Since(start).Seconds()
	fmt.Printf("Vreme dobijanja podataka iz %s: %.2f sekundi (Redova: %d)\n", ticketStr, duration, totalRows)

	mean := 0.0
	if totalRows > 0 {
		mean = totalSum / float64(totalRows)
	}
	return totalSum, totalRows, mean
}

func main() {
	const maxMsgSize = 100 * 1024 * 1024

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsgSize)),
	}

	client, err := flight.NewClientWithMiddleware("127.0.0.1:8888", nil, nil, opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	fmt.Println("--- Go Flight Client Analitika ---")

	sumPg, rowsPg, meanPg := getTableStats(client, "postgres")
	sumMg, rowsMg, meanMg := getTableStats(client, "mongo")
	sumDb, rowsDb, meanDb := getTableStats(client, "duckdb")

	fmt.Printf("\nPostgres redova: %d, Prosek: %.2f\n", rowsPg, meanPg)
	fmt.Printf("MongoDB redova: %d, Prosek: %.2f\n", rowsMg, meanMg)
	fmt.Printf("DuckDB redova: %d, Prosek: %.2f\n", rowsDb, meanDb)

	totalRows := rowsPg + rowsMg + rowsDb
	totalSum := sumPg + sumMg + sumDb
	totalMean := totalSum / float64(totalRows)

	fmt.Printf("\nUkupno spojenih redova: %d\n", totalRows)
	fmt.Printf("Prosečna vrednost svih senzora: %.2f\n", totalMean)

	avgOfMeans := (meanPg + meanMg + meanDb) / 3
	fmt.Printf("Prosečna vrednost Postgres + MongoDB + DuckDB senzora: %.2f\n", avgOfMeans)
}
