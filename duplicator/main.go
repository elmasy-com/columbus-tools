package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"

	sdk "github.com/elmasy-com/columbus-sdk"
	"github.com/elmasy-com/columbus-sdk/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var NumWorker = runtime.NumCPU() * 2
var DomainChan chan sdk.Domain
var Duplicates []sdk.Domain
var WorkerCancel context.CancelFunc

func DuplicateWorker(wg *sync.WaitGroup, n int) {

	defer log.Printf("Duplicate Worker #%02d stopped!\n", n)
	defer wg.Done()

	for d := range DomainChan {

		n, err := db.Domains.CountDocuments(context.TODO(), bson.M{"domain": d.Domain, "tld": d.TLD, "sub": d.Sub})
		if err != nil {
			log.Printf("Failed to count %#v: %s\n", d, err)
			WorkerCancel()
			return
		}

		if n == 1 {
			continue
		}

		if n == 0 {
			log.Printf("Invalid count for %#v: %d\n", d, n)
			WorkerCancel()
			return
		}

		if n > 1 {
			log.Printf("Duplicate found: %#v\n", d)
			Duplicates = append(Duplicates, d)

		}
	}
}

func main() {

	if len(os.Args) != 2 {
		log.Printf("Usage: %s <mongo-uri>\n", os.Args[0])
		os.Exit(1)
	}

	DomainChan = make(chan sdk.Domain, 10000)

	index := 0
	wg := new(sync.WaitGroup)
	sigs := make(chan os.Signal, 1)
	ctx, cancel := context.WithCancel(context.TODO())
	WorkerCancel = cancel
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	for i := 0; i < NumWorker; i++ {
		wg.Add(1)
		go DuplicateWorker(wg, i)
		log.Printf("Duplicate Worker #%d started!\n", i)
	}

	log.Printf("Connecting to MongoDB...\n")

	err := db.Connect(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %s\n", err)
		os.Exit(1)
	}
	defer db.Disconnect()

	query := bson.M{}

	cursor, err := db.Domains.Find(context.TODO(), query, options.Find().SetBatchSize(100000))
	if err != nil {
		log.Printf("Failed to find: %s\n", err)
		os.Exit(1)
	}
	defer cursor.Close(context.TODO())

	log.Printf("Reading domains...\n")

cursorLoop:
	for cursor.Next(context.TODO()) {

		select {
		case <-sigs:
			break cursorLoop
		case <-ctx.Done():
			break cursorLoop
		default:

			var d sdk.Domain

			if index%10000000 == 0 {
				log.Printf("Reading %d domain...\n", index)
			}

			err = cursor.Decode(&d)
			if err != nil {
				log.Printf("Failed to decode domain: %s\n", err)
				WorkerCancel()
				continue
			}

			DomainChan <- d
			index++

		}
	}

	err = cursor.Err()
	if err != nil {
		log.Printf("Cursor failed: %s\n", err)
		//os.Exit(1)
	}

	close(DomainChan)
	wg.Wait()

	log.Printf("Checked %d domains\n", index)
	log.Printf("Found %d dupliacates\n", len(Duplicates))

	// Delete duplicates
	for i := range Duplicates {

		log.Printf("Removing %s...\n", Duplicates[i].String())

		_, err := db.Domains.DeleteMany(context.TODO(), bson.M{"domain": Duplicates[i].Domain, "tld": Duplicates[i].TLD, "sub": Duplicates[i].Sub})
		if err != nil {
			log.Printf("Failed to delete %#v: %s\n", Duplicates[i], err)
			break
		}

		err = db.Insert(Duplicates[i].String())
		if err != nil {
			log.Printf("Failed to reinsert %#v: %s\n", Duplicates[i], err)
			break
		}

		n, err := db.Domains.CountDocuments(context.TODO(), bson.M{"domain": Duplicates[i].Domain, "tld": Duplicates[i].TLD, "sub": Duplicates[i].Sub})
		if err != nil {
			log.Printf("Failed to count %#v: %s\n", Duplicates[i], err)
			break
		}

		if n != 1 {
			log.Printf("Failed to remove %#v: %s\n", Duplicates[i], err)
			break
		}
	}
}
