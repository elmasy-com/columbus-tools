package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	sdk "github.com/elmasy-com/columbus-sdk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	verbose    bool
	client     *mongo.Client
	collection *mongo.Collection
	domainChan chan sdk.Domain
)

func insertWorker(wg *sync.WaitGroup) {

	defer wg.Done()

	for d := range domainChan {

		if verbose {
			fmt.Printf("Inserting %s/%d...\n", d.Domain, d.Shard)
		}

		start := time.Now()

		full := d.GetFull()
		for i := range full {

			err := sdk.Insert(full[i])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to insert %s: %s\n", full[i], err)
				os.Exit(1)
			}
		}

		res, err := collection.DeleteOne(context.TODO(), bson.D{{Key: "domain", Value: d.Domain}, {Key: "shard", Value: d.Shard}})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to delete %s/%d: %s\n", d.Domain, d.Shard, err)
			os.Exit(1)
		}
		if res.DeletedCount == 0 {
			fmt.Fprintf(os.Stderr, "Faile to delete %s/%d: nothing deleted\n", d.Domain, d.Shard)
			os.Exit(1)
		}

		if verbose {
			fmt.Printf("%s/%d inserted successfully in %s\n", d.Domain, d.Shard, time.Since(start).String())
		}
	}
}

func main() {
	mongoUri := flag.String("uri", "", "Mongo connection URI")
	key := flag.String("key", "", "Columbus API key")
	flag.BoolVar(&verbose, "verbose", false, "Print successfuly inserted domains")
	workers := flag.Int("workers", runtime.NumCPU(), "Number of workers to insert")
	server := flag.String("columbus", "https://columbus.elmasy.com", "Columbus server URI")

	flag.Parse()

	wg := sync.WaitGroup{}

	if *mongoUri == "" {
		fmt.Fprintf(os.Stderr, "FAIL: uri is missing!\n")
		fmt.Printf("Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
	if *key == "" {
		fmt.Fprintf(os.Stderr, "key is missing!")
		fmt.Printf("Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
	if *workers <= 0 {
		fmt.Fprintf(os.Stderr, "Invalid number of workers: %d\n", *workers)
		fmt.Printf("Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	fmt.Printf("Starting at %s...\n", time.Now().Format(time.Stamp))

	domainChan = make(chan sdk.Domain, 256)
	var err error

	client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI(*mongoUri))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to MongoDB: %s\n", err)
		os.Exit(1)
	}
	defer client.Disconnect(context.TODO())

	collection = client.Database("columbus").Collection("domains")

	domainNum, err := collection.CountDocuments(context.TODO(), bson.D{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to count domains: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Domains in the database: %d\n", domainNum)

	sdk.SetURI(*server)

	err = sdk.GetDefaultUser(*key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get Columbus user: %s\n", err)
		os.Exit(1)
	}

	for numw := 0; numw < *workers; numw++ {
		wg.Add(1)
		go insertWorker(&wg)
	}

	cursor, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to find domains: %s\n", err)
		os.Exit(1)
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {

		var d sdk.Domain

		err = cursor.Decode(&d)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to decode domain: %s\n", err)
			os.Exit(1)
		}

		if len(d.Subs) == 0 {
			fmt.Fprintf(os.Stderr, "Domain with empty subs found: %s/%d\n", d.Domain, d.Shard)
			os.Exit(1)
		}

		domainChan <- d
	}

	wg.Wait()

	if err := cursor.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Cursor error: %s\n", err)
		os.Exit(2)
	}
}
