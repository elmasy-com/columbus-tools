package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	sdk "github.com/elmasy-com/columbus-sdk"
	"github.com/elmasy-com/columbus-sdk/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func worker(dc <-chan *sdk.Domain, ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup) {

	defer wg.Done()

	for d := range dc {

		select {
		case <-ctx.Done():
			return
		default:

			_, err := db.UniqueTlds.UpdateOne(context.TODO(), bson.M{"tld": d.TLD}, bson.M{"$setOnInsert": bson.M{"tld": d.TLD}}, options.Update().SetUpsert(true))
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to update TLD %s: %s", d.TLD, err)
				cancel()
				return
			}

			_, err = db.UniqueDomains.UpdateOne(context.TODO(), bson.M{"domain": d.Domain}, bson.M{"$setOnInsert": bson.M{"domain": d.Domain}}, options.Update().SetUpsert(true))
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to update domain %s: %s", d.Domain, err)
				cancel()
				return
			}

			_, err = db.UniqueFullDomains.UpdateOne(context.TODO(), bson.M{"domain": d.FullDomain()}, bson.M{"$setOnInsert": bson.M{"domain": d.FullDomain()}}, options.Update().SetUpsert(true))
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to update full domain %s: %s", d.Domain, err)
				cancel()
				return
			}

			if d.Sub == "" {
				// Do not insert empty subdomain into uniqueSubs
				continue
			}

			_, err = db.UniqueSubs.UpdateOne(context.TODO(), bson.M{"sub": d.Sub}, bson.M{"$setOnInsert": bson.M{"sub": d.Sub}}, options.Update().SetUpsert(true))
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to update subdomain %s: %s", d.TLD, err)
				cancel()
				return
			}
		}
	}
}

func main() {

	if os.Getenv("COLUMBUS_MONGO_URI") == "" {
		fmt.Fprintf(os.Stderr, "Set COLUMBUS_MONGO_URI to connect to the database!\n")
		os.Exit(1)
	}

	err := db.Connect(os.Getenv("COLUMBUS_MONGO_URI"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to MongoDB: %s\n", err)
		os.Exit(1)
	}
	defer db.Disconnect()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT)
	defer cancel()

	cursor, err := db.Domains.Find(ctx, bson.M{}, options.Find().SetBatchSize(10000))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to find(): %s\n", err)
		os.Exit(1)
	}
	defer cursor.Close(ctx)

	index := 0

	wg := new(sync.WaitGroup)
	dc := make(chan *sdk.Domain)

	for i := 0; i < 40; i++ {
		wg.Add(1)
		go worker(dc, ctx, cancel, wg)
	}

cursorLoop:
	for cursor.Next(ctx) {

		if index%1000000 == 0 {
			fmt.Printf("Parsing %d...\n", index)
		}

		index++

		select {
		case <-ctx.Done():
			break cursorLoop
		default:

			d := new(sdk.Domain)

			err = cursor.Decode(d)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to decode domain: %s\n", err)
				cancel()
				break cursorLoop
			}

			dc <- d
		}
	}

	err = cursor.Err()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cursor error: %s\n", err)
	}

	close(dc)

	wg.Wait()
}
