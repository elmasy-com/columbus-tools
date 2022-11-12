package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	sdk "github.com/elmasy-com/columbus-sdk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	mongoUri := flag.String("uri", "", "Mongo connection URI")
	key := flag.String("key", "", "Columbus API key")
	verbose := flag.Bool("verbose", false, "Print successfuly inserted domains")
	server := flag.String("columbus", "https://columbus.elmasy.com", "Columbus server URI")

	flag.Parse()

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

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(*mongoUri))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to MongoDB: %s\n", err)
		os.Exit(1)
	}
	defer client.Disconnect(context.TODO())

	collection := client.Database("columbus").Collection("domains")

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

	cursor, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to find domains: %s\n", err)
		os.Exit(1)
	}
	defer cursor.Close(context.TODO())

	var d sdk.Domain
	for cursor.Next(context.TODO()) {

		start := time.Now()

		err = cursor.Decode(&d)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to decode domain: %s\n", err)
			os.Exit(1)
		}

		if len(d.Subs) == 0 {
			fmt.Fprintf(os.Stderr, "Domain with empty subs found: %s/%d\n", d.Domain, d.Shard)
			os.Exit(1)
		}

		fmt.Printf("Running %s/%d\r", d.Domain, d.Shard)

		full := d.GetFull()
		for i := range full {

			err = sdk.Insert(full[i])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to insert %s: %s\n", full[i], err)
				os.Exit(1)
			}
		}

		res, err := collection.DeleteOne(context.TODO(), bson.D{{"domain", d.Domain}, {"shard", d.Shard}})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to delete %s/%d: %s\n", d.Domain, d.Shard, err)
			os.Exit(1)
		}
		if res.DeletedCount == 0 {
			fmt.Fprintf(os.Stderr, "Faile to delete %s/%d: nothing deleted\n", d.Domain, d.Shard)
			os.Exit(1)
		}

		if *verbose {
			fmt.Printf("%s/%d inserted successfully in %s\n", d.Domain, d.Shard, time.Since(start).String())
		}
	}
}
