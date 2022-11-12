package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	sdk "github.com/elmasy-com/columbus-sdk"
	"github.com/elmasy-com/elnet/domain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {

	mongoUri := flag.String("uri", "", "MongoDB Connection URI")
	contains := flag.String("contains", "", "Check if any domain contains the given string")
	flag.Parse()

	if *mongoUri == "" {
		fmt.Fprintf(os.Stderr, "uri is empty!\n")
		fmt.Printf("Use -help to help\n")
		os.Exit(1)
	}

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(*mongoUri))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to MongoDB: %s\n", err)
		os.Exit(1)
	}
	defer client.Disconnect(context.TODO())

	collection := client.Database("columbus").Collection("domains")

	total, err := collection.CountDocuments(context.TODO(), bson.D{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to count documents: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Total documents: %d\n", total)

	cursor, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to find: %s\n", err)
	}
	defer cursor.Close(context.TODO())

	var result sdk.Domain

	for cursor.Next(context.TODO()) {

		if err := cursor.Decode(&result); err != nil {
			log.Fatal(err)
		}

		d := domain.GetDomain(result.Domain)
		if d == "" {
			fmt.Fprintf(os.Stderr, "Failed to domain.GetDomain(): empty result for %s\n", result.Domain)
			os.Exit(1)
		} else if d != result.Domain {
			fmt.Fprintf(os.Stderr, "Different result after GetDomain() for %s: %s\n", result.Domain, d)
			os.Exit(1)
		}

		if len(result.Subs) == 0 {
			fmt.Printf("%s -> Invalid length of subs: %d\n", result.Domain, len(result.Subs))
		}

		if result.Shard > 0 {
			fmt.Printf("%s -> More than one shard: %d \n", result.Domain, result.Shard)
		}

		if *contains != "" && strings.Contains(result.Domain, *contains) {
			fmt.Printf("%s -> Contains %s\n", result.Domain, *contains)
		}
	}
}
