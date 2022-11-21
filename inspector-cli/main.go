package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	sdk "github.com/elmasy-com/columbus-sdk"
	"github.com/elmasy-com/elnet/domain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {

	mongoUri := flag.String("uri", "", "MongoDB Connection URI")
	contains := flag.String("contains", "", "Check if any domain contains the given string")
	size := flag.Int("size", -1, "Show domains with subs size >= than the value. If value is -1, disables this check.")

	totalDomains := 0
	totalSubs := 0
	totalInvalidSubs := 0
	totalContains := 0
	totalSizeDomain := 0

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

	cursor, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to find: %s\n", err)
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {

		var result sdk.Domain

		if err := cursor.Decode(&result); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to decode domain: %s", err)
			os.Exit(1)
		}

		totalDomains++

		d := domain.GetDomain(result.Domain)
		if d == "" {
			fmt.Fprintf(os.Stderr, "INVALIDDOM  -> Failed to domain.GetDomain(): empty result for %s\n", result.Domain)
			continue
		} else if d != result.Domain {
			fmt.Fprintf(os.Stderr, "INVALIDDOM  -> Different result after GetDomain() for %s: %s\n", result.Domain, d)
			continue
		}

		l := len(result.Subs)

		if l == 0 {
			fmt.Printf("INVALIDLEN  -> Invalid length of subs for %s: %d\n", result.Domain, len(result.Subs))
			continue
		}

		if *size != -1 && l >= *size {
			fmt.Printf("SIZE        -> Size of %s: %d \n", result.Domain, l)
			totalSizeDomain++
		}

		full := result.GetFull()
		for i := range full {

			totalSubs++

			if !domain.IsValid(full[i]) {
				fmt.Fprintf(os.Stderr, "INVALID     -> %s is invalid domain\n", full[i])
				totalInvalidSubs++
			}
		}

		if *contains != "" && strings.Contains(result.Domain, *contains) {
			fmt.Printf("CONTAINS    -> %s with size if %d\n", result.Domain, l)
			totalContains++
		}
	}

	fmt.Printf("--------------\n")
	fmt.Printf("Time: %s\n", time.Now().Format(time.Stamp))
	fmt.Printf("Total domains: %d\n", totalDomains)
	fmt.Printf("Total subs: %d\n", totalSubs)
	fmt.Printf("Total invalid subs: %d\n", totalInvalidSubs)

	if *size != -1 {
		fmt.Printf("Number of domains with size >= %d: %d\n", *size, totalSizeDomain)
	}
	if *contains != "" {
		fmt.Printf("Number of contains match: %d\n", totalContains)
	}
}
