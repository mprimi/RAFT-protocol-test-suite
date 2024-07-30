package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/nats-io/nats.go"
)

func main() {
	var (
		groupId string
		natsUrl string
	)
	flag.StringVar(&groupId, "group-id", "", "raft group id")
	flag.StringVar(&natsUrl, "nats-url", nats.DefaultURL, "nats url")
	flag.Parse()

	fatalErr := func(err error) {
		fmt.Println(err)
		os.Exit(1)
	}

	if groupId == "" {
		fatalErr(fmt.Errorf("missing required argument: group-id"))
	}

	nc, err := nats.Connect(natsUrl)
	if err != nil {
		fatalErr(fmt.Errorf("failed to connect: %w", err))
	}
	defer nc.Close()

	// TODO wait for RAFT group to be established -- not sure how.
	// For now, do the dumb thing:
	time.Sleep(3 * time.Second)

	// If running in antithesis, signal setup is complete
	lifecycle.SetupComplete(nil)

	// Block forever
	for {
		select {
		case <-time.After(5 * time.Second):
			fmt.Printf("Idle...\n")
		}
	}
}
