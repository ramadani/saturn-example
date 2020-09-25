package main

import (
	"context"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/ramadani/titan"
	"log"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	addresses := []string{"localhost:9092"}

	prd, err := sarama.NewSyncProducer(addresses, config)
	if err != nil {
		log.Fatal(err)
	}
	defer prd.Close()

	event := titan.NewEmitter(prd)
	ctx := context.Background()

	userRegistered := &userRegistration{
		Name:  "Ramadani",
		Email: "dani@gmail.com",
	}

	log.Println("user registered")
	if err = event.Emit(ctx, &userRegisteredEvent{data: userRegistered}); err != nil {
		log.Fatal(err)
	}

	userActivated := &userActivation{
		Name:         "Ramadani",
		Email:        "dani@gmail.com",
		ReferralCode: "qwerty",
	}

	log.Println("user activated")
	if err = event.Emit(ctx, &userActivatedEvent{data: userActivated}); err != nil {
		log.Fatal(err)
	}
}

type userRegistration struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type userActivation struct {
	Name         string `json:"name"`
	Email        string `json:"email"`
	ReferralCode string `json:"referralCode"`
}

type userRegisteredEvent struct {
	data *userRegistration
}

func (u userRegisteredEvent) Header() string {
	return "userRegistered"
}

func (u userRegisteredEvent) Body() ([]byte, error) {
	return json.Marshal(u.data)
}

type userActivatedEvent struct {
	data *userActivation
}

func (u userActivatedEvent) Header() string {
	return "userActivated"
}

func (u userActivatedEvent) Body() ([]byte, error) {
	return json.Marshal(u.data)
}
