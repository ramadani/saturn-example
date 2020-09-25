package main

import (
	"context"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/ramadani/saturn"
	"github.com/ramadani/titan"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	saramaCog := sarama.NewConfig()
	saramaCog.Version = sarama.V0_10_2_0
	saramaCog.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaCog.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	addresses := []string{"localhost:9092"}
	consumerGroup, err := sarama.NewConsumerGroup(addresses, "email-service", saramaCog)
	if err != nil {
		log.Panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	event := titan.DefaultConsumerGroupEventListener(consumerGroup)

	_ = event.On(ctx, "userRegistered", []saturn.Listener{
		&sendEmailVerificationListener{},
	})

	_ = event.On(ctx, "userActivated", []saturn.Listener{
		&sendWelcomeEmailListener{},
	})

	go func() {
		if err = event.Listen(ctx); err != nil {
			log.Panic(err)
		}
	}()

	//<-event.Ready()

	log.Println("Email Service up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()

	if err = consumerGroup.Close(); err != nil {
		log.Panicf("Error closing consumer group: %v", err)
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

type sendEmailVerificationListener struct{}

func (l *sendEmailVerificationListener) Handle(ctx context.Context, value []byte) (err error) {
	data := &userRegistration{}
	_ = json.Unmarshal(value, data)

	log.Println("send email verification to", data.Email)
	return
}

type sendWelcomeEmailListener struct{}

func (l *sendWelcomeEmailListener) Handle(ctx context.Context, value []byte) (err error) {
	data := &userActivation{}
	_ = json.Unmarshal(value, data)

	log.Println("send welcome email to", data.Email)
	return
}
