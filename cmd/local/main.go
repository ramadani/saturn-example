package main

import (
	"context"
	"encoding/json"
	"github.com/ramadani/saturn"
	"log"
	"time"
)

func main() {
	event := saturn.NewLocalEvent(map[string][]saturn.Listener{
		"user.created": {
			&sendEmailListener{},
			&checkRewardsListener{},
		},
	})
	emitter := saturn.NewLocalEmitter(event)
	ctx := context.Background()
	_ = event.On(ctx, "user.created", []saturn.Listener{
		&logNewUserListener{},
	})

	start := time.Now()

	err := emitter.Emit(ctx, NewUserCreated(user{Name: "Ramadani", Email: "dani@gmail.id"}))
	log.Println("emit err", err)

	end := time.Now()
	diff := end.Sub(start)
	log.Println("process time", diff)
}

type user struct {
	Name, Email string
}

type userCreated struct {
	data user
}

func (e *userCreated) Header() string {
	return "user.created"
}

func (e *userCreated) Body() ([]byte, error) {
	b, err := json.Marshal(&e.data)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func NewUserCreated(data user) saturn.Dispatchable {
	return &userCreated{data: data}
}

type sendEmailListener struct{}

func (l *sendEmailListener) Handle(_ context.Context, value []byte) (err error) {
	data := &user{}
	_ = json.Unmarshal(value, data)

	time.Sleep(time.Second * 3)

	log.Println("send email to", data.Email)
	return
}

type checkRewardsListener struct{}

func (l *checkRewardsListener) Handle(_ context.Context, value []byte) (err error) {
	data := &user{}
	_ = json.Unmarshal(value, data)

	time.Sleep(time.Second * 5)

	log.Println("check rewards for", data.Name)
	return
}

type logNewUserListener struct{}

func (l *logNewUserListener) Handle(_ context.Context, value []byte) (err error) {
	data := &user{}
	_ = json.Unmarshal(value, data)

	time.Sleep(time.Second * 1)

	log.Println("log new user", data.Name)
	return
}
