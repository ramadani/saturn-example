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
		"userRegistered": {
			&sendEmailVerificationListener{},
		},
	})
	emitter := saturn.NewLocalEmitter(event)
	ctx := context.Background()

	_ = event.On(ctx, "userRegistered", []saturn.Listener{
		&logNewUserListener{},
	})

	start := time.Now()

	err := emitter.Emit(ctx, &userRegistered{data: user{Name: "Ramadani", Email: "dani@gmail.id"}})
	log.Println("emit err", err)

	end := time.Now()
	diff := end.Sub(start)
	log.Println("process time", diff)
}

type user struct {
	Name, Email string
}

type userRegistered struct {
	data user
}

func (e *userRegistered) Header() string {
	return "userRegistered"
}

func (e *userRegistered) Body() ([]byte, error) {
	b, err := json.Marshal(&e.data)
	if err != nil {
		return nil, err
	}
	return b, nil
}

type sendEmailVerificationListener struct{}

func (l *sendEmailVerificationListener) Handle(_ context.Context, value []byte) (err error) {
	data := &user{}
	_ = json.Unmarshal(value, data)

	time.Sleep(time.Second * 3)

	log.Println("send email to", data.Email)
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
