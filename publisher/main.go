package main

import (
	"encoding/json"
	golang_rabbitmq "golang-rabbitmq"
	"log"
	"math/rand"
	"time"

	"github.com/streadway/amqp"
)

func handleError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial(golang_rabbitmq.Config.AMQPConnectionURL)
	handleError(err, "Can't connect to AMQP")
	defer conn.Close()

	amqpChannel, err := conn.Channel()
	handleError(err, "Can't create a new amqpChannel")
	defer amqpChannel.Close()

	queue, err := amqpChannel.QueueDeclare("add", true, false, false, false, nil)
	handleError(err, "Could not declare `add` queue")

	rand.Seed(time.Now().UnixNano())

	addTask := golang_rabbitmq.AddTask{Number1: rand.Intn(999), Number2: rand.Intn(999)}
	body, err := json.Marshal(addTask)
	handleError(err, "Error endcoding JSON")

	err = amqpChannel.Publish("", queue.Name, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         body,
	})
	if err != nil {
		log.Printf("Error publishing msg: %s", err)
	}
	log.Printf("AddTask: %d+%d", addTask.Number1, addTask.Number2)
}
