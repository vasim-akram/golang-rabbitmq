package main

import (
	"encoding/json"
	golang_rabbitmq "golang-rabbitmq"
	"log"
	"os"
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

	err = amqpChannel.Qos(1, 0, false)
	handleError(err, "Could not configue QoS")

	messageChannel, err := amqpChannel.Consume(queue.Name, "", false, false, false, false, nil)
	handleError(err, "Could not register consumer")

	stopChan := make(chan bool)

	go func() {
		log.Printf("Consumer ready, PID: %d", os.Getpid())
		for d := range messageChannel {
			log.Printf("Received a message: %s", d.Body)

			// Sleep
			time.Sleep(5 * time.Second)

			addTask := &golang_rabbitmq.AddTask{}
			err := json.Unmarshal(d.Body, addTask)
			if err != nil {
				log.Printf("Error decoding JOSN: %d", err)
			}
			log.Printf("Result of %d + %d is: %d", addTask.Number1, addTask.Number2, addTask.Number1+addTask.Number2)

			if err := d.Ack(false); err != nil {
				log.Printf("Error acknowdeleging message: %s", err)
			} else {
				log.Printf("Acknowdeleged message")
			}
		}
	}()

	<-stopChan
}
