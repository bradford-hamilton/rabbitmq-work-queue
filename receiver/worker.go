package main

import (
	"bytes"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Create a channel
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare a queue - see Note 2 in comments below
	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// msgs retured by Consume is a channel for us to read messages from
	// Also see "Note 1" in comments below
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	neverEndingChan := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			log.Printf("Done")
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	<-neverEndingChan
}

/*
	Note 1:
	In order to make sure a message is never lost, RabbitMQ supports message acknowledgments.
	An ack(nowledgement) is sent back by the consumer to tell RabbitMQ that a particular message
	has been received, processed and that RabbitMQ is free to delete it. If a consumer
	dies (its channel is closed, connection is closed, or TCP connection is lost) without
	sending an ack, RabbitMQ will understand that a message wasn't processed fully and will
	re-queue it. If there are other consumers online at the same time, it will then quickly
	redeliver it to another consumer. That way you can be sure that no message is lost, even
	if the workers occasionally die. There aren't any message timeouts; RabbitMQ will redeliver
	the message when the consumer dies. It's fine even if processing a message takes a
	very, very long time. In this example we will use manual message acknowledgements by
	passing a false for the "auto-ack" argument and then send a proper acknowledgment from
	the worker with d.Ack(false) (this acknowledges a single delivery), once we're done
	with a task. Acknowledgement must be sent on the same channel that received the delivery.
	Attempts to acknowledge using a different channel will result in a channel-level protocol
	exception.
*/

/*
	Note 2:
	We declare the queue here on the receiver as well because we might start the consumer before the
	publisher, we want to make sure the queue exists before we try to consume messages from it.
	RabbitMQ doesn't allow you to redefine an existing queue with different parameters (like adding
	durable: true when it has already been declared at another point without this) and will return
	an error to any program that tries to do that. When RabbitMQ quits or crashes it will forget the
	queues and messages unless you tell it not to. Two things are required to make sure that
	messages aren't lost: we need to mark both the queue and messages as durable.
*/
