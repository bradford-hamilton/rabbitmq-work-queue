package main

import (
	"log"
	"os"
	"strings"

	"github.com/streadway/amqp"
)

func main() {
	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Create a channel
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	/*
		Declaring a queue is idempotent - it will only be created if it doesn't exist already.
		The message content is a byte array, so you can encode whatever you like there.
	*/

	// Declare a queue
	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// See "Note 1" in comments below
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	// Create message
	body := bodyFrom(os.Args)

	// Publish message (see "Note 2" in comments below)
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		},
	)
	failOnError(err, "Failed to publish a message")

	log.Printf(" [x] Sent %s", body)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "Default message"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}

/*
	Note 1:
	Dispatching can potentially not work as we'd want. For example in a situation with
	two workers, when all odd messages are heavy and even messages are light, one worker
	will be constantly busy and the other one will do hardly any work. RabbitMQ doesn't
	know anything about that and will still dispatch messages evenly. This happens
	because RabbitMQ just dispatches a message when the message enters the queue. It
	doesn't look at the number of unacknowledged messages for a consumer. It just blindly
	dispatches every n-th message to the n-th consumer. In order to defeat this we can
	set the prefetch count with the value of 1. This tells RabbitMQ not to give more
	than one message to a worker at a time. Or, in other words, don't dispatch a new
	message to a worker until it has processed and acknowledged the previous one.
	Instead,it will dispatch it to the next worker that is not still busy.
*/

/*
	Note 2:
	Marking messages as persistent doesn't fully guarantee that a message won't be lost.
	Although it tells RabbitMQ to save the message to disk, there is still a short time
	window when RabbitMQ has accepted a message and hasn't saved it yet. Also, RabbitMQ
	doesn't do fsync(2) for every message -- it may be just saved to cache and not really
	written to the disk. The persistence guarantees aren't strong, but it's more than
	enough for our simple task queue. If you need a stronger guarantee then you can use
	publisher confirms.
*/

// Other Notes:

/*
	If all the workers are busy, your queue can fill up. You will want to keep an eye on
	that, and maybe add more workers, or have some other strategy.
*/
