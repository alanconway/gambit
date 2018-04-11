/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

// Equivalent of gambit/python/demo.py using the original electron API
// TODO: request/response demos.
//
// Simplest way to run it: install proton where the Go binding can find it, or
// run the proton config.sh script to find it in your build. Then:
//
//     export GOPATH=/home/aconway/proton/go
//     go run demo-electron.go
//
// To speed things up:
//
//     go install qpid.apache.org/...
//
// This will install the compiled Go binding in your GOPATH rather than compiling it
// each time you run the demo.
//
package main

// Note the 0.22 Go client doesn't print human-readable Messages, fixed for 0.23
// https://issues.apache.org/jira/browse/PROTON-1826

import (
	"fmt"
	"os"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	"sync"
	"time"
)

var hostPort string = "localhost:5672"

func printError(err error) {
	if err != nil {
		fmt.Printf("error: %s\n", err)
	}
}

func main() {
	if len(os.Args) > 1 {
		hostPort = os.Args[1]
	}
	container := electron.NewContainer("demo")
	conn, err := container.Dial("tcp", hostPort)
	if err != nil {
		printError(err)
		os.Exit(1)
	}
	defer conn.Close(nil)

	SendOnce(conn)
	ReceiveOnce(conn)
	SendOnceSynchronously(conn)
	ReceiveOnceWithAccept(conn)
	SendBatch(conn)
	SendBatchWithTrackerQueue(conn)
	ReceiveBatch(conn)
	SendAndReceiveIndefinitely(conn)
}

func SendOnce(conn electron.Connection) {
	fmt.Printf("\nSendOnce\n")
	var err error
	if sender, err := conn.Sender(electron.Target("examples")); err == nil {
		defer sender.Close(nil)
		m := amqp.NewMessageWith("hello")
		sender.SendForget(m) // fire-and-forget - no blocking, no return value, no error status
		fmt.Printf("Sent %v\n", m)
	}
	printError(err)
}

func ReceiveOnce(conn electron.Connection) {
	fmt.Printf("\nReceiveOnce\n")
	var err error
	if receiver, err := conn.Receiver(electron.Source("examples")); err == nil {
		if rm, err := receiver.Receive(); err == nil { // Blocks for a message
			fmt.Printf("Received %v\n", rm.Message)
		}
	}
	printError(err)
}

func SendOnceSynchronously(conn electron.Connection) {
	fmt.Printf("\nSendOnceSynchronously\n")
	var err error
	if sender, err := conn.Sender(electron.Target("examples")); err == nil {
		m := amqp.NewMessageWith("hello")
		outcome := sender.SendSync(m) // Blocks for the outcome
		fmt.Printf("Sent %v (%v)\n", m, outcome.Status)
	}
	printError(err)
}

func ReceiveOnceWithAccept(conn electron.Connection) {
	fmt.Printf("\nReceiveOnceWithAccept\n")
	var err error
	if receiver, err := conn.Receiver(electron.Source("examples")); err == nil {
		if rm, err := receiver.Receive(); err == nil {
			rm.Accept() // rm is a ReceivedMessage, provides Message and ack methods
			fmt.Printf("Received %v\n", rm.Message)
		}
	}
	printError(err)
}

// Note: we use the same SendAsync API for both of the following batch-send
// cases. The difference is in how the application chooses to use channels to
// receive Outcomes.
//
// send_batch: use a new channel with capacity 1 for each send for
// individually waitable outcomes.
//
// send_batch_with_tracker_queue: use a single channel of capacity N to receive
// all outcomes in the order they become available.
//
// An electron.Outcome is the final state of a message.
// A gambit.tracker lets you wait for the final state.
// I'll use "tracker" variables names below for channels of Outcomes since that's the analogue

func SendBatch(conn electron.Connection) {
	fmt.Printf("\nSendBatch\n")
	var err error
	trackers := make([]chan electron.Outcome, 3) // Array of channels
	if sender, err := conn.Sender(electron.Target("examples")); err == nil {
		for i := 0; i < 3; i++ {
			m := amqp.NewMessageWith(fmt.Sprintf("hello-%v", i))
			trackers[i] = make(chan electron.Outcome, 1) // Individual tracker for this message
			// Note: final 'm' param means the message will be saved as Outcome.Value
			// We could pass a different correlation value or nil
			sender.SendAsync(m, trackers[i], m)
		}
		// Wait in order of sending, not order of acknowledgement
		for i := 0; i < 3; i++ {
			outcome := <-trackers[i]
			fmt.Printf("Sent %v (%v)\n", outcome.Value, outcome.Status)
		}
	}
	printError(err)
}

func SendBatchWithTrackerQueue(conn electron.Connection) {
	fmt.Printf("\nSendBatchWithTrackerQueue\n")
	var err error
	// Use a single channel as the "tracker queue" for all sends
	trackerQueue := make(chan electron.Outcome, 3)
	if sender, err := conn.Sender(electron.Target("examples")); err == nil {
		for i := 0; i < 3; i++ {
			m := amqp.NewMessageWith(fmt.Sprintf("hello-%v", i))
			sender.SendAsync(m, trackerQueue, m) // Pass same channel to each send
		}
		// Wait in order of acknowledgedment, not order of sending
		for i := 0; i < 3; i++ {
			outcome := <-trackerQueue
			fmt.Printf("Sent and acknowledged %v (%v)\n", outcome.Value, outcome.Status)
		}
	}
	printError(err)
}

func ReceiveBatch(conn electron.Connection) {
	fmt.Printf("\nReceiveBatch\n")
	var err error
	if receiver, err := conn.Receiver(electron.Source("examples")); err == nil {
		for i := 0; i < 3; i++ {
			if rm, err := receiver.Receive(); err == nil {
				fmt.Printf("Received %v\n", rm.Message)
			} else {
				printError(err)
				return
			}
		}
	}
	printError(err)
}

// Return true if err != nil, log a message if err != nil && err != electron.Closed
// err == electron.Closed indicates closed normally.
func checkClosed(err error) bool {
	switch err {
	case nil:
		return false
	case electron.Closed:
		return true
	default:
		fmt.Printf("endpoint closed with error: %v", err)
		return true
	}
}

func SendAndReceiveIndefinitely(conn electron.Connection) {
	fmt.Printf("\nSendAndReceiveIndefinitely\n")
	var err error
	// Note we stop the send/receive goroutines by closing the sender/receiver.
	// A separate signalling channel would work for the sender but *not* the receiver.
	// demo.py is buggy in this regard - it could hang forever if the stop Event is set when
	// the receiver thread is blocked and there are no more messages to wake it.
	//
	// We should consider this point for the API - do we need to be able to
	// interrupt a blocked call to Receive() (or blocking Send()) without closing
	// the Receiver/Sender? For Go a clean way to do this would be make all
	// blocking calls available as channels. Go provides `select` to try reading
	// on multiple channels in parallel and get the first result, it's common to use
	// this to wait on a data channel and a signalling channel so a goroutine can be
	// woken even if no data arrives (and without closing the data channel)
	// This is illustrated below in SendIndefinitely to wake the "tracker loop"
	//
	var wait sync.WaitGroup
	if receiver, err := conn.Receiver(electron.Source("examples")); err == nil {
		if sender, err := conn.Sender(electron.Target("examples")); err == nil {
			wait.Add(2)
			go ReceiveIndefinitely(receiver, &wait)
			go SendIndefinitely(sender, &wait)
			time.Sleep(time.Second / 10)
			sender.Close(nil)
			receiver.Close(nil)
			wait.Wait() // Equivalent to join()
		}
	}
	printError(err)
}

func SendIndefinitely(sender electron.Sender, wait *sync.WaitGroup) {
	defer wait.Done()
	tracker := make(chan electron.Outcome)
	// Note: No callback, use concurrent send and tracker loops.
	// We could add callbacks, but this style is more concurrent.
	var wait2 sync.WaitGroup
	wait2.Add(2)
	stop := make(chan struct{}) // Signalling channel to stop the tracker loop

	go func() { // Send loop
		defer wait2.Done()
		for i := 0; ; i++ {
			if checkClosed(sender.Error()) {
				close(stop) // Signal the tracker loop to stop
				return
			}
			m := amqp.NewMessageWith(fmt.Sprintf("hello-%v", i))
			sender.SendAsync(m, tracker, m)
		}
	}()

	// Tracker loop - uses `select` to wake-up when the stop channel is closed.
	//
	// Note closing the Sender does not automatically close the tracker channel,
	// because it might be shared between senders. Hence we need the stop channel.
	//
	// Review this: it would be nice to automatically have tracker channels close
	// when the sender does because then the tracker loop would be very easy:
	//
	//     for outcome := range(tracker) { process(outcome) }
	//
	go func() {
		defer wait2.Done()
		for {
			select {
			case outcome := <-tracker:
				fmt.Printf("Sent %v (%v)\n", outcome.Value, outcome.Status)
			case <-stop:
				return
			}
		}
	}()
	wait2.Wait()
}

func ReceiveIndefinitely(receiver electron.Receiver, wait *sync.WaitGroup) {
	defer wait.Done()
	for {
		rm, err := receiver.Receive()
		if checkClosed(err) {
			return
		} else {
			fmt.Printf("Received %v\n", rm.Message)
		}
	}
}
