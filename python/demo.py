#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys
import threading
import time

from gambit import *

def send_once(host, port):
    message = Message("hello")

    with Container("send") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("examples")

        sender.send(message)

        print("Sent {}".format(message))

def receive_once(host, port):
    with Container("receive") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("examples")

        delivery = receiver.receive()

        print("Received {}".format(delivery.message))

def send_once_synchronously_using_sender_await_delivery(host, port):
    message = Message("hello")

    with Container("send") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("examples")

        def on_delivery(tracker):
            print("Sent {} ({})".format(tracker.message, tracker.state))

        sender.send(message, on_delivery)
        sender.await_delivery()

def send_once_synchronously_using_tracker_await_delivery(host, port):
    message = Message("hello")

    with Container("send") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("examples")

        tracker = sender.send(message)
        tracker.await_delivery()
        
        print("Sent {} ({})".format(tracker.message, tracker.state))

def receive_once_with_explicit_acks(host, port):
    with Container("receive") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("examples", auto_accept=False)

        delivery = receiver.receive()
        delivery.accept()
        # In principle: delivery.await_settlement()

        print("Received {}".format(delivery.message))

def send_batch(host, port):
    messages = [Message("hello-{}".format(x)) for x in range(3)]
    trackers = list()

    with Container("send") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("examples")

        for message in messages:
            # Talk about potential threading issues
            # Callback runs on worker thread - there's no good way I see to run it on the API thread
            # We would have to document the need for synchronization
            # Which makes me prefer send() returning a tracker
            sender.send(message, lambda x: trackers.append(x))

        for tracker in trackers:
            print("Sent {} ({})".format(tracker.message, tracker.state))

def receive_batch(host, port):
    with Container("receive") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("examples")

        for i in range(3):
            delivery = receiver.receive()
            print("Received {}".format(delivery.message))

def send_indefinitely(host, port, stopping):
    message = Message()

    def on_delivery(tracker):
        print("Sent {} ({})".format(tracker.message, tracker.state))

    with Container("send") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("examples")

        for i in xrange(sys.maxint):
            message.body = "message-{}".format(i)
            # OTOH, the callback makes the "side effect" case more convenient
            sender.send(message, on_delivery=on_delivery)

            if stopping.is_set(): break

def receive_indefinitely(host, port, stopping):
    with Container("receive") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("examples")

        for delivery in receiver:
            print("Received {}".format(delivery.message))

            if stopping.is_set(): break

def request_once(host, port):
    with Container("request") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("requests")
        receiver = conn.open_dynamic_receiver()

        request = Message("abc")
        request.reply_to = receiver.source.address

        sender.send(request)

        delivery = receiver.receive()

        print("Sent {} and received {}".format(request, delivery.message))

def request_once_using_send_request(host, port):
    with Container("request") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("requests")

        request = Message("abc")
        
        receiver = sender.send_request(request)
        delivery = receiver.receive()

        print("Sent {} and received {}".format(request, delivery.message))

def respond_once(host, port):
    with Container("respond") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("requests")

        delivery = receiver.receive()

        response = Message(delivery.message.body.upper())
        response.to = delivery.message.reply_to

        tracker = conn.send(response)
        tracker.await_delivery()

        print("Processed {} and sent {}".format(delivery.message, response))

def request_batch(host, port):
    requests = [Message("request-{}".format(x)) for x in range(3)]

    with Container("request") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("requests")
        receiver = conn.open_dynamic_receiver()

        for request in requests:
            sender.send_request(request, receiver=receiver)

        for request in requests:
            delivery = receiver.receive()

            print("Sent {} and received {}".format(request, delivery.message))

def respond_batch(host, port):
    with Container("respond") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("requests")

        for i in range(3):
            delivery = receiver.receive()

            response = Message(delivery.message.body.upper())
            response.to = delivery.message.reply_to

            conn.send(response)

            print("Processed {} and sent {}".format(delivery.message, response))

def main():
    try:
        host, port = sys.argv[1:3]
        port = int(port)
    except:
        sys.exit("Usage: demo HOST PORT")

    # Send and receive once

    send_once(host, port)
    receive_once(host, port)

    # Send and receive once with one style of await_delivery

    send_once_synchronously_using_sender_await_delivery(host, port)
    receive_once_with_explicit_acks(host, port)

    # Send and receive once with another style of await_delivery

    send_once_synchronously_using_tracker_await_delivery(host, port)
    receive_once_with_explicit_acks(host, port)

    # Send and receive a batch of three

    send_batch(host, port)
    receive_batch(host, port)

    # Send and receive indefinitely

    stopping = threading.Event()

    send_thread = threading.Thread(target=send_indefinitely, args=(host, port, stopping))
    send_thread.start()

    receive_thread = threading.Thread(target=receive_indefinitely, args=(host, port, stopping))
    receive_thread.start()

    time.sleep(0.02)
    stopping.set()

    send_thread.join()
    receive_thread.join()

    # Request and respond once

    respond_thread = threading.Thread(target=respond_once, args=(host, port))
    respond_thread.start()

    request_once(host, port)

    respond_thread.join()

    # Request using send_request and respond once

    respond_thread = threading.Thread(target=respond_once, args=(host, port))
    respond_thread.start()

    request_once_using_send_request(host, port)

    respond_thread.join()

    # Request and respond in a batch of three

    respond_thread = threading.Thread(target=respond_batch, args=(host, port))
    respond_thread.start()

    request_batch(host, port)

    respond_thread.join()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
