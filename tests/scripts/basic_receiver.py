from queue import Queue

from pynatsio import NatsService


nats = NatsService()  # no urls, should connect to localhost:4222 - the default nats

que = Queue()


def on_message(msg):
    print(f"Got a message {msg.subject} {msg.data} {msg.metadata} {msg.reply}")
    que.put_nowait(msg)


def on_error(e):
    print(f"Something went wrong: {e}")


nats.subscribe("test", on_message, on_error)


while True:
    msg = que.get()
    nats.publish(msg.reply, "ack")
