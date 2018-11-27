#### Python3.6 Nats.io Client ####


Simple client that builds on top of the [async.io nats client]( https://github.com/nats-io/asyncio-nats) so that one can write much cleaner nats code without having to deal with the abomination that is async.io.


#### Example

Trivial example

```python

from queue import Queue

from pynatsio import NatsService


nats = NatsService()
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

``` 


```python

from pynatsio import NatsService


nats = NatsService()
reply = nats.request("test", {"number": 123, "hello": "world", "do": True})
print(reply.data, reply.metadata, reply.subject)

```

Simple eh? No coroutine, new_event_loop, run_until_complete, async .. yada yada. Just straight forward nats.  


Disclaimer: I've given this very little testing ;) Might pay to add tests if you're planning on using it in anger.
