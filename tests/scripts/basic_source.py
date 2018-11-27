from pynatsio import NatsService


nats = NatsService()  # no urls, should connect to localhost:4222 - the default nats

reply = nats.request("test", {"number": 123, "hello": "world", "do": True})

print(reply.data, reply.metadata, reply.subject)
