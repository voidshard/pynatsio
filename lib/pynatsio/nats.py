import asyncio
import logging
import threading
import queue

from nats.aio.client import Client as _NatsClient
from nats.aio import errors as nats_errors

from pynatsio import exceptions as exc
from pynatsio import message
from pynatsio import utils


_NATS_MIN_TIMEOUT = 5


_logger = logging.getLogger(__name__)


class NatsService:
    """Nats.io service.

    """

    def __init__(self, urls=None, ssl_cert=None, ssl_key=None, ssl_verify=False):
        if not urls:
            urls = ["nats://localhost:4222"]

        tls = None

        if ssl_cert and ssl_key:
            tls = utils.load_ssl_context(ssl_key, ssl_cert, verify=ssl_verify)

        self.__conn = _NatsService(urls, tls)
        self.__conn.setDaemon(True)
        self.__conn.start()

    def stop(self):
        """Shutdown.

        """
        self.__conn.stop()

    def subscribe(self, key: str, callback: callable, error_callback: callable):
        """Register some function to be called on some key.

        :param key: the key (subject) to subscribe to
        :param callback: a function that accepts a single tapestry_common.domain.message.Message
        :param error_callback: a function that accepts a single Exception

        """
        return self.__conn.subscribe(key, callback, error_callback)

    def publish(self, key: str, data):
        """Publish a message & do NOT wait for a reply.

        :param data: data to send
        :param key: the key (subject) to send the message to

        """
        return self.__conn.publish(key, data)

    def request(self, key: str, data, timeout: int=5) -> message.Message:
        """Send a request to the server & await the reply.

        :param data: data to send
        :param key: the key (subject) to send the message to
        :param timeout: some time in seconds to wait before calling it quits
        :returns: message.Message
        :raises: RequestTimeoutError
        :raises: ConnectionClosedError
        :raises: NoServersError

        """
        return self.__conn.request(key, data, timeout=timeout)


class _NatsService(threading.Thread):
    """Wrapper class around the Nats.IO asyncio implementation to provide us with
    a much nicer interface to work with.

    Seriously the vanilla client is unusable and hideous as sin ..
        https://github.com/nats-io/asyncio-nats
    Who wants to pepper their code with "@asyncio.coroutine" and deal with asyncio loops and
    other such garbage.

    """

    _MAX_RECONNECTS = 10

    def __init__(self, urls, tls):
        threading.Thread.__init__(self)
        self._conn = None
        self._kill = queue.Queue()
        self._requests = queue.Queue()  # request(s) added in request()
        self._publishes = queue.Queue()  # messages to publish (fire & forget)
        self._subscriptions = queue.Queue()  # subscriptions to queue up
        self._running = False

        self.opts = {  # opts to pass to Nats.io client
            "servers": urls,
            "allow_reconnect": True,
            "max_reconnect_attempts": self._MAX_RECONNECTS,
        }

        if tls:
            self.opts["tls"] = tls

    def _decode_message(self, nats_message):
        """Decode a nats message into our message format.

        :param nats_message: nats.Message obj
        :return: message.Message
        :raises: MessageFormatError
        :raises: InvalidMessageSignature

        """
        return message.Message.decode(
            nats_message.subject,
            nats_message.reply,
            nats_message.data
        )

    def _encode_message(self, msg: message.Message) -> bytes:
        """Prepare a signed message for sending

        :param msg: message.Message to send
        :return: bytes

        """
        return msg.encode()

    @asyncio.coroutine
    def main(self, loop):
        """Connects to remote host(s) & sets up the main loop that deals with asyncio & the nats
        servers.

        In order to add sanity to asyncio we set up four queues:
         kill queue - for when someone wants us to quit
         publish queue - a (key, msg) tuple is given to us to send, no reply is expected
         subscription queue - a (key, callback, error_callback) is given,
            we need to set up a subscription and create a new function that wraps the given
            callbacks for asyncio to call when a message arrives
         request queue - a (queue, key, msg, timeout) is given. We need to register a callback
            to be called when the request returns (as in subscription). The resulting parsed
            message needs to be put on the given queue. (This allows the thread passing the queue
            in to block until it gets a result, or timeout .. without needing to care about
            asyncio or be in the main loop).

        Raises:
            NoServersError

        """
        # Notes to the wary:
        #  Be *very* careful what you do in here, even removing the sleep statements, using
        #  'continue' or simple 'yield' will totally break async io's fragile little brain.
        #
        #  Another protip: even if we're passed callbacks, we must wrap these in dynamically
        #  created functions for things to work.

        # explicitly set the asyncio event loop so it can't get confused ..
        asyncio.set_event_loop(loop)

        self._conn = _NatsClient()
        sleep_time = 0.05

        try:
            yield from self._conn.connect(io_loop=loop, **self.opts)
        except nats_errors.ErrNoServers as e:
            # Could not connect to any server in the cluster.
            raise exc.NoServersError(e)

        while self._running:  # main async.io loop
            if not any([
                not self._subscriptions.empty(),
                not self._requests.empty(),
                not self._publishes.empty()
            ]):
                # nothing needs sending
                yield from asyncio.sleep(sleep_time, loop=loop)

            if not self._kill.empty():
                break  # if there is anything in this queue it means we should exit

            if not self._conn.is_connected:
                # we should reconnect automatically, we just need to wait a bit ..
                yield from asyncio.sleep(sleep_time, loop=loop)

            if not self._publishes.empty():
                # Send publishes (essentially messages to which we don't care about a reply)

                key, msg = self._publishes.get_nowait()
                try:
                    yield from self._conn.publish(key, self._encode_message(msg))
                except nats_errors.ErrConnectionClosed as e:
                    _logger.error(e)
                except (nats_errors.ErrTimeout, queue.Empty) as e:
                    _logger.error(e)
                except Exception as e:  # pass all exc up to the caller
                    _logger.error(e)

            if not self._subscriptions.empty():
                # Subscribe to subscription(s)

                def fn(success, fail):
                    # be careful here to return a uniquely generated callback

                    def call(recv_msg):  # the actual callback given to async io
                        try:
                            result = self._decode_message(recv_msg)
                        except exc.MessageFormatError as e:
                            fail(e)
                        else:
                            success(result)
                    return call

                key, callback, err_callback = self._subscriptions.get_nowait()
                yield from self._conn.subscribe(key, cb=fn(callback, err_callback))

            if not self._requests.empty():
                # Send request(s) (messages to which we expect a reply)

                def fn(r_queue):
                    # be careful here to return a uniquely generated callback

                    def call(recv_msg):  # the actual callback given to async io
                        try:
                            r_queue.put_nowait(self._decode_message(recv_msg))
                        except exc.MessageFormatError as e:
                            r_queue.put_nowait(e)
                        except nats_errors.ErrConnectionClosed as e:
                            r_queue.put_nowait(exc.ConnectionClosedError(e))
                        except (nats_errors.ErrTimeout, queue.Empty) as e:
                            r_queue.put_nowait(exc.RequestTimeoutError(e))
                        except Exception as e:  # pass all exc up to the caller
                            r_queue.put_nowait(e)
                    return call

                reply_queue, key, msg, timeout = self._requests.get_nowait()
                yield from self._conn.request(
                    key, self._encode_message(msg), timeout, expected=1, cb=fn(reply_queue)
                )

        yield from self._conn.close()

    def subscribe(self, key: str, callback: callable, error_callback: callable):
        """Subscribe to a channel "key" & call "callback" passing in the message object
        whenever something arrives. Alternatively, if something errors, call "error_callback"
        and pass in the error that was raised.

        :param key:
        :param callback: a function that accepts a single arg
        :param error_callback: a function that accepts a single arg

        """
        self._subscriptions.put_nowait((key, callback, error_callback))

    def publish(self, key: str, data):
        """Publish a message & do NOT wait for a reply.

        :param data: data to send
        :param key: the key (subject) to send the message to

        """
        self._publishes.put_nowait((key, message.Message(data=data)))  # add to outbound

    def request(self, key: str, data: dict, timeout: int=5) -> message.Message:
        """Send a request to the server & await the reply.

        :param data: data to send
        :param key: the key (subject) to send the message to
        :param timeout: some time in seconds to wait before calling it quits
        :returns: message.Message
        :raises: RequestTimeoutError
        :raises: ConnectionClosedError
        :raises: NoServersError

        """
        q = queue.Queue(maxsize=1)  # create a queue to get a reply on
        self._requests.put_nowait((q, key, message.Message(data=data), timeout))  # add to outbound
        try:
            result = q.get(timeout=max([_NATS_MIN_TIMEOUT, timeout]))  # block for a reply
        except queue.Empty as e:  # we waited, but nothing was returned to us :(
            raise exc.RequestTimeoutError("Timeout waiting for server reply. Original %s" % e)

        if isinstance(result, Exception):
            raise result

        return result

    def stop(self):
        """Stop the service, killing open connection(s)

        """
        # set to false to kill coroutine running in main()
        self._running = False

        # interpreted as a poison pill (causes main() loop to break)
        self._kill.put(True)

        if not self._conn:
            return

        try:
            # flush & kill the actual connections
            self._conn.flush()
            self._conn.close()
        except Exception:
            pass

    def run(self):
        """Start the service

        """
        if self._running:
            return

        self._running = True

        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.main(loop))
        try:
            loop.close()
        except Exception:
            pass
