
class RequestTimeoutError(Exception):
    """Request timed out.

    """
    pass


class MessageFormatError(Exception):
    """We got a message but were unable to parse it.

    """
    pass


class ConnectionClosedError(Exception):
    """The other end closed the connection on us.

    """
    pass


class NoServersError(Exception):
    """There are no nats server(s) to connect to.

    """
    pass
