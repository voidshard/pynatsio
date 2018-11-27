import ssl


def load_ssl_context(key, cert, verify=False):
    """Util func to load an ssl context.

    Args:
        key (str): path to file
        cert (str): path to file
        verify (bool): if true, we'll verify the server's certs
    Returns:
        ssl_context

    """
    purpose = ssl.Purpose.SERVER_AUTH if verify else ssl.Purpose.CLIENT_AUTH

    tls = ssl.create_default_context(purpose=purpose)
    tls.protocol = ssl.PROTOCOL_TLSv1_2
    tls.load_cert_chain(certfile=cert, keyfile=key)

    return tls
