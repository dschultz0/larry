import larrydata


_client = None


def client():
    global _client
    if _client is None:
        _client = larrydata.session().client('sts')
    return _client


def account_id():
    sts = client()
    return sts.get_caller_identity()['Account']
