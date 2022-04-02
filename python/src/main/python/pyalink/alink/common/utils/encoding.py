def ensure_unicode(x):
    if isinstance(x, (bytes,)):
        return x.decode('utf-8')
    return x
