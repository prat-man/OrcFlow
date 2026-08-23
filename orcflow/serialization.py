import cloudpickle


def serialize_call(fn, args, kwargs):
    """Serialize a callable and its arguments for process transport."""
    return cloudpickle.dumps((fn, args, kwargs))


def deserialize_call(payload):
    """Deserialize a callable and its arguments inside a worker process."""
    return cloudpickle.loads(payload)
