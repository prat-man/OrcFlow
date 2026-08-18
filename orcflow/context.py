def get_context():
    """Return the current OrcFlow context when context support is available."""
    raise RuntimeError("get_context() can only be used inside a flow or task")
