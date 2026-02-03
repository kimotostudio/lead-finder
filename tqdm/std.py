"""
Minimal safe replacement for tqdm.std to avoid writing/flushing stderr
in background threads on Windows. Provides `status_printer` and a
`tqdm` noop class.
"""
class _DummyPrinter:
    def write(self, *args, **kwargs):
        return None

    def flush(self, *args, **kwargs):
        return None


def status_printer(fp):
    """Return a dummy printer that ignores write/flush."""
    return _DummyPrinter()


class tqdm:
    def __init__(self, *args, **kwargs):
        self.total = kwargs.get('total', 0)

    def update(self, *args, **kwargs):
        return None

    def close(self, *args, **kwargs):
        return None

    def __iter__(self):
        return iter(())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False
