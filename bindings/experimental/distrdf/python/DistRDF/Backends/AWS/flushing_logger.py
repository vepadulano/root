import logging


class FlushingLogger:
    FLUSHED_METHODS = [
        'info',               
        'warning',
        'debug',
        'error',
        'critical'
    ]

    def __init__(self):
        self.logger = logging.getLogger()

    def __getattr__(self, name):
        method = getattr(self.logger, name)

        if name not in self.FLUSHED_METHODS:
            return method

        def flushed_method(msg, *args, **kwargs):
            method(msg, *args, **kwargs)
            for h in self.logger.handlers:
                h.flush()

        return flushed_method

