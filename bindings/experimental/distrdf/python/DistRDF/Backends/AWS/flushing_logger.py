import logging


class FlushingLogger:
    def __init__(self):
        self.logger = logging.getLogger()

    def __getattr__(self, name):
        method = getattr(self.logger, name)
        if name in ['info', 'warning', 'debug', 'error', 'critical', ]:
            def flushed_method(msg, *args, **kwargs):
                method(msg, *args, **kwargs)
                for h in self.logger.handlers:
                    h.flush()

            return flushed_method
        else:
            return method

