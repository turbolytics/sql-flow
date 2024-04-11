

class Tumbling:
    def __init__(self, size_seconds, output, trigger='on_close'):
        self.size_seconds = size_seconds
        self.output = output
        self.poll_interval_seconds = 1

    def start(self):
        pass
