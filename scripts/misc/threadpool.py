from threading import Thread
import Queue


class ThreadPoolThread(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.obj_queue = Queue.Queue()
        self.id = None

    def run(self):
        while True:
            obj = self.obj_queue.get()
            if obj is None:
                return
            self.process(obj)

    def process(self, obj):
        raise NotImplementedError()


class ThreadPool:
    def __init__(self, num_threads, cls, *params):
        self.threads = []
        for n in range(num_threads):
            new_thread = cls(*params)
            new_thread.id = n+1
            self.threads.append(new_thread)

    def start(self):
        for thread in self.threads:
            thread.start()

    def wait(self):
        for thread in self.threads:
            thread.join()
