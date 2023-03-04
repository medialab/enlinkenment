import timeit
import datetime

class Timer:
    def __init__(self, message=None) -> None:
        self.message = message
        self.start = timeit.default_timer()
        if self.message:
            print(f'\n{self.message}')
        print('Began at {}'.format(datetime.datetime.now().time()))

    def stop(self):
        stop = timeit.default_timer()
        delta = stop - self.start
        print('Finished in {}.\n'.format(str(datetime.timedelta(seconds=round(delta)))))
