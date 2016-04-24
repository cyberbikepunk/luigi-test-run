from luigi import Task, LocalTarget, run, IntParameter
from pandas import DataFrame
from numpy.random import random


class TestParameter(Task):
    n = IntParameter(default=20)
    try:
        print(n + 1)
    except TypeError:
        print('does not work outside a method')

    def requires(self):
        return []

    def output(self):
        pass

    def run(self):
        print(self.n + 1)


if __name__ == '__main__':
    run()
