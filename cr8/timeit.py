#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh
import requests
import itertools

from appmetrics import metrics
from time import time
from crate.client import connect
from functools import partial

from cr8 import aio
from .cli import lines_from_stdin, to_int
from .stats import Result


class QueryRunner:
    def __init__(self, stmt, repeats, hosts, concurrency, loop=None):
        self.loop = loop = loop or aio.asyncio.get_event_loop()
        self.stmt = stmt
        self.concurrency = concurrency
        self.hosts = hosts
        self.repeats = repeats
        self.conn = connect(hosts)
        cursor = self.conn.cursor()
        self.execute = partial(aio.execute, loop, cursor)

        self.hist = hist = metrics.new_histogram(
            'durations-{}-{}-{}'.format(stmt, repeats, concurrency))
        self.measure = partial(aio.measure, hist, self.execute)

    def warmup(self, num_warmup):
        statements = itertools.repeat((self.stmt,), num_warmup)
        aio.run(self.execute, statements, 0, self.loop)

    def run(self):
        version_info = self.get_version_info(self.conn.client.active_servers[0])

        started = time()

        statements = itertools.repeat((self.stmt,), self.repeats)
        aio.run(self.measure, statements, self.concurrency, self.loop)
        ended = time()

        return Result(
            statement=self.stmt,
            version_info=version_info,
            started=started,
            ended=ended,
            repeats=self.repeats,
            concurrency=self.concurrency,
            stats=self.hist.get()
        )

    @staticmethod
    def get_version_info(server):
        data = requests.get(server).json()
        return {
            'number': data['version']['number'],
            'hash': data['version']['build_hash']
        }


@argh.arg('hosts', help='crate hosts', type=str)
@argh.arg('-w', '--warmup', type=to_int)
@argh.arg('-r', '--repeat', type=to_int)
@argh.arg('-c', '--concurrency', type=to_int)
def timeit(hosts, stmt=None, warmup=30, repeat=30, concurrency=1):
    """ runs the given statement a number of times and returns the runtime stats
    """
    num_lines = 0
    loop = aio.asyncio.get_event_loop()
    for line in lines_from_stdin(stmt):
        runner = QueryRunner(line, repeat, hosts, concurrency, loop=loop)
        runner.warmup(warmup)
        result = runner.run()
        yield result
        num_lines += 1
    if num_lines == 0:
        raise SystemExit(
            'No SQL statements provided. Use --stmt or provide statements via stdin')


def main():
    argh.dispatch_command(timeit)


if __name__ == '__main__':
    main()
