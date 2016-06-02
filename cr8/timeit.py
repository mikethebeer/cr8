#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import argh
from urllib.request import urlopen
import itertools

from functools import partial
from time import time

from .metrics import Stats
from . import aio, cli


class Result:
    def __init__(self,
                 version_info,
                 statement,
                 started,
                 ended,
                 stats,
                 concurrency,
                 bulk_size=None):
        self.version_info = version_info
        self.statement = statement
        # need ts in ms in crate
        self.started = int(started * 1000)
        self.ended = int(ended * 1000)
        self.runtime_stats = stats.get()
        self.concurrency = concurrency
        self.bulk_size = bulk_size

    def __str__(self):
        return json.dumps(self.__dict__)


class QueryRunner:
    def __init__(self, stmt, repeats, hosts, concurrency):
        self.stmt = stmt
        self.repeats = repeats
        self.concurrency = concurrency
        self.loop = aio.asyncio.get_event_loop()
        self.host = next(iter(hosts))
        urls = itertools.cycle([u + '/_sql' for u in hosts])
        self.execute = partial(aio.execute, urls)

    def warmup(self, num_warmup):
        statements = itertools.repeat((self.stmt,), num_warmup)
        aio.run(self.execute, statements, 0, loop=self.loop)

    def run(self):
        version_info = self.get_version_info(self.host)

        started = time()
        statements = itertools.repeat((self.stmt,), self.repeats)
        stats = Stats(min(self.repeats, 1000))
        measure = partial(aio.measure, stats, self.execute)

        aio.run(measure, statements, self.concurrency, loop=self.loop)
        ended = time()

        return Result(
            statement=self.stmt,
            version_info=version_info,
            started=started,
            ended=ended,
            stats=stats,
            concurrency=self.concurrency
        )

    @staticmethod
    def get_version_info(server):
        r = urlopen(server)
        data = json.loads(r.read().decode('utf-8'))
        return {
            'number': data['version']['number'],
            'hash': data['version']['build_hash']
        }


@argh.arg('hosts', help='crate hosts', type=cli.to_hosts)
@argh.arg('-w', '--warmup', type=cli.to_int)
@argh.arg('-r', '--repeat', type=cli.to_int)
@argh.arg('-c', '--concurrency', type=cli.to_int)
def timeit(hosts, stmt=None, warmup=30, repeat=30, concurrency=1):
    """ runs the given statement a number of times and returns the runtime stats
    """
    num_lines = 0
    for line in cli.lines_from_stdin(stmt):
        runner = QueryRunner(line, repeat, hosts, concurrency)
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
