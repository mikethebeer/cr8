
from tqdm import tqdm
import asyncio
import aiohttp
import json
import functools
import itertools
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


async def _exec(url, data):
    with aiohttp.ClientSession() as session:
        async with session.post(url, data=data) as resp:
            r = await resp.json()
            if 'error' in r:
                raise ValueError(r['error']['message'])
            return r['duration']


async def execute(urls, stmt, args=None):
    payload = {'stmt': stmt}
    if args:
        payload['args'] = args
    return await _exec(next(urls), json.dumps(payload))


async def execute_many(urls, stmt, bulk_args):
    data = json.dumps(dict(stmt=stmt, bulk_args=bulk_args))
    return await _exec(next(urls), data)


def create_execute_many(hosts):
    urls = itertools.cycle([i + '/_sql' for i in hosts])
    return functools.partial(execute_many, urls)


async def measure(stats, f, *args, **kws):
    duration = await f(*args, **kws)
    stats.measure(duration)
    return duration


async def map_async(q, corof, iterable):
    for i in iterable:
        task = asyncio.ensure_future(corof(*i))
        await q.put(task)
    await q.put(None)


async def consume(q, total=None):
    with tqdm(total=total, unit=' requests', smoothing=0.1) as t:
        while True:
            task = await q.get()
            if task is None:
                break
            await task
            t.update(1)


async def run_sync(coro, iterable, total=None):
    for i in tqdm(iterable, total=total, unit=' requests', smoothing=0.1):
        await coro(*i)


def run(coro, iterable, concurrency, loop=None, num_items=None):
    loop = loop or asyncio.get_event_loop()
    if concurrency == 1:
        return loop.run_until_complete(run_sync(coro, iterable, total=num_items))
    q = asyncio.Queue(maxsize=concurrency)
    loop.run_until_complete(asyncio.gather(
        map_async(q, coro, iterable),
        consume(q, total=num_items)))
