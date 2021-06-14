# -*- coding: utf-8 -*-
"""
MODULE: Kafka to S3 Streamer
"""


import sys
import random
import signal
import asyncio
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import List

import typer
import uvloop
import psutil
import orjson as json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
from aiokafka import AIOKafkaConsumer
from janus import Queue, PriorityQueue


app = typer.Typer()
signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT, signal.SIGQUIT)

config = {
    "handlers": [
        {
            "sink": sys.stdout,
            "format": "{time:YYYY-MM-DD HH:mm:ss} - {process.name} – [{process.id}] - {level} - {message}",
            "backtrace": False,
            "diagnose": True,
        },
    ],
}
logger.configure(**config)


def deserializer(value):
    return json.loads(value)


async def producer(
    queue, topic, bootstrap_servers, group_id, _username, _password, _cert_file
):
    try:
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=deserializer,
        )

        await consumer.start()
        # Consume messages
        async for msg in consumer:
            try:
                await queue.join()
                await queue.put(msg)
            except asyncio.CancelledError:
                break
    except Exception as err:
        logger.error(err)
        sys.exit(1)
    finally:
        await consumer.stop()


async def periodic(interval, periodic_event):
    while True:
        await asyncio.sleep(interval)
        periodic_event.set()


async def mapper(queue, p_queue, periodic_event, batch_size):
    batch = list()
    while True:
        try:
            msg = await queue.get()
            queue.task_done()
            batch.append(msg)

            if len(batch) == batch_size:
                logger.info(f"Committing sized batch: {len(batch)} records")
                await p_queue.put((1, batch))
                await p_queue.join()
                batch.clear()

            if periodic_event.is_set():
                logger.info(f"Commiting periodic batch {len(batch)} records")
                await p_queue.put((0, batch))
                await p_queue.join()
                periodic_event.clear()
                batch.clear()

        except asyncio.CancelledError:
            break


async def streamer(p_queue):
    while True:
        try:
            batch = await p_queue.get()
            p_queue.task_done()
            logger.info(f"Processing batch {len(batch[1])}")
        except asyncio.CancelledError:
            break


async def shutdown(loop, signal=None):
    """Cleanup tasks tied to the service's shutdown."""
    if signal:
        logger.info(f"Received exit signal {signal.name}...")

    logger.info("Nacking outstanding messages")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    [task.cancel() for task in tasks]

    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info(f"Flushing metrics")
    loop.stop()


def run(*args):
    async def _run(
        _wid,
        _bs,
        _gid,
        _topic,
        _username,
        _password,
        _cert_file,
        _batch_size,
        _batch_interval,
        _bucket,
        _region,
        _num_workers,
        _num_worker_threads,
    ):
        tasks = None
        mp.current_process().name = f"Worker-{_wid+1}"
        logger.info(f"Starting {mp.current_process().name}")

        loop = asyncio.get_event_loop()
        for s in signals:
            loop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(shutdown(loop, signal=s))
            )

        try:
            queue = Queue()
            p_queue = PriorityQueue()
            periodic_event = asyncio.Event()

            producer_worker = producer(
                queue.async_q,
                _topic,
                _bs,
                _gid,
                _username,
                _password,
                _cert_file,
            )

            periodic_worker = asyncio.create_task(
                periodic(_batch_interval, periodic_event)
            )

            mapper_workers = [
                mapper(queue.async_q, p_queue.async_q, periodic_event, _batch_size)
                for _ in range(_num_workers * _num_worker_threads)
            ]

            stream_worker = [
                asyncio.create_task(streamer(p_queue.async_q))
                for _ in range(_num_workers * _num_worker_threads)
            ]

            workers = [
                producer_worker,
                periodic_worker,
                *mapper_workers,
                *stream_worker,
            ]

            tasks = await asyncio.gather(
                *workers,
                return_exceptions=True,
            )

            queue.close()
            await queue.wait_closed()
        except asyncio.CancelledError:
            if tasks:
                for task in tasks:
                    task.cancel()

    return asyncio.run(_run(*args))


async def k2s3(
    bs,
    gid,
    topic,
    username,
    password,
    cert_file,
    batch_size,
    batch_interval,
    bucket,
    region,
    num_workers,
    num_worker_threads,
):
    try:
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            loop = asyncio.get_running_loop()

            for s in signals:
                loop.add_signal_handler(
                    s, lambda s=s: asyncio.create_task(shutdown(loop, signal=s))
                )

            run_args = [
                bs,
                gid,
                topic,
                username,
                password,
                cert_file,
                batch_size,
                batch_interval,
                bucket,
                region,
                num_workers,
                num_worker_threads,
            ]
            tasks = [
                loop.run_in_executor(executor, run, i, *run_args)
                for i in range(num_workers)
            ]

            await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        pass


@app.command()
def main(
    bs: str = typer.Option(..., "--bootstrap-servers", "-bs"),
    gid: str = typer.Option(..., "--group-id", "-gid"),
    topic: List[str] = typer.Option(..., "--topic", "-t"),
    username: str = typer.Option(..., "--username", "-u"),
    password: str = typer.Option(..., "--password", "-p"),
    cert_file: Path = typer.Option(..., "--cert-file", "-c"),
    commit_size: int = typer.Option(..., "--commit-size", "-s"),
    commit_interval: int = typer.Option(..., "--commit-interval", "-i"),
    bucket: str = typer.Option(..., "--bucket", "-b"),
    region: str = typer.Option(..., "--region", "-r"),
    num_workers: int = typer.Option(
        psutil.cpu_count(logical=False), "--num-workers", "-W"
    ),
    num_worker_threads: int = typer.Option(1, "--num-worker-threads", "-T"),
):
    """
    Kafka to S3 Streamer
    """
    # upgrade event loop
    # to use libuv over native
    uvloop.install()

    cpu_count = psutil.cpu_count(logical=False)
    num_workers = cpu_count if num_workers > cpu_count else num_workers

    logger.info("Starting k2s3 consumer group using configuration...")
    logger.info(f"Bootstrap Servers: {bs}")
    logger.info(f"Consumer Group ID: {gid}")
    logger.info(f"Total Workers: {num_workers}")
    logger.info(f"Threads per Worker: {num_worker_threads}")

    asyncio.run(
        k2s3(
            bs,
            gid,
            topic,
            username,
            password,
            cert_file,
            commit_size,
            commit_interval,
            bucket,
            region,
            num_workers,
            num_worker_threads,
        )
    )
