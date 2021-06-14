# -*- coding: utf-8 -*-
"""
MODULE: Kafka to S3 Streamer
"""


import sys
import random
import signal
import asyncio
from pathlib import Path
import multiprocessing as mp

import attr
import typer
import psutil
from loguru import logger
from janus import Queue, PriorityQueue
from concurrent.futures import ProcessPoolExecutor

app = typer.Typer()
signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT, signal.SIGQUIT)

config = {
    "handlers": [
        {
            "sink": sys.stdout,
            "format": "{time:YYYY-MM-DD HH:mm:ss} - {process.name} – [{process.id}] - {level} - {message}",
        },
    ],
}
logger.configure(**config)


@attr.s
class Message:
    id = attr.ib()


async def producer(queue):
    while True:
        try:
            msg = Message(id=random.randint(1, 10))
            await queue.join()
            await queue.put(msg)
        except asyncio.CancelledError:
            break


async def periodic(interval, periodic_event):
    while True:
        await asyncio.sleep(interval)
        periodic_event.set()


async def consumer(queue, p_queue, periodic_event, batch_size):
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


def run(
    name, topic, username, password, cert_file, batch_size, batch_interval, num_workers
):
    async def _run(
        _process_idx,
        _topic,
        _username,
        _password,
        _cert_file,
        _batch_size,
        _batch_interval,
        _num_workers,
    ):
        tasks = None
        mp.current_process().name = f"Worker-{_process_idx+1}"
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

            producer_worker = producer(queue.async_q)
            consumer_worker = [
                consumer(queue.async_q, p_queue.async_q, periodic_event, _batch_size)
                for _ in range(_num_workers * 2)
            ]
            stream_worker = [
                asyncio.create_task(streamer(p_queue.async_q))
                for _ in range(_num_workers * 2)
            ]
            periodic_worker = asyncio.create_task(
                periodic(_batch_interval, periodic_event)
            )
            workers = [
                producer_worker,
                *consumer_worker,
                *stream_worker,
                periodic_worker,
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

    return asyncio.run(
        _run(
            name,
            topic,
            username,
            password,
            cert_file,
            batch_size,
            batch_interval,
            num_workers,
        )
    )


async def k2s3(
    topic, username, password, cert_file, batch_size, batch_interval, num_workers
):
    try:
        # start producers and consumers
        # in a process pool executor
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            loop = asyncio.get_running_loop()

            for s in signals:
                loop.add_signal_handler(
                    s, lambda s=s: asyncio.create_task(shutdown(loop, signal=s))
                )
            tasks = [
                loop.run_in_executor(
                    executor,
                    run,
                    i,
                    topic,
                    username,
                    password,
                    cert_file,
                    batch_size,
                    batch_interval,
                    num_workers,
                )
                for i in range(num_workers)
            ]

            await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        pass


@app.command()
def main(
    bs: str = typer.Option(..., "--bootstrap-servers", "-bs"),
    gid: str = typer.Option(..., "--group-id", "-gid"),
    topic: str = typer.Option(..., "--topic", "-t"),
    username: str = typer.Option(..., "--username", "-u"),
    password: str = typer.Option(..., "--password", "-p"),
    cert_file: Path = typer.Option(..., "--cert-file", "-c"),
    commit_size: int = typer.Option(..., "--commit-size", "-s"),
    commit_interval: int = typer.Option(..., "--commit-interval", "-i"),
    bucket: str = typer.Option(..., "--bucket", "-b"),
    num_workers: int = typer.Option(1, "--num-workers", "-w"),
):
    """
    Kafka to S3 Streamer
    """

    cpu_count = psutil.cpu_count(logical=False)
    num_workers = cpu_count if num_workers > cpu_count else num_workers

    logger.info("Starting k2s3 consumer group using configuration...")
    logger.info(f"Bootstrap Servers: {bs}")
    logger.info(f"Consumer Group ID: {gid}")
    logger.info(f"Num Workers: {num_workers}")

    asyncio.run(
        k2s3(
            topic,
            username,
            password,
            cert_file,
            commit_size,
            commit_interval,
            num_workers,
        )
    )
