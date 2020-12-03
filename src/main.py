#!/usr/bin/env python

from environs import Env
from multiprocessing import Process, Manager, Queue  # , freeze_support
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# from azure.storage.blob.aio import BlobServiceClient
# import time
# import os

# import numpy as np
# rate
# mean
# stdeev
# mu, sigma, size=...
# https://towardsdatascience.com/unraveling-pythons-threading-mysteries-e79e001ab4c


def run():
    env = Env()
    env.read_env()

    jobs: Process = []
    queue = Manager().Queue()
    jobs.append(Process(target=generator_worker, args=(queue,)))
    jobs.append(Process(target=queue_reader, args=(queue,)))

    for j in jobs:
        j.start()

    for j in jobs:
        j.join()
        print(j)


def generator_worker(queue: Queue,  seed: int = 42, chunk_size: int = 5, rate_ms: float = 100.0, mean: float = 1.0, std_dev: float = 1.0):
    from numpy.random import Generator, PCG64
    rg = Generator(PCG64(seed=seed))
    for i in range(0, 10):
        from time import sleep
        sleep(rate_ms/1000)
        rv_set = rg.normal(mean, std_dev, size=chunk_size)
        queue.put(rv_set)

    queue.all_tasks_done = True  # TODO: check if useful
    queue.put('DONE')


def queue_reader(queue: Queue):
    sender = BlobSender()
    while True:
        task = queue.get()
        if str(task) == 'DONE':
            break

        sender.send_data(task)
        #print(task)


class BlobSender:

    def __init__(self) -> None:
        super().__init__()
        env = Env()
        env.read_env()

        with env.prefixed('BLOBSTORAGE_'):
            self.storage_url = env.url('URL')

        default_credential = DefaultAzureCredential()
        client = BlobServiceClient(self.storage_url.geturl(), credential=default_credential)

    def send_data(self, data):
        print(data)


if __name__ == '__main__':
    # freeze_support()
    run()
