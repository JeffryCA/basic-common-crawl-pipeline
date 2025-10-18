from abc import ABC, abstractmethod
import json
import argparse
from math import inf
from typing import Any, Mapping, Sequence
from prometheus_client import Counter, start_http_server

from commoncrawl import (
    BASE_URL,
    CRAWL_PATH,
    CCDownloader,
    CSVIndexReader,
    Downloader,
    IndexReader,
)
from rabbitmq import QUEUE_NAME, MessageQueueChannel, RabbitMQChannel


batch_counter = Counter("batcher_batches", "Number of published batches")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batcher")
    parser.add_argument(
        "--cluster-idx-filename", type=str, help="Input file path", required=True
    )
    parser.add_argument(
        "--batch-size", type=int, help="Batch size", required=False, default=50
    )
    parser.add_argument(
        "--max-batches", type=int, help="Max number of URLs", required=False, default=inf
    )
    return parser.parse_args()


def publish_batch(
    channel: MessageQueueChannel,
    batch: Sequence[Mapping[str, Any]],
) -> None:
    print("Pushing batch of size", len(batch))
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(batch),
    )
    batch_counter.inc()


def process_index(
    index: IndexReader,
    channel: MessageQueueChannel,
    downloader: Downloader,
    batch_size: int,
    max_batches: int 
) -> None:
    found_urls = []
    published_batches = 0
    for cdx_chunk in index:
        data = downloader.download_and_unzip(
            cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
        ).decode("utf-8")
        for line in data.split("\n"):
            if line == "":
                continue
            values = line.split(" ")
            metadata = json.loads("".join(values[2:]))
            if (
                "languages" in metadata
                and "eng" in metadata["languages"]
                and metadata["status"] == "200"
            ):
                found_urls.append(
                    {
                        "surt_url": values[0],
                        "timestamp": values[1],
                        "metadata": metadata,
                    }
                )
            if len(found_urls) >= batch_size:
                publish_batch(channel, found_urls)
                found_urls = []
                published_batches += 1

            if published_batches >= max_batches:
                print("Early stopping")
                return

    if len(found_urls) > 0 and published_batches < max_batches:
        publish_batch(channel, found_urls)


def main() -> None:
    args = parse_args()
    start_http_server(9000)
    channel = RabbitMQChannel()
    downloader = CCDownloader(f"{BASE_URL}/{CRAWL_PATH}")
    index_reader = CSVIndexReader(args.cluster_idx_filename)
    process_index(index_reader, channel, downloader, args.batch_size, args.max_batches)


if __name__ == "__main__":
    main()
