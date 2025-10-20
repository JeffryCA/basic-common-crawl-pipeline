import json
import argparse
from typing import Any, Mapping, Optional, Sequence
from prometheus_client import Counter, start_http_server, write_to_textfile, REGISTRY

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
filtered_docs_counter = Counter("batcher_filtered_docs", "Number of filtered out documents", ['reason'])
rejected_docs_counter = Counter("batcher_rejected_docs", "Number of rejected documents")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batcher")
    parser.add_argument(
        "--cluster-idx-filename", type=str, help="Input file path", required=True
    )
    parser.add_argument(
        "--batch-size", type=int, help="Batch size", required=False, default=50
    )
    parser.add_argument(
        "--max-batches", type=int, help="Max number of batches", required=False, default=None
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
    max_batches: Optional[int] = None
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

            # Filtering criteria
            is_eng = "languages" in metadata and "eng" in metadata["languages"]
            is_status_200 = metadata.get("status") == "200"

            reasons = []
            if not is_eng:
                reasons.append("non_english")
            if not is_status_200:
                reasons.append("status_not_200")
            if reasons:
                for reason in reasons:
                    filtered_docs_counter.labels(reason=reason).inc()
                rejected_docs_counter.inc()
                continue

            # If passed all filters, add to batch
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

            if max_batches and published_batches >= max_batches:
                print("Early stopping")
                return

    if len(found_urls) > 0 and (max_batches is None or published_batches < max_batches):
        publish_batch(channel, found_urls)


def main() -> None:
    args = parse_args()
    start_http_server(9000)
    channel = RabbitMQChannel()
    downloader = CCDownloader(f"{BASE_URL}/{CRAWL_PATH}")
    index_reader = CSVIndexReader(args.cluster_idx_filename)
    try:
        process_index(index_reader, channel, downloader, args.batch_size, args.max_batches)
    finally:
        write_to_textfile("./temp/batcher_metrics.txt", REGISTRY)


if __name__ == "__main__":
    main()
