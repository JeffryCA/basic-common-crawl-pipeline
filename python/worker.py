import io
import json
from prometheus_client import start_http_server
import trafilatura
from warcio.archiveiterator import WARCIterator
from prometheus_client import Counter

from commoncrawl import BASE_URL, CCDownloader, Downloader
from rabbitmq import QUEUE_NAME, rabbitmq_channel


batch_counter = Counter("worker_batches", "Number of consumed batches")
filtered_docs_counter = Counter("worker_filtered_docs", "Number of filtered out documents")
processed_docs_counter = Counter("worker_processed_docs", "Number of processed documents")

def process_batch(downloader: Downloader, ch, method, _properties, body):
    print("Received batch of size", len(body))
    batch = json.loads(body)
    for item in batch:
        data = downloader.download_and_unzip(
            item["metadata"]["filename"],
            int(item["metadata"]["offset"]),
            int(item["metadata"]["length"]),
        )
        has_text = False
        for record in WARCIterator(io.BytesIO(data)):
            if record.rec_type == "response":
                text = trafilatura.extract(record.content_stream().read())
                if text and text.strip():
                    # TODO: process text
                    processed_docs_counter.inc()   
                    has_text = True
                    break # Assume one response per WARC record

        if not has_text: 
            filtered_docs_counter.inc()
    batch_counter.inc()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    start_http_server(9001)
    downloader = CCDownloader(BASE_URL)
    channel = rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: process_batch(
            downloader, ch, method, properties, body
        ),
    )
    channel.start_consuming()


if __name__ == "__main__":
    main()
