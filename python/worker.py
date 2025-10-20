import argparse
import io
import json
import numpy as np
from dotenv import load_dotenv
from prometheus_client import start_http_server
import trafilatura
from transformers import AutoTokenizer
from warcio.archiveiterator import WARCIterator
from prometheus_client import Counter, write_to_textfile, REGISTRY

from commoncrawl import BASE_URL, CCDownloader, Downloader
from object_store import ensure_bucket_exists
from shard_writer import ShardWriter
from rabbitmq import QUEUE_NAME, rabbitmq_channel


batch_counter = Counter("worker_batches", "Number of consumed batches")
filtered_docs_counter = Counter("worker_filtered_docs", "Number of filtered out documents", ["reason"])
processed_docs_counter = Counter("worker_processed_docs", "Number of processed documents")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Worker")
    parser.add_argument(
        "--s3-prefix", type=str, help="S3 prefix for input files", required=True
    )
    parser.add_argument(
        "--max-samples-per-shard", type=int, default=1000, help="Maximum samples per shard"
    )
    parser.add_argument(
        "--max-shard-size", type=int, default=100 * 1024 * 1024, help="Maximum shard size in bytes"
    )
    return parser.parse_args()


def get_tokenizer() -> AutoTokenizer:
    tok = AutoTokenizer.from_pretrained("gpt2") # In general choose a tokenizer suited for target model task (finetune / train) and language
    if tok.pad_token is None:
        tok.pad_token = tok.eos_token
    return tok

def process_sample(item, text, tok: AutoTokenizer, writer: ShardWriter) -> None:
    # text -> token ids
    ids = tok.encode(text.strip(), add_special_tokens=False) # TODO: split into sequences of max length
    # tokens -> .npy bytes (uint32)
    arr = np.asarray(ids, dtype=np.uint32)
    tokens_buf = io.BytesIO()
    np.save(tokens_buf, arr, allow_pickle=False) # prevent potential malicious code from being executed when loading data
    tokens_payload = tokens_buf.getvalue()
    # Save some metadata
    meta = {
        "tokenizer": {"name": tok.name_or_path, "eos_id": tok.eos_token_id},
        "num_tokens": int(arr.size),
        "source": "commoncrawl",
        "crawl": item["metadata"]["filename"],
        "offset": int(item["metadata"]["offset"]),
        "length": int(item["metadata"]["length"]),
        "lang": "en",
        # TODO add document hash of normalized text
    }
    meta_payload = json.dumps(meta, ensure_ascii=False).encode("utf-8")
    # Write sample to tar
    writer.add_sample(tokens_payload, meta_payload)
    # Update counter
    processed_docs_counter.inc()   

def process_batch(downloader: Downloader, ch, method, _properties, body, writer : ShardWriter) -> None:
    print("Received batch of size", len(body))
    batch = json.loads(body)
    tok = get_tokenizer()
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
                    has_text = True
                    if not (500 <= len(text) <= 1_000_000):
                        filtered_docs_counter.labels(reason="invalid_length").inc()
                        break
                    process_sample(item, text, tok, writer)
                    break  # Assume one response per WARC record

        # Update counter
        if not has_text: 
            filtered_docs_counter.labels(reason="no_text").inc()

    batch_counter.inc()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    load_dotenv()
    start_http_server(9001)
    ensure_bucket_exists()
    args = parse_args()
    writer = ShardWriter(
        upload_prefix=args.s3_prefix, 
        max_shard_size=args.max_shard_size,
        max_samples_per_shard=args.max_samples_per_shard,
    )
    downloader = CCDownloader(BASE_URL)
    channel = rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: process_batch(
            downloader, ch, method, properties, body, writer
        ),
    )
    try:
        channel.start_consuming()
    finally:
        writer.flush()
        write_to_textfile("./temp/worker_metrics.txt", REGISTRY)


if __name__ == "__main__":
    main()
