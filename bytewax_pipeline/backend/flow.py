import requests
from datetime import datetime, timedelta
import time
from typing import Optional
from bytewax.inputs import SimplePollingSource
from bytewax.connectors.stdio import StdOutSink
import bytewax.operators as op
from bytewax.dataflow import Dataflow
import logging
from bytewax_pipeline.backend.model import HackerNewsModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants for formatting
CURRENT_TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


# Hacker News Fetching Logic


class HackerNewsInput(SimplePollingSource):
    def __init__(
        self,
        interval: timedelta,
        align_to: Optional[datetime] = None,
        init_item: Optional[int] = None,
    ):
        super().__init__(interval, align_to)
        logger.info(f"Starting with item ID: {init_item}")
        self.max_id = init_item

    def next_item(self):
        """
        Get all new items from Hacker News API between
        the last known max ID and the current max ID.
        """
        if not self.max_id:
            self.max_id = requests.get(
                "https://hacker-news.firebaseio.com/v0/maxitem.json"
            ).json()

        new_max_id = requests.get(
            "https://hacker-news.firebaseio.com/v0/maxitem.json"
        ).json()
        logger.info(f"Fetching articles between {self.max_id} and {new_max_id}")
        ids = [int(i) for i in range(self.max_id, new_max_id)]
        self.max_id = new_max_id
        return ids


def download_metadata(hn_id):
    """
    Given a Hacker News ID, fetch the metadata from the Hacker News API.
    """
    req = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json")
    if not req.json():
        logger.warning(f"Error retrieving item {hn_id}, retrying...")
        time.sleep(0.5)
        return download_metadata(hn_id)
    return req.json()


def run_hn_flow(init_item):
    """
    Define the Bytewax dataflow for processing Hacker News articles.
    """
    flow = Dataflow("hacker_news_stream")

    # Input source fetching new articles
    inp = op.input(
        "input", flow, HackerNewsInput(timedelta(seconds=15), None, init_item)
    )
    # op.inspect("inspect_fetched_articles", inp)

    # Flatten the list of article IDs
    article_ids = op.flat_map("flatten_ids", inp, lambda ids: ids)

    # op.inspect("flatten_ids_dbg", article_ids)

    # Fetch metadata for each article ID
    article_metadata = op.map("fetch_metadata", article_ids, download_metadata)

    # op.inspect("fetch_metadata_dbg", article_metadata)

    # Convert metadata to HackerNewsModel, then transform to CommonDocument
    common_docs = op.map(
        "to_common_document",
        article_metadata,
        lambda metadata: HackerNewsModel(**metadata).to_common(),
    )

    # Inspect the processed CommonDocument
    # op.inspect("processed_common_document", common_docs)

    op.output("std-out", common_docs, StdOutSink())

    return flow
