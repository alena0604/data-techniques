import requests
from datetime import datetime, timedelta
import time
from typing import Optional, List, Dict, Any
from bytewax.inputs import SimplePollingSource
from bytewax.connectors.stdio import StdOutSink
import bytewax.operators as op
from bytewax.dataflow import Dataflow
import logging
from bytewax_pipeline.backend.model import HackerNewsModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
HN_MAX_ITEM_URL = "https://hacker-news.firebaseio.com/v0/maxitem.json"
HN_ITEM_URL_TEMPLATE = "https://hacker-news.firebaseio.com/v0/item/{hn_id}.json"
FETCH_INTERVAL_SECONDS = 15


# Hacker News Input Source
class HackerNewsInput(SimplePollingSource):
    def __init__(
        self,
        interval: timedelta,
        align_to: Optional[datetime] = None,
        init_item: Optional[int] = None,
    ):
        super().__init__(interval, align_to)
        self.max_id = init_item
        logger.info(f"Initializing HackerNewsInput with starting item ID: {init_item}")

    def next_item(self) -> List[int]:
        """
        Fetch new item IDs from the Hacker News API between the last known max ID and the current max ID.
        """
        if not self.max_id:
            self.max_id = self._fetch_max_id()

        new_max_id = self._fetch_max_id()
        logger.info(f"Fetching articles between {self.max_id} and {new_max_id}")

        if self.max_id >= new_max_id:
            return []  # No new items

        ids = list(range(self.max_id + 1, new_max_id + 1))
        self.max_id = new_max_id
        return ids

    @staticmethod
    def _fetch_max_id() -> int:
        """
        Fetch the maximum item ID from Hacker News API.
        """
        try:
            response = requests.get(HN_MAX_ITEM_URL, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching max ID: {e}")
            time.sleep(5)
            return HackerNewsInput._fetch_max_id()


# Fetch Article Metadata
def download_metadata(hn_id: int) -> Dict[str, Any]:
    """
    Fetch metadata for a given Hacker News ID.
    """
    url = HN_ITEM_URL_TEMPLATE.format(hn_id=hn_id)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data:
            logger.warning(f"No data found for item ID {hn_id}. Skipping.")
            return {}
        return data
    except requests.RequestException as e:
        logger.error(f"Error fetching metadata for item ID {hn_id}: {e}")
        return {}


# Bytewax Dataflow
def run_hn_flow(init_item: Optional[int] = None) -> Dataflow:
    """
    Define the Bytewax dataflow for processing Hacker News articles.
    """
    flow = Dataflow("hacker_news_stream")

    # Input source fetching new articles
    inp = op.input(
        "input", flow, HackerNewsInput(timedelta(seconds=15), None, init_item)
    )

    # Flatten the list of article IDs
    article_ids = op.flat_map("flatten_ids", inp, lambda ids: ids)

    # Fetch metadata for each article ID
    article_metadata = op.map("fetch_metadata", article_ids, download_metadata)

    # Convert metadata to HackerNewsModel, then transform to CommonDocument
    common_docs = op.map(
        "to_common_document",
        article_metadata,
        lambda metadata: HackerNewsModel(**metadata).to_common(),
    )

    op.output("std-out", common_docs, StdOutSink())

    return flow
