import logging
import uuid
from abc import abstractmethod, ABC
from datetime import datetime
from typing import Union

from requests import get
from posthog import Client

log = logging.getLogger("resoto.cloud2sql")


class AnalyticsEventSender(ABC):
    """
    Abstract event sender used to send analytics events.
    """

    @abstractmethod
    def capture(self, kind: str, **context: Union[str, int, float]) -> None:
        pass

    def flush(self) -> None:
        pass


class NoEventSender(AnalyticsEventSender):
    """
    Use this sender to not emit any events other than writing it to the log file.
    """

    def capture(self, kind: str, **context: Union[str, int, float]) -> None:
        log.debug(f"Capture event {kind}: {context}")


class PosthogEventSender(AnalyticsEventSender):
    """
    This sender is used to collect analytics events in posthog.
    """

    def __init__(self) -> None:
        self.client = Client(host="https://analytics.some.engineering", api_key="")
        self.uid = uuid.uuid4()

    def capture(self, kind: str, **context: Union[str, int, float]) -> None:
        if self.client.api_key == "":
            api_key = get("https://cdn.some.engineering/posthog/public_api_key").text.strip()
            self.client.api_key = api_key
            for consumer in self.client.consumers:
                consumer.api_key = api_key
        self.client.capture(
            distinct_id=self.uid,
            event="cloud2sql." + kind,
            properties={
                **context,
                "source": "cloud2sql",
                "run_id": self.uid,
            },
            timestamp=datetime.now(),
        )

    def flush(self) -> None:
        self.client.flush()
