"""
Base search engine interface.
"""
from abc import ABC, abstractmethod
from typing import List
import logging

logger = logging.getLogger(__name__)


class SearchEngine(ABC):
    """Abstract base class for search engines."""

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")
        self.url_titles: dict = {}  # side-channel: URL -> search result title

    @abstractmethod
    def search(self, query: str, max_results: int = 20) -> List[str]:
        """
        Search for URLs using the engine.

        Args:
            query: Search query
            max_results: Maximum number of results

        Returns:
            List of URLs
        """
        pass

    def is_available(self) -> bool:
        """
        Check if engine is available (e.g., has API key if needed).

        Returns:
            True if engine can be used
        """
        return True
