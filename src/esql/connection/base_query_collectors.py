from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generator

from empire_commons.types_ import JsonListType, JsonType


class BaseQueryCollectors(ABC):
    """
    Repository of collector methods
    """
    @staticmethod
    @abstractmethod
    def gather_all_records(cursor: Any, unused: int) -> JsonListType:
        """
        Returns all the records returned by a query, in the form of a list of JSON dictionaries
        :param cursor: The cursor
        :param unused:
        :return: A list of JSON dictionaries
        """
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def make_generator(cursor: Any, batch_size: int) -> Generator[JsonType, Any, None]:
        """
        Makes a generator from the returned records of a query. Each element is a JSON dictionary
        :param cursor: The cursor
        :param batch_size:
        :return:
        """
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def fetch_one_record(cursor: Any, unused: int) -> JsonType:
        """
        Returns a single JSON record
        :param cursor: The cursor
        :param unused:
        :return: The record
        """
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def fetch_n_records(cursor: Any, quantity: int) -> JsonListType:
        """
        Returns N records from the query
        :param cursor: The cursor
        :param quantity: The number of records to get
        :return: A list of JSON dictionaries
        """
        raise NotImplementedError()
