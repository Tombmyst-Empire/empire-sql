from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseValues(ABC):
    @staticmethod
    @abstractmethod
    def prepare_value_by_deducing_python_type(value: Any, *, should_parse_json: bool = False) -> str:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def prepare_value_maybe_json_by_deducing_python_type(value: Any) -> str:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def prepare_value_by_type(value: Any, type_: type, *, should_parse_json: bool = False) -> str:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def prepare_value_maybe_json_by_type(value: Any, type_: type) -> str:
        raise NotImplementedError()
