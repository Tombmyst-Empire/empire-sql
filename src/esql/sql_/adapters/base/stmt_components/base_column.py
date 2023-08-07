from __future__ import annotations

from typing import TypeVar, Generic

from empire_commons.abc_ import *


ColumnDataType_ = TypeVar('ColumnDataType_')


class BaseColumn(Generic[ColumnDataType_], ABC):
    __slots__ = (
        '_name',
        '_type'
    )

    def __init__(self, name: str, type_: ColumnDataType_):
        self._name: str = name
        self._type: ColumnDataType_ = type_

    @property
    def type_(self) -> ColumnDataType_:
        return self._type

    @property
    def name(self) -> str:
        return self._name

    @abstractmethod
    def is_of_type(self, *type_: type) -> bool:
        """
        TODO: parse all type_ to SnowflakeDataType
        :param type_:
        :return:
        """
        raise NotImplementedError()

    @abstractmethod
    def build(self) -> str:
        raise NotImplementedError()
