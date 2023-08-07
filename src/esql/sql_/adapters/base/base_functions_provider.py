from __future__ import annotations

from typing import Any

from empire_commons.abc_ import *


class BaseFunctionsProvider(ABC):
    @abstractstaticmethod
    def abs_(value: Any) -> str:
        raise NotImplementedError()
