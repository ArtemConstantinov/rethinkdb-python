# Copyright 2022 RethinkDB
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This file incorporates work covered by the following copyright:
# Copyright 2010-2016 RethinkDB, all rights reserved.

"""
This module contains the REPL's helper class to manage the established connection
on the local thread.
"""
from __future__ import annotations
__all__ = ("Repl",)

import threading
from typing import TYPE_CHECKING
import warnings
if TYPE_CHECKING:
    from .connection import Connection


# region Warning setup

class ReqlUserWarning(UserWarning):
    ...


def formatter(message, category, filename, lineno, *_) -> str:
    if category is ReqlUserWarning:
        return f"\n\33[103m{category.__name__}\33[0m: {message}\n\n"
    return f"\n\33[103m{category.__name__}\33[0m: {message}\n\n"


warnings.formatwarning = formatter

# endregion

REPL_CONNECTION_ATTRIBUTE: str = "conn"


class Repl:
    """
    REPL helper class to get, set and clear the connection on the local thread.
    """
    # Those variables must act as a singleton
    thread_data = threading.local()
    is_repl_active = False

    def get_connection(self) -> "Connection" | None:
        """
        Get connection object from local thread.
        """

        return getattr(self.thread_data, REPL_CONNECTION_ATTRIBUTE, None)

    def set_connection(self, connection: "Connection") -> None:
        """
        Set connection on local thread and activate REPL.
        """
        warnings.warn("Using Repl is not thread safe.", ReqlUserWarning)
        self.is_repl_active = True
        setattr(self.thread_data, REPL_CONNECTION_ATTRIBUTE, connection)

    def clear_connection(self) -> None:
        """
        Clear the local thread and deactivate REPL.
        """

        self.is_repl_active = False

        if hasattr(self.thread_data, REPL_CONNECTION_ATTRIBUTE):
            delattr(self.thread_data, REPL_CONNECTION_ATTRIBUTE)
