"""Compatibility entry point for the Binance basis monitor."""
from __future__ import annotations

import asyncio
import os
import websockets

from monitor.settings import *
from monitor.models import *
from monitor.funding.core import *
from monitor.basis.core import *
from monitor.network.core import *
from monitor.terminal.pages import *
from monitor.app import run

if __name__ == "__main__":
    if os.name == "nt":
        os.system("")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
