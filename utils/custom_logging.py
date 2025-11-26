import logging
import datetime as dt
import bittensor as bt

class CustomColoredFormatter(logging.Formatter):
    COLORS = {
        'TRACE': "\033[35m",     # Magenta
        'DEBUG': "\033[36m",     # Cyan
        'INFO': "\033[37m",      # White
        'WARNING': "\033[33m",   # Yellow
        'ERROR': "\033[31m",     # Red
        'CRITICAL': "\033[41m",  # Red background
        'SUCCESS': "\033[32m",   # Green
        'RESET': "\033[0m",
    }

    def formatTime(self, record, datefmt=None):
        datetime = dt.datetime.fromtimestamp(record.created)
        return datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # milliseconds

    def format(self, record):
        if record.name.startswith("bittensor"):
            record.name = record.name.replace("bittensor", "bt", 1)

        levelname = record.levelname
        color = self.COLORS.get(levelname, "")
        reset = self.COLORS['RESET']
        record.levelname_colored = f"{color}{levelname.center(10)}{reset}"

        self._style._fmt = (
            "%(asctime)s | %(levelname_colored)s | %(filename)s:%(lineno)d | %(message)s"
        )

        return super().format(record)

def setup_bt_logging():
    for handler in bt.logging._handlers:
        handler.setFormatter(CustomColoredFormatter())
