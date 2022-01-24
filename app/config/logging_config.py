from typing import Any

from colorama import init as colorama_init
from colorama import Fore, Back, Style


class Logger:

    def __init__(self):
        """
        FROM COLORAMA DOCS:
            On Windows, calling init() will filter ANSI escape sequences out of any text sent to stdout or stderr,
                and replace them with equivalent Win32 calls.

            On other platforms, calling init() has no effect (unless you request other optional functionality);
                By design, this permits applications to call init() unconditionally on all platforms,
                after which ANSI output should just work.
        """
        colorama_init()
        self.max_len = 500

    @staticmethod
    def info(message: Any, errors=None):
        if errors is not None:
            Logger.error(f"{message},{errors}")
        else:
            print(message)

    @staticmethod
    def success(message: str):
        print(f"{Fore.LIGHTGREEN_EX}{message}{Style.RESET_ALL}")

    @staticmethod
    def warn(message: str):
        print(f"{Fore.LIGHTYELLOW_EX}{message}{Style.RESET_ALL}")

    @staticmethod
    def magenta(message: str):
        print(f"{Fore.MAGENTA}{message}{Style.RESET_ALL}")

    @staticmethod
    def blue(message: str):
        print(f"{Fore.BLUE}{message}{Style.RESET_ALL}")

    @staticmethod
    def error(message: str):
        print(f"{Fore.RED}{message}{Style.RESET_ALL}")


# export
log = Logger()
