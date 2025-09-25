"""Dirlin (Directory Handler), set of tools to help process local files."""

__version__ = "0.4.0"

from src.dirlin.core.api import (
    DirlinFormatter,  # formatting functions
    Document,  # special dataframe wrapper
    TqdmLoggingHandler  # logger
)

from dirlin.folder import (
    Folder,  # directory handling
    Directory,  # pre-made Folder manager
    Path,  # pathlib.Path,
)

import pandas  # using the Pandas library
import numpy  # using the numpy lib


__all__ = [
    "DirlinFormatter",
    "Document",
    "TqdmLoggingHandler",
    "Folder",
    "Directory",
    "Path",
    "pandas",
    "numpy"
]
