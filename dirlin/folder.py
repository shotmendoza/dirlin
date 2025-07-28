"""For the classic Dirlin Folders"""
import logging
import os.path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path, PosixPath
from typing import Optional
from urllib.parse import urlparse
from urllib.error import HTTPError

import pandas as pd
import pandas.errors

import chardet
from tqdm import tqdm

from dirlin.core.api import Document, DirlinFormatter, TqdmLoggingHandler


class Directory:
    def __init__(self, path: str | Path | None = None, initialize_posix_path: bool = True):
        """object used for directory level data wrangling. Can think of directories as a collection
        of Folders, where the Folder object handles different functionality for processing, while the
        Directory object keeps them organized.

        For macOS, we have extra fields we can use - DOWNLOADS, DESKTOP, DOCUMENTS, DEVELOPER
        if available.

        ...

        Attributes:
            - path: the directory path the folder is set to. If left as None, the path will be set to the downloads
            folder for macOS. Windows currently not supported.
            - initialize_posix_path: assumes the script is on a macOS directory

        :param path: initializes a Folder.folder FolderPath object
        :param initialize_posix_path: initializes default macOS folders
        """
        # [Part 1] setting up constants
        _curr_folder_directory = Path.cwd().home()

        # [Part 2] Loggers
        self.logger = logging.getLogger("dirlin.core.directory")
        self.logger.setLevel(logging.INFO)
        _tqdm_handler = TqdmLoggingHandler()
        self.logger.addHandler(_tqdm_handler)

        # [Part 3] setup placeholder folders
        self.DOWNLOADS: Folder | None = None
        self.DESKTOP: Folder | None = None
        self.DOCUMENTS: Folder | None = None
        self.DEVELOPER: Folder | None = None

        # adding the macOS only directory paths, so we don't have to define these in the future and they are included
        if initialize_posix_path is True:
            if isinstance(_curr_folder_directory, PosixPath):
                self.logger.info(f"Adding macOS specific default directories...")
                self.DOWNLOADS = Folder(_curr_folder_directory / "Downloads")
                self.DESKTOP = Folder(_curr_folder_directory / "Desktop")
                self.DOCUMENTS = Folder(_curr_folder_directory / "Documents")

                try:
                    self.DEVELOPER = Folder(_curr_folder_directory / "Developer")
                except AttributeError:
                    self.logger.warning(f"Developer folder has not been created yet on this mac.")

        if path is None:
            path = self.DOWNLOADS.path

        self.folder: Folder = Folder(path)
        """argument given by user pointing to a specific directory"""

    def __truediv__(self, other) -> Path:
        return self.folder / other


class Folder:
    def __init__(self, path: Path | str | None = None):
        """utility object that allows for handling files inside of folders.

        Dataframe Functions:
            - open() : opens a path file and converts it into a dataframe
            - open_recent(): opens the most recent file as a dataframe based on naming conventions
            - find_and_combine(): finds all files that follow naming conventions and creates a single dataframe
            - as_map(): creates a dictionary based on two columns from dataframe
            - index_files(): creates a list of paths based on file_ext or file suffixes (.csv, .xlsx, etc.)

        ...

        Document Functions:
            - open_as_document(): opens a path file and converts it into a document object
            - open_recent_as_document(): opens the most recent Path file and converts it into a document object

        ...

            Attributes:
                - path: the directory path the folder is set to. If left as None, the path will be set to the downloads
                folder for macOS. Windows currently not supported.
                - initialize_posix_path: assumes the script is on a macOS directory

        :param path: Path to the Folder. This is the directory the functions will use to search files in
        """
        # [Part 1] Setting up the Path based properties
        if isinstance(path, str):
            path = Path(path)
        self.path = path

        if not self.path.is_dir():
            raise ValueError(f"Expected a path to a folder / directory. Got {self.path}.")

        self.path_open_recent: Path | None = None
        """stores the most recent path used on self.open_recent or self.open_recent_as_document()"""

        # [Part 2] Setting up the Utility Properties
        # [2.1] the string formatting for parsing and formatting args
        self._format: DirlinFormatter = DirlinFormatter()
        """used for parsing and formatting the user args"""

        # [2.2] cache utility property for storing the results of get_all_files
        self._cached_get_all_files: (str, list[Path]) | None = None
        """placeholder for storing the most recently run get_all_files results. (filename_pattern, list[Path])"""

        # [2.3] Logging utility class and setting up logging
        self.logger = logging.getLogger("dirlin.core.folder")
        """used for logging to the console, or, in the future, logging to a file"""
        self.logger.setLevel(logging.INFO)

        # [2.3.1] adding the TQDM Logging
        tqdm_handler = TqdmLoggingHandler()
        self.logger.addHandler(tqdm_handler)

    def __repr__(self):
        return f"{self.path}"

    def __str__(self):
        return f"{self.path}"

    def __truediv__(self, other) -> Path:
        return Path(self.path) / other

    @staticmethod
    def _path_param_fits(
            path: Path,
            days: int | None = None
    ) -> bool:
        """helper function for _find_all_files that will parse the string and determine whether
        it belongs in the final results.
        """
        # [Part 1] Create the masks for the type of files we want to keep
        mask_not_temp_lock = not path.name.startswith("~")
        mask_not_hidden = not path.name.startswith(".")

        # [Part 2] Return if we have no Dates we need to handle
        if days is None:
            results = all((mask_not_temp_lock, mask_not_hidden))
            return results

        # [Part 3] handling dates and most recent files
        date_modified = date.fromtimestamp(path.stat().st_mtime)
        date_from_n = date.today() - timedelta(days=days)

        # [Part 4] return the results of the masks with date consideration
        mask_in_date_range = date_modified >= date_from_n
        results = all((mask_not_temp_lock, mask_not_hidden, mask_in_date_range))
        return results

    def get_all_files(
            self,
            filename_pattern: str,
            with_asterisks: bool = True,
            recurse: bool = False,
            days: int | None = None,
            use_cached: bool = True,
    ) -> list[Path]:
        """function for finding all files that follow certain naming conventions.
        Will search a folder for the filename and return all Paths that match (i.e. list[.xlsx, .xlsx])

        Used in _find_recent_files(), find_and_combine()

        :param filename_pattern: the naming convention of the file, will look in the specified directory
        :param days: looks back past x number of days. If looking in 12/31, then days=5 would look back to 12/26 files
        :param with_asterisks: defaults to True, adds the asterisks at the end of the filename_pattern arg.
        :param recurse: whether to recurse through sub-folders
        :param use_cached: whether to use cached results or not. Speeds up repetitive operations. Make sure to cache
        only if the instance wants to look into the same filename pattern.
        :return: Path object that meets the parameters
        """

        # [Part 0]: the happy path using the cached version
        try:
            # 2025.06.07 caused an error saying None is unscripable, so we can assume if this mask works
            # that self._cached_get_all_files is not None
            cached_previously_and_same_filename_pattern = filename_pattern == self._cached_get_all_files[0]
        except TypeError:
            cached_previously_and_same_filename_pattern = False

        if use_cached is True and cached_previously_and_same_filename_pattern:
            return self._cached_get_all_files[1]
        # ===note===: below will only run if the use_cached function is False or this function was never used.

        # [Part 1]: create mapping for the different user input
        # [1.1] determines whether to include asterisks in filename pattern. Shortcut for getting all files
        asterisks_mapping = {
            True: f"{filename_pattern}*",
            False: filename_pattern
        }
        # [1.2] determines whether to recurse through folders. Allows us to keep same line.
        # ===note===: the value returns a function. One searches recursively and the other searches in the given folder
        recurse_fn_mapping = {
            True: self.path.rglob,
            False: self.path.glob,
        }

        # [Part 2] checking for user arguments and raising an error if mismatch
        if not with_asterisks and filename_pattern == "":
            raise ValueError(f"filename_pattern cannot be left as default if with_asterisks parameter is set to False")

        # [Part 3]: I/O with the directory and get list of all Paths based on filename pattern
        files = [
            f for f in recurse_fn_mapping[recurse](pattern=asterisks_mapping[with_asterisks])
            if self._path_param_fits(f, days)
        ]

        # [Part 4]: sort the files and return the list of Path
        files = sorted(files, key=os.path.getmtime, reverse=True)

        # [4.1] the cached values for get_all_files
        self._cached_get_all_files = (filename_pattern, files.copy())
        return files

    def _find_recent_file(
            self,
            filename_pattern: str,
            days: int = 30,
            with_asterisks: bool = True,
            recurse: bool = False) -> Path:
        """
        Base function for finding the most recently modified file.
        Used in open_recent() and open_recent_as_document()

        :param filename_pattern: the naming convention of the file, will look in the specified directory
        :param days: looks back past x number of days. If looking in 12/31, then days=5 would look back to 12/26 files
        :param with_asterisks: defaults to True, adds the asterisks at the end of the filename_pattern arg.
        :param recurse: whether to recurse through sub-folders
        :return: Path object that meets the parameters
        """
        files = self.get_all_files(
            filename_pattern=filename_pattern,
            with_asterisks=with_asterisks,
            recurse=recurse,
            days=days
        )
        try:
            most_recent_file = Path(files[0])
            return most_recent_file
        except IndexError:
            raise FileNotFoundError(
                f"No reports found in '{self.path.parent.name}/{self.path.name}' directory in the past {days} days.")

    def _combine_in_order(self, files: list[Path], *args, **kwargs) -> list[pd.DataFrame]:
        results = []
        with tqdm(total=len(files), desc="Combining PDF Files", position=0) as pbar:
            for file in files:
                pbar.set_description(f"Adding: {file}")
                try:
                    temp_df = self.open(file, *args, **kwargs)
                except Exception as e:
                    raise e
                if temp_df is None:
                    temp_df = pd.DataFrame()
                results.append(temp_df)
                pbar.update()
            pbar.set_description(f"Combining Excel files completed...")
        return results

    def _combine_in_parallel(self, files: list[Path], *args, **kwargs) -> list[pd.DataFrame]:
        # [3.2] Loop in Parallel
        results = []
        with ThreadPoolExecutor() as executor:  # for running in parallel
            futures = {
                executor.submit(self.open, file, True, *args, **kwargs): file
                for file in files if not file.is_dir()
            }
            pbar = tqdm(total=len(futures), desc="Combining Excel Files", position=0)

            for future in as_completed(futures):
                try:
                    # [3.2.1] Adding filepath to progress bar
                    file_path = futures[future]
                    pbar.set_description(f"Adding: {file_path}")

                    # [3.2.2] Adding dataframe to the concat list
                    df = future.result()
                    results.append(df)

                    # [3.2.3] Updating the progress bar
                    pbar.update()
                except Exception as e:
                    raise e
            pbar.set_description(f"Combining Excel files completed...")
        return results

    def open(self, file_path: str | Path, add_source: bool = False, *args, **kwargs) -> pd.DataFrame:
        """
        Opens a string or pathlib.Path object into a pandas.DataFrame object.
        Currently, reads .xlsx, .xls, .csv, .txt, .json, .pdf file extensions, and will raise a
        KeyError for any other file types.

        ...

        kwargs for Excel Sheets
            - sheet_name: str which tab to parse

        ...

        kwargs for PDF files
            - file_path: Path,
            - field_names: list[str] | None = None,
            - skip_first_row: Literal['page', 'pdf'] | None = None,
            - table_settings: dict | None = None,
            - remove_repeated_keywords: str | None = None,

        ...

        :param file_path: path to the file you want to open
        :param add_source: determines whether to add the `From` (source) field when opening the file
        :param args: see documentation for pd.DataFrame object
        :param kwargs: see documentation for pd.DataFrame object / check documentation on this function above
        :return: pd.DataFrame object of the file
        """
        if isinstance(file_path, str):
            file_path = Path(file_path)

        if not file_path.exists():
            file_path = self.path / file_path

        # Valid File Types
        _excel_types = (".xlsx", ".xls", ",xlsb")
        _text_types = (".txt", ".csv", ".bz2", ".tsv")
        _image_types = (".pdf", )

        # Handling Excel Files
        if file_path.suffix in _excel_types:
            try:
                if "sheet_name" not in kwargs.keys():
                    kwargs["sheet_name"] = 0
                df = pd.read_excel(file_path, *args, **kwargs)
                if add_source is True:
                    df["From"] = file_path.stem
                return df
            except ValueError as ve:
                if "sheet_name" not in kwargs.keys():
                    _msg = f"Could not find {kwargs['sheet_name']} in {file_path}, returning empty DateFrame object..."
                    self.logger.warning(_msg)
                    return pd.DataFrame()
                _msg = f"{ve}: creating an empty DateFrame object..."
                self.logger.warning(_msg)
                return pd.DataFrame()

        # Handling CSV and Text Files
        elif file_path.suffix in _text_types:
            try:
                df = pd.read_csv(file_path, *args, **kwargs)
                if add_source is True:
                    df["From"] = file_path.stem
                return df
            except pandas.errors.ParserError:
                self.logger.warning(f"Could not parse in C, attempting to reparse in Python...")
                return pd.read_csv(file_path, engine='python', on_bad_lines='warn', *args, **kwargs)
            except UnicodeDecodeError as uni_error:
                self.logger.warning(f"{uni_error}: reattempting to parse with chardet...")
                with open(file_path, "rb") as f:
                    file_path_encoding = chardet.detect(f.read())
                    df = pd.read_csv(file_path, encoding=file_path_encoding['encoding'], *args, **kwargs)
                    if add_source is True:
                        df["From"] = file_path.stem
                    return df
            except pd.errors.DtypeWarning as dt_warning:
                self.logger.warning(f"{dt_warning}: reprocessing with lower_memory arg...")
                df = pd.read_csv(file_path, low_memory=False, *args, **kwargs)
                if add_source is True:
                    df["From"] = file_path.stem
                return df

        # Handling JSON files
        elif file_path.suffix == ".json":
            df = pd.read_json(file_path, *args, **kwargs)
            if add_source is True:
                df["From"] = file_path.stem
            return df

        # Handling PDFs
        elif file_path.suffix in _image_types:  # pdf
            try:
                from dirlin.pdf import PDFFile
                pdf = PDFFile(file_path)
                df = pdf.to_dataframe(*args, **kwargs)
                return df
            except ImportError as import_error:
                print(import_error)
                print(f"run pip install dirlin[pdf] to install PDF capabilities")

        # for the web / Google Sheets
        try:
            url = urlparse(str(file_path))
            if url.netloc == "docs.google.com" and "format=csv" in url.query.split("&"):
                df = pd.read_csv(file_path, *args, **kwargs)
                if add_source is True:
                    df["From"] = file_path.stem
                return df
        except HTTPError as e:
            raise e
        else:
            raise KeyError(f"File suffix {file_path.suffix} is an unsupported format. Path: {file_path}")

    def open_as_document(self, file_path: str | Path, *args, **kwargs) -> Document:
        """opens the file path as a Document object.

        Document objects have a series of functionality not available to a standard Dataframe.

        Document Functions:
            - open_as_document(): opens a path file and converts it into a document object
            - open_recent_as_document(): opens the most recent Path file and converts it into a document object
            - check_columns(): checks that the columns exist in the DataFrame
            - move_file(): moves the Document to a specified location
            - chunk(): splits a large dataframe into smaller chunk-sized DataFrames, useful if there's an upload limit
            - as_ordered_transaction(): converts a document file into an aggregated dataframe, with cumsum aggregation

        :param file_path: path to the file you want to open as a document
        :param args: see documentation for pd.DataFrame object
        :param kwargs: see documentation for pd.DataFrame object
        :return: a Document object
        """
        df = self.open(file_path, *args, **kwargs)
        return Document(df=df, path=file_path)

    def open_recent(
            self,  
            filename_pattern: str = "",
            days: int = 30,
            with_asterisks: bool = True,
            recurse: bool = False, *args, **kwargs) -> pd.DataFrame:
        """
        Finds the most recently modified file in the directory based on a filename pattern.
        Other than this, please read the documentation for the self.open() method for more details.

        :param filename_pattern: the naming convention of the file, will look in the specified directory
        :param days: the number of days the function should look back
        :param with_asterisks: defaults to True, adds the asterisks at the end of the filename_pattern arg
        :param recurse: recursively search through sub-folders for the files in the directory
        :param args: arguments under DataFrame object
        :param kwargs: named arguments for the DataFrame object
        :return: a DataFrame object
        """
        self.path_open_recent = self._find_recent_file(filename_pattern, days, with_asterisks, recurse)
        return self.open(self.path_open_recent, *args, **kwargs)

    def open_recent_as_document(
            self,
            filename_pattern: str = "",
            days: int = 30,
            with_asterisks: bool = True,
            recurse: bool = False, *args, **kwargs) -> Document:
        """
        Please refer to documentation on open_recent() and open_as_document().
        The difference is that this function returns a Document object.

        :param filename_pattern: the naming convention for the file you are searching
        :param days: number of days back you would like to search
        :param with_asterisks: whether to append an asterisks at the end of the filename_pattern
        :param recurse: whether to recurse through sub-folders
        :param args: arguments for dataframe function
        :param kwargs: keyword arguments for dataframe function
        :return: returns a Document object
        """
        self.path_open_recent = self._find_recent_file(filename_pattern, days, with_asterisks, recurse)
        return self.open_as_document(self.path_open_recent, *args, **kwargs)

    def as_map(
            self,
            file_path: str | Path,
            key_column: str,
            value_column: str, *args, **kwargs) -> dict[str, str]:
        """
        Function for creating a dictionary based on two DataFrame columns

        :param file_path: str or Path to DataFrame file. If str, just give it filename. If Path, give it the full-path.
        :param key_column: the DataFrame column to be used as the keys for the dict
        :param value_column: the DataFrame column to be used as the values in the dict
        :param args: arguments in the pd.DataFrame.from_csv() or pd.DataFrame.from_excel() functions
        :param kwargs: keyword arguments in the pd.DataFrame.from_csv() or pd.DataFrame.from_excel() functions
        :return: Dictionary of the two columns
        """
        try:
            if isinstance(file_path, str):
                file_path = self.path / file_path
        except Exception as e:
            raise e
        df = self.open(file_path=file_path, *args, **kwargs)

        if not all([column in df.columns for column in (key_column, value_column)]):
            raise f"Expected key {key_column} and value {value_column}. Missing one or all from the Dataframe."
        mapping = {
            k: v for k, v in zip(df[key_column], df[value_column])
        }
        return mapping

    def find_and_combine(
            self,
            filename_pattern: str = "",
            with_asterisks: bool = True,
            recurse: bool = False,
            limit: int | None = None,
            use_cache: bool = True,
            *args, **kwargs) -> pd.DataFrame:
        """
        Uses a filename pattern to find all files that follow the naming convention
        and converts the files into a single DataFrame object6

        :param filename_pattern: the naming convention of the files you are searching for
        :param with_asterisks: defaults to True. Determines whether an asterisks are added at the end of the
        filename pattern
        :param recurse: defaults to False. Determines whether to search for sub-folders
        :param limit: defaults to None. Determines whether to only look at the first x files to combine.
        don't use this parameter if you are unsure of the number of files in the folder
        :param args: args used in pd.DataFrame objects
        :param kwargs: keyword args used in pd.DataFrame objects
        :param use_cache: determines whether to use the previous results from find and combine to save time
        :return: a DataFrame object of all the files that share similar naming conventions in a folder
        """
        # formatting the filename pattern convention and handling asterisks
        if not with_asterisks and filename_pattern == "":
            raise ValueError(f"filename_pattern cannot be left as default if with_asterisks parameter is set to False")

        # [Part 2]: getting the paths of the files
        # [2.1] if user does not want to use the cache or if cache is None then run the function
        files = self.get_all_files(
            filename_pattern=filename_pattern,
            with_asterisks=with_asterisks,
            recurse=recurse,
            use_cached=use_cache
        )

        # [2.2] handling the only_first_x param or the limit (the max files we want to combine)
        if limit is not None:
            try:
                files = files[: limit]
            except IndexError:
                _msg = f"limit arg was too large ({limit}) compared to found files ({len(files)}). "
                _msg2 = "Keeping all files found. Limit has not been applied."
                self.logger.warning(_msg + _msg2)
            else:
                if limit < 0:
                    raise ValueError(f"The value of limit must be a positive integer. Got ({limit}).")

        # [Part 3] handling the combining of files

        # 2025.06.07 notes that PDFs aren't good running in parallel, so we'll check
        if str(files[0].suffix) == ".pdf":
            print(f"PDF, running non-parallel")
            results = self._combine_in_order(files, *args, **kwargs)
        else:
            # [3.2] Loop in Parallel (Happy Path)
            results = self._combine_in_parallel(files, *args, **kwargs)

        # [Part 4] combine and move from to the front
        df = pd.concat(results)
        if len(df) == 0:
            return pd.DataFrame()  # return empty dataframe if everything was empty

        try:
            df.insert(0, "From", df.pop("From"))
        except KeyError:
            # likely from using Source instead
            if "source" in df.columns:
                df.insert(0, "source", df.pop("source"))
        except Exception as e:
            raise e
        return df

    def index_files(
            self,
            file_ext: str,
            filename_convention: Optional[str] = "*",
            recurse: bool = False,
    ) -> list[Path]:
        """Creates a list of Path objects based on the file extension

        :param file_ext: The suffix of the file (.mp4, .xlsx, .csv)
        :param filename_convention: The naming convention of the file. Defaults to every file with extension
        :param recurse: Defaults to False. If True, will recurse through subdirectories
        :return: list of Pathlib.Path objects representing a file
        """
        if recurse:
            files = [
                f for f in self.path.rglob(
                    pattern=f"{filename_convention}{file_ext}"
                ) if not f.name.startswith("~") and not f.name.startswith(".")
            ]
        else:
            files = [
                f for f in self.path.glob(pattern=f"{filename_convention}{file_ext}")
                if not f.name.startswith("~") and not f.name.startswith(".")
            ]
        return files
