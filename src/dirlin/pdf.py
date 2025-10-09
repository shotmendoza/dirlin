import io
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Any

import pandas as pd
import pdfplumber
from pdfplumber.display import PageImage
from pdfplumber.page import Page
from pypdf import PdfReader, PdfWriter, PageObject

from dirlin.core.api import DirlinFormatter

# Parse PDF
# Read with PDF Plumber fn(path: Path) => Page
# If it fails, read with (1) PyPDF fn(path: Page) then (2) read with PDF Plumber fn(path: Page)
# returns pd.Dataframe

# Note
# (1) Path => Page / list[list[],  list[]] = (CLASS #1)
# (2) Page / list[list[]] => pd.Dataframe = (CLASS #2)
# (want) Path => pd.Dataframe
# [(1) Path] => [(2) intermediate] => (3) pd.Dataframe]


class PDFFile:
    def __init__(self, path: Path):
        """
        :param path: Path or Folder or str to parse a PDF with. Depending on the path,
        we would be able to accomplish different tasks and functions
        """
        self.path = path
        self._process = DebugHelper()

    def to_dataframe(
            self,
            field_names: list[str] | None = None,
            skip_first_per_page: bool = False,
            skip_first_per_pdf: bool = False,
            table_settings: dict | None = None,
            remove_repeated_keywords: str | None = None,
            remove_duplicated_records: bool | None = None,
            remove_records_with_all_null: bool | None = None,
            add_source: bool = True,
            debug: bool = False,
            *args,
            **kwargs
    ):
        """converts a PDF file into a Dataframe

        ...

        # Below are some notes on some of the use-cases of the parameters in this function.

        `skip_first_row` is useful when there's a mini-table in the table you parsed:
            - in > [["Bad Field", None, None], ["Column A", "Column B", "Column C"], [1, 2, 3]]
            - out > [["Column A", "Column B", "Column C"], [1, 2, 3]]

        ...

        `remove_repeated_keywords` can be used when there's a persistent keyword you want to remove:
            - in > [["Total", ...], ..., ["Total", ...], ..., ["Total", ...]]
            - out > [...]

        ...

        `add_source` adding a column to the dataframe
            - in > df.columns > ["foo", "bar", "foobar", ...]
            - out > df.columns > ["foo", "bar", "foobar", ... , "source"]

        ...

        :param field_names: column names you want to process the dataframe with
        :param table_settings: for `pdfplumber` settings for parsing the PDF table
        :param remove_repeated_keywords: determines whether to remove a row based on a keyword
        :param skip_first_per_pdf:
        :param skip_first_per_page:
        :param debug:
        :param remove_duplicated_records: determines whether to remove duplicated records
        :param remove_records_with_all_null: determines whether to remove records with null values
        :param add_source: determines whether to add the `source` field when opening the file
        """
        helper = _PyPdfFormatter()

        # Read all the pages into Datastore, so we only need to loop through once
        pdf_info = PDFStore(pdf_path=self.path)
        path = pdf_info.pdf_path

        # 2025.06.08 going to push a problem into the future
        # we're going to assume that if we use the pyPDF we need to rotate the PDF
        if pdf_info.errors() is True:
            error_pages = pdf_info.from_pypdf()
            path = helper.rotate_pdf(error_pages)

        # [Part 3] Now that we have the list[Page] we'll create the tables

        # SETTING UP PARAMS TO PREP FOR PAGE PARSING
        if table_settings is None:
            table_settings = {}

        # =NOTE= hopefully the rotation worked by this point
        records = pdf_info.from_pdf_plumber(
            path=path,
            table_settings=table_settings,
            skip_first_per_page=skip_first_per_page,
            inplace=True,
            debug=debug
        )

        # Bytes IO NEEDS TO BE CLOSED
        if isinstance(path, io.BytesIO):
            path.close()

        # [PART 4] Cleaning up the records now.
        if field_names is None:
            field_names = helper.get_field_names(records, field_names)

            # if the field names is still None, we'll return an empty Dataframe
            if field_names is None:
                flag_empty_df_triggered = True
                print(f"Records were unable to be parsed. {pdf_info.pdf_path} Empty Dataframe returned.")
                print(f"flag_empty_df_triggered: {flag_empty_df_triggered}")
                return pd.DataFrame()

        #  We'll attempt to generate a dataframe here
        try:
            df = pd.DataFrame(records, columns=field_names, *args, **kwargs)
        except Exception as e:
            raise e
        df = helper.formatting.clean_table(
            df,
            remove_duplicated_records=remove_duplicated_records,
            remove_records_with_all_null=remove_records_with_all_null,
            remove_keyword_records=remove_repeated_keywords
        )

        if add_source is True:
            df["source"] = pdf_info.pdf_path.stem
        return df


@dataclass
class PDFStore:
    """stores debug issues / PDF information as a dataclass

    Properties:
        - pdf_path: Path
        - page: Page

        After using generate_page_image_and_path:
            - table_image_path: Path | None = None
            - table_image: PageImage | None = None

    """
    pdf_path: Path

    # intermediary steps for when parsing with PdfReader
    pages_objects: list[PageObject] | PageObject | None = None
    """from pyPDF"""

    tables: list[list[Any]] | None = None
    """from PdfPlumber"""

    # debug steps for when there's some kind of error
    table_image_path: Path | None = None
    table_image: PageImage | None = None

    # flags to check for proper debug action
    height: int | float | None = None
    width: int | float | None = None
    extraction_failed: bool | None = None

    # High Level error properties
    iter: int = 1  # used for debug table, the number of times the fn was run
    errors_log: list[str] | None = None

    # === END OF PROPERTIES ===

    def from_pdf_plumber(
            self,
            path: Path | io.BytesIO | None = None,
            table_settings: dict | None = None,
            skip_first_per_page: bool = False,
            inplace: bool = False,
            debug: bool = False
    ) -> list[list[Any]]:
        """uses pdf plumber to get the table records after table extraction

        If no path is given, will default to self.tables, which uses the default
        pdf path

        :param path: Path
        :param table_settings: dict | None
        :param debug:
        :param skip_first_per_page: determines whether to skip the first row on page level
        :param inplace: will save the list of pages under self.pages
        """
        if path is None:
            if self.tables is not None:
                # it means we ran this before
                return self.tables.copy()
            path = self.pdf_path

        if not isinstance(path, Path) and not isinstance(path, io.BytesIO):
            raise TypeError(f'path must be Path or io.BytesIO. Got {type(path)}.')

        tbl = self.extract_table_from_pdf_plumber(path, table_settings, skip_first_per_page, debug)

        if inplace is True:
            self.tables = tbl
        return tbl

    def from_pypdf(
            self,
            inplace: bool = False
    ) -> list[PageObject]:
        """uses pypdf to parse a PDF file and get a workable object.
        This is usually used when a PDF was not able to be parsed with pdf plumber.
        """
        # Reading with PdfReader instead of PdfPlumber
        pdf = PdfReader(self.pdf_path)
        # reads the pdf pages and returns a list
        pages = [page for page in pdf.pages]
        if inplace is True:
            self.pages_objects = pages
        return pages

    def errors(self) -> bool:
        # Handling self.pages not being initialized
        if self.height is None or self.width is None or self.extraction_failed is None:
            self._extract_data_from_pdf_plumber()

        check_error = [
            self.check_rotation_error(),
            self.check_nothing_extracted()
        ]
        return any(check_error)

    def check_nothing_extracted(self):
        """checks to see if we were able to extract any records
        """
        return self.extraction_failed

    def check_rotation_error(self) -> bool:
        """checks for rotation issues
        """
        height = self.height * .75
        width = self.width

        # For handling bad rotation -> marks a flag if rotation error detected
        return height > width

    @classmethod
    def _extract_table_with_pdf_plumber(
            cls,
            page: Page,
            table_settings: dict | None = None,
            skip_first_per_page: bool = False,
    ) -> list[list]:
        """function to make it easier to list comprehension
        """
        # RUNNING THE TABLE EXTRACTION
        tbl = page.extract_table(table_settings)
        if skip_first_per_page is True:
            tbl = tbl[1:]
        return tbl

    def _extract_data_from_pdf_plumber(self) -> None:
        """we're going to use this function to update this class
        and gather all the data we need in order to complete our tasks.
        
        We need to do this because we can't pass a list[Page] objects around,
        so we just need to pull all the data while we have the PDF open.
        
        *For Future* we'll need to extract data for checks if we have them,
        like Height, Width, etc.
        
        This won't return anything, just update the class inplace.
        
        """
        with pdfplumber.open(self.pdf_path) as pdf:
            pages = [page for page in pdf.pages]

            # Rotation Check Variables
            width = sum(page.width for page in pages) / len(pages)
            height = sum(page.height for page in pages) / len(pages)
            self.width, self.height = width, height

            # Text Extraction Variables
            extracted_text = [page.extract_text() for page in pages]
            self.extraction_failed = any(text == "" for text in extracted_text)

    def extract_table_from_pdf_plumber(
            self,
            path: Path | io.BytesIO | None = None,
            table_settings: dict | None = None,
            skip_first_per_page: bool = False,
            debug: bool = False
    ) -> list[list]:
        """This function is the public facing API for pulling a table with PDF plumber.
        We use this function to extract the table from a PDF file. The private version
        of this function takes a page param, which makes the looking easier here.

        """
        if path is None:
            path = self.pdf_path

        with pdfplumber.open(path) as pdf:
            tables = [
                self._extract_table_with_pdf_plumber(page, table_settings, skip_first_per_page)
                for page in pdf.pages
            ]

            if debug is True:
                self.extract_debug_image_and_path()
        return tables[0]

    def extract_debug_image_and_path(self) -> None:
        """updates the object with new values for the table_image and table_image_path
        so that we can refer to this later.

        """
        # creating the filepath from given filepath
        parent_path = self.pdf_path.parent
        filepath = self.pdf_path.stem
        curr_iter = self.iter

        # creating the table image from given Page instance
        with pdfplumber.open(self.pdf_path) as pdf:
            for page in pdf.pages:
                image = page.to_image().debug_tablefinder()
                filename = parent_path / f"Curr_{curr_iter}_{filepath}_bound_boxes_{page.page_number}.png"
                image.save(filename)
        self.iter += 1


class _PyPdfFormatter:
    formatting = DirlinFormatter()

    @classmethod
    def rotate_pdf(
            cls, pages: PageObject | list[PageObject],
            degree: Literal[0, 90, 180, 270] = 90
    ) -> io.BytesIO:
        """we need to use this because PDFPlumber can't handle rotating PDFs.

        **NOTE** we assume that if this function is running, we are rotating the
        PDF.
        """
        # Happy Path with one PageObject
        if isinstance(pages, PageObject):
            pages = [pages]

        # Create transformation matrix
        if degree == 90:
            matrix = (0, 1, -1, 0, pages[0].mediabox.height, 0)
        elif degree == 180:
            matrix = (-1, 0, 0, -1, pages[0].mediabox.width, pages[0].mediabox.height)
        elif degree == 270:
            matrix = (0, -1, 1, 0, 0, pages[0].mediabox.width)
        else:
            raise ValueError("Rotation must be 0, 90, 180, or 270 degrees.")

        for page in pages:
            page.add_transformation(matrix)

        return cls.write_pdf_as_buffer(pages)

    @classmethod
    def write_pdf_as_buffer(cls, pages: PageObject | list[PageObject]) -> io.BytesIO:
        if isinstance(pages, PageObject):
            pages = [pages,]

        # write in buffer, so we don't have to create new file
        pdf = PdfWriter()
        for page in pages:
            pdf.add_page(page)

        buffer = io.BytesIO()
        pdf.write(buffer)
        buffer.seek(0)
        return buffer

    @classmethod
    def get_field_names(cls, records: list[list], field_names: list[str] | None = None) -> list[str]:
        # If Field Name is None, we automatically try to parse it
        if field_names is None:
            try:
                # Attempts to take the first row as the column names
                field_names = [
                    cls.formatting.convert_string_to_python_readable(f, True) for f in records[0]
                ]
            except TypeError as te:
                # the function in the try block will raise a type error if None comes in as the name
                # similar to attribute error, this happens when the PDF gets parsed as an empty table
                print(f"Table unable to be pulled (TE)")
                print(f"Hint: try using `skip_first_per_page` or  skip_first_per_pdf argument (`page`, `pdf`).")
                raise te
            except AttributeError as ae:
                # This happens when the PDF table that gets parsed is empty.
                # I think we're going to return an empty DataFrame so columns don't get affected.
                print(f"Table unable to be pulled (AE)")
                raise ae
            return field_names


class DebugHelper:
    def __init__(self):
        """helps handle debugging and identify the issue
        """
        self.curr_issue: PDFStore | None = None
        """the current issue that's being looked at"""

        self.issue_mapping: dict = {}
        """stores the filepath and the Page that is causing issues.
        """

        self.all_issues: list[PDFStore] = []
        """stores all issue pages in a flat list"""

        self.unique_paths: list[Path] = []
        """used to store the unique Paths. This is used because pypdf uses the path to reprocess pages, 
        so we want to keep a unique list to only have to go through this exercise once
        """

    def analyze(self, file_path: Path, page: Page) -> None:
        """saves down `file_path` as a property, to check against. Saves a page under the filepath
        so that we know which page led to the issue. We can't directly use the Page, so we need to
        figure out how to work around it

        """
        # so we can later iterate once we think we have all the issue PDFs
        page_info = PDFStore(file_path, page)

        try:
            self.issue_mapping[file_path].append(page_info)
        except KeyError:
            # This PDF was not previously processed
            self.issue_mapping[file_path] = []
            self.issue_mapping[file_path].append(page_info)
        finally:
            self.all_issues.append(page_info)

        # looks like debug is being used in a loop and is looking at single pages at a time.
        # adding this, so we can work on problems one at a time as well.
        self.curr_issue = page_info

    @classmethod
    def to_page(
            cls,
            pdf: PdfWriter,
            table_settings: dict | None = None,
            skip_first_row: Literal['page', 'pdf'] | None = None,
    ) -> list[list]:
        """converts a pypdf to list of table lists similar to the other function (_parse_table)

        """
        # easier to make arg None so setting default
        if table_settings is None:
            table_settings = {}

        # write in buffer, so we don't have to create new file
        buffer = io.BytesIO()
        pdf.write(buffer)
        buffer.seek(0)

        processed_tables = []
        with pdfplumber.open(buffer) as pdf:
            for page in pdf.pages:
                temp_tbl = page.extract_table(table_settings)

                if skip_first_row == "page":
                    temp_tbl = temp_tbl[1:]

                processed_tables.extend(temp_tbl)

            # second check in case they want to only skip it on the PDF level (after page loop)
            if skip_first_row == "pdf":
                processed_tables = processed_tables[1:]
        return processed_tables
