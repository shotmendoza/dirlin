# Overview
## Background
Dirlin is a library with a set of tools that help manage directory based applications and workflows.
This package is build on the back of many other packages and attempts to abstract out common repetitive tasks
so that you don't have to end up rewriting basic utility functions from project to project.

The idea here is to provide the most amount of tools, without bloating the project. So far, we have added
a ton of tools, but slowly looking to refine and optimize some of the workflows so that the tools this
project provides is clean and intuitive to use.

## Tools

### Core Objects
| Object      | Description                                                  | Example Functions                 |
|-------------|--------------------------------------------------------------|-----------------------------------|
| `Folder`    | utility object that allows for handling files inside folders | `open_recent`, `find_and_combine` |
| `Directory` | a collection of `Folder` objects                             | `logger`, `DOWNLOADS`             |
| `Document`  | a helper / wrapper around `Dataframe` and `Path` objects     | `move_file`, `chunk`              |

> Some things you can do with the `Folder` object:
> 1. Getting the most recently downloaded files into a DataFrame
> 2. Finding files with the same naming conventions into a Dataframe
> 3. Creating mappings for fast column index-matching

***

### Utility Objects
| Object               | Description                                                           | Example Functions                   |
|----------------------|-----------------------------------------------------------------------|-------------------------------------|
| `DirlinFormatter`    | utility object used for formatting strings and `pd.Series`            | `convert_string_to_python_readable` |
| `TqdmLoggingHandler` | utility object used for handling `TQDM` logging and messages          | `emit`                              |
| `PDFFile`            | utility object used for handling PDF files (still a work in progress) | `to_dataframe`                      |

***

### Pipelines
| Object           | Description                                                                   | Example Functions              |
|------------------|-------------------------------------------------------------------------------|--------------------------------|
| `BaseValidation` | allows for functions that return `bool` types can be reused over many reports | `run_summary`, `run_error_log` |
| `SqlSetup`       | abstracts some of the initial `SqlAlchemy` database setup in new projects     | `create_db_if_not_exists`      |

> `BaseValidation` creates pipelines
> Some use cases include:
> 1. Validating different fields based on logic you set up
> 2. Creating error logs for any records that don't follow pre-defined rules
> 3. Doing exploratory analysis

# Installation

## Through Pip
You can install this project via pip:
```bash
pip install dirlin
```

## Through Github
Through Github for the latest developer version:
```shell
pip install git+https://github.com/shotmendoza/directory-handler
```

With a specific version:
```shell
pip install git+https://github.com/shotmendoza/directory-handler@v0.4.2
```

## With UV
```shell
uv pip install dirlin
```

# Quickstart
This will make it easier to pull files from your local directory to work on different projects.

1. Import Path and Folder Objects
2. Define the Helper Object
3. Create the folder attributes
4. Define a function to create new folders

## Setting up the Directory object in a new project

### Minimum Setup
```python
# object.py
from dirlin import Directory  # 1 - import Directory object

class LocalHelper(Directory):  # 2 - inherit the Directory object
    def __init__(self):
        super().__init__()  # 3 - call the parent
```

### Setup with more functionality
```python
# object.py
from dirlin import Directory, Folder, Path
from datetime import date
from dateutil import relativedelta

class LocalHelper(Directory):
    def __init__(self):
        super().__init__()
        self.folder_foo = Folder(Path("path/to/folder/foo"))
        self.folder_bar = Folder("path/to/folder/bar")

    def get_report_foobar(self, path: str | Path | None = None):
        if path is None:
            df = self.DOWNLOADS.open_recent("FooBarReport")
            return df
        df = self.folder.open(path)
        return df

    def get_previous_foobarfoo_path(self) -> Folder:
        previous_month = date.today() + relativedelta(months=-1, day=31)
        formatted = Folder(self.DOWNLOADS / previous_month.strftime("%Y-%m"))
        return formatted
```

## Using the Folder object

### Getting the most recently downloaded file

```python
filename = "naming convention"

# returns a dataframe of the most recent file with that pattern
df = LocalHelper.DOWNLOADS.open_recent(filename)  
```

### Combining Multiple Excel documents into a single file

```python
from dirlin import Folder, Path


def get_most_recent(filename):
    _base_path = Path("path to directory")
    folder = Folder(_base_path / "Folder1")

    combined_df = folder.find_and_combine(filename_pattern=filename)  # combines documents 
    return combined_df
```

## Using Dirlin Validation
1. Define a check function that returns a `bool` of a scalar or `pd.Series` type
2. Create a class that inherits from `BaseValidation`
   1. Create a mapping to tie out the `parameter` names, from the check function, to the `column` names, in the Dataframe you are validating
      1. The mapping should be named `alias_mapping`
   2. Add in the check functions you want to use for the `Validation` object by making it a field
3. Validate the files by using `run_summary` or `run_error_log`


