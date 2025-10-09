from abc import ABC, abstractmethod


class Query(ABC):
    """represents a single query to a table in a database

    This class is used so that all queries in any database we have in this project
    behave in the same manner.

    """
    @abstractmethod
    def execute(self, *args, **kwargs):
        """executes a specific query

        Should be some action towards the database or table,
        and usually would be some kind of CRUD action.

        Parameters:
            session: the session used to communicate with the database
        """
