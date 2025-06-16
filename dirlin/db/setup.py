import uuid
from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid5, NAMESPACE_DNS

from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase
from sqlalchemy import Engine, create_engine

from dirlin.db.utils import database_exists


@dataclass(frozen=True)
class SqlSetup(ABC):
    """used for metadata and setting up tables and databases in the app

    Attributes:
        url: the database URL
        engine: the database engine generated from create_engine()
        session: the session context used to communicate with the database

    """
    url: str
    Base: DeclarativeBase | type[DeclarativeBase] | None = None
    engine: Engine | None = None
    session: Session | sessionmaker | None = None

    def __post_init__(self):
        # set the value for engine if the engine value was empty
        if self.engine is None:
            engine = self._generate_engine()
            object.__setattr__(self, 'engine', engine)

        # set the value for session if the session value was empty
        if self.session is None:
            session = self._create_session_factory()
            object.__setattr__(self, 'session', session)

    def _generate_engine(self) -> Engine:
        """creates an engine based off the init `url` property.
        """
        return create_engine(self.url, echo=True)

    def _create_session_factory(self):
        """generates a session factory for the database
        """
        return sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

    def create_base_factory(
            self,
    ) -> type[DeclarativeBase]:
        """generates a parent of a DeclarativeBase, used for table creation.

        Example:
            class SampleTable(Base):
                __tablename__ = "bank_statements"
                id: Mapped[int] = mapped_column(primary_key=True)
        """
        if self.Base is None:
            class Base(DeclarativeBase):
                """used for creating different SqlAlchemy tables. Tables will usually inherit from this class.

                Example:
                    class SampleTable(Base):
                        __tablename__ = "bank_statements"
                        id: Mapped[int] = mapped_column(primary_key=True)
                        transaction_id: Mapped[str] = mapped_column(unique=True)
                """
            return Base
        return self.Base

    @classmethod
    def _resolve_db_path(cls):
        """will be made cleaner in the future, but use this function
        to get the DB URL from wherever you are calling it
        """
        url = _BASE_DIR = Path(__file__).resolve().parent.parent.parent
        print(url)
        return url

    def create_if_not_exist(self) -> None:
        if not database_exists(self.url):
            self.Base.metadata.create_all(self.engine)

    @classmethod
    def generate_uuid(cls, value: str) -> str:
        """generates a usable UUID code for your table
        """
        namespace = uuid5(NAMESPACE_DNS, cls.__name__)
        uuid_val = str(uuid5(namespace, value))
        return uuid_val
