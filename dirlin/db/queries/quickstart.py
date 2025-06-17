from pydantic import BaseModel
from sqlalchemy import select, update
from sqlalchemy.orm import DeclarativeBase

from dirlin.db.queries.query import Query
from dirlin.db.setup import SqlSetup


class CreateOrUpdateRecord(Query):
    def __init__(self, setup: SqlSetup):
        """creates a new record in the table or updates existing record

        Assumes that tables has an `iid` column used as the primary key.
        """
        self.setup = setup

    def execute(
            self,
            table: type[DeclarativeBase],
            model: BaseModel,
            table_id_field: str,
            model_id_field: str,
    ) -> None:
        with self.setup.session.begin() as sesh:
            query = sesh.execute(
                select(table)
                .where(getattr(table, table_id_field) == getattr(model, model_id_field))  # type:ignore
            )
            if query.one_or_none() is None:
                new_record = table(**model.model_dump(exclude_none=True))
                sesh.add(new_record)
            else:
                sesh.execute(
                    update(table)
                    .where(getattr(table, table_id_field) == getattr(model, model_id_field)),  # type:ignore
                    model.model_dump(exclude={model_id_field: True}, exclude_none=True)
                )


class ReadRecordWithTransactionID(Query):
    def __init__(self, setup: SqlSetup):
        """gets an existing record from the database

        """
        self.setup = setup

    def execute(
            self,
            table: type[DeclarativeBase],
            model: type[BaseModel],
            table_id_field: str,
            id_lookup: str | None = None,
    ) -> type[BaseModel]:
        with self.setup.session.begin() as sesh:
            query = sesh.execute(select(table))
            if id_lookup is not None:
                query = sesh.execute(
                    select(table).where(getattr(table, table_id_field) == id_lookup)  # type:ignore
                )
            return model.model_validate(query.scalar_one())  # type:ignore


__all__ = ['CreateOrUpdateRecord', 'ReadRecordWithTransactionID']
