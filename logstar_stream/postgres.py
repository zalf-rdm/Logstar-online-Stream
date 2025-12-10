import logging
import pandas as pd
import sqlalchemy as sq
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect

class Postgres:

    # PostgreSQL interaction
    # ref: https://stackoverflow.com/questions/30337394/pandas-to-sql-fails-on-duplicate-primary-key
    @staticmethod
    def __insert_or_do_nothing_on_conflict__(table, conn, keys, data_iter):
        """
        Insert all records from data_iter into table. If a record already exists (as determined by the primary keys), do nothing.
        """
        insert_stmt = insert(table.table).values(list(data_iter))
        on_duplicate_key_stmt = insert_stmt.on_conflict_do_nothing()
        conn.execute(on_duplicate_key_stmt)


    # ref: https://stackoverflow.com/questions/30867390/python-pandas-to-sql-how-to-create-a-table-with-a-primary-key
    @staticmethod
    def create_table(
        pandas_sql,
        frame,
        name,
        if_exists="fail",
        index=True,
        index_label=None,
        schema=None,
        chunksize=None,
        dtype=None,
        keys=None,
        **kwargs,
    ):
        if dtype is not None:
            from sqlalchemy.types import to_instance, TypeEngine

            # check provided types
            for col, my_type in dtype.items():
                if not isinstance(to_instance(my_type), TypeEngine):
                    raise ValueError("The type of %s is not a SQLAlchemy " "type " % col)
        
        table = pd.io.sql.SQLTable(
            name,
            pandas_sql,
            frame=frame,
            index=index,
            if_exists=if_exists,
            index_label=index_label,
            schema=schema,
            dtype=dtype,
            keys=keys,
            **kwargs,
        )
        table.create()

    @staticmethod
    def write_to_database(name, df, database_engine, db_schema, db_table_prefix, datetime_column, **kwargs):
        table_name = db_table_prefix + name

        if not inspect(database_engine).has_table(table_name=table_name, schema=db_schema):
            logging.info(
                f"creating database table {table_name} with primary key on {datetime_column} ..."
            )
            pandas_sql = pd.io.sql.pandasSQL_builder(database_engine, schema=db_schema)

            # create table with constrains
            Postgres.create_table(
                pandas_sql,
                df,
                table_name,
                index=None,
                index_label=None,
                keys=datetime_column,
                if_exists="replace",
                schema=db_schema
            )

        else:
            # Check constraints
            try:
                constraints = inspect(database_engine).get_pk_constraint(
                    table_name=table_name, schema=db_schema
                )
                # Handle both single column and composite key
                expected_columns = datetime_column if isinstance(datetime_column, list) else [datetime_column]
                if (
                    not "constrained_columns" in constraints
                    or not all(col in constraints["constrained_columns"] for col in expected_columns)
                ):
                    logging.warning(
                        f"Table {table_name} has no primary key set on {expected_columns} column(s), this can result in duplicated data in table  ..."
                    )
            except Exception as e:
                logging.warning(f"Could not inspect constraints for {table_name}: {e}")

        to_sql_arguments = {
            "name": table_name,
            "con": database_engine,
            "schema": db_schema,
            "if_exists": "append",
            "index": False,
            "chunksize": 4096,
            "method": Postgres.__insert_or_do_nothing_on_conflict__,
        }

        try:
            logging.info(f"writing {table_name} to database ...")
            df.to_sql(**to_sql_arguments)
            logging.info(f"succesfully writing data ...")
        except Exception as E:
            logging.error(f"failed writing data to {table_name} ...")
            logging.error(E)
