import os
import pandas as pd
import sqlalchemy
from typing import Optional, Literal
import contextlib

import utils

from sqlalchemy.sql.ddl import DropSchema
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import MetaData, inspect, create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.schema import CreateSchema


class DBConfig(utils.Model):
    def __init__(self, delicate: str = 'postgresql', host: str = 'localhost', port: int = 5432,
                 database: str = None, username: str = None, password: str = None, auth_file: str = None,
                 query: dict = None, stream: bool = False, echo: bool = False):

        self.__delicate = delicate
        self.__host = host
        self.__port = port
        self.__database = database
        self.__username = username
        self.__password = password
        self.__auth_file = auth_file
        self.__stream = stream
        self.__echo = echo
        self.__query = query.copy() if query else {}

    @property
    def query(self):

        return self.__query.copy() if self.__query else self.__query

    @query.setter
    def query(self, query):
        if query is not None and not isinstance(query, dict):
            raise ValueError("Query must be a dict.")
        self.__query = query.copy() if query else {}

    @property
    def delicate(self):
        return self.__delicate

    @delicate.setter
    def delicate(self, delicate):
        if delicate is None:
            raise ValueError("Delicate can not be emtpy or None.")

        if not isinstance(delicate, str):
            raise ValueError("Delicate must be a string.")
        self.__delicate = delicate

    @property
    def host(self):
        return self.__host

    @host.setter
    def host(self, host):
        if host is None:
            raise ValueError("Host can not be emtpy or None.")

        if not isinstance(host, str):
            raise ValueError("Host must be a string.")
        self.__host = host

    @property
    def port(self):
        return self.__port

    @port.setter
    def port(self, port):
        if port is None:
            raise ValueError("Port can not be emtpy or None.")
        if not isinstance(port, int) or not (0 < port < 65536):
            raise ValueError("Port must be an integer between 1 and 65535.")
        self.__port = port

    @property
    def database(self):
        return self.__database

    @database.setter
    def database(self, database):
        if database is None:
            raise ValueError("Database name can not be emtpy or None.")
        if not isinstance(database, str):
            raise ValueError("Database name must be a string.")
        self.__database = database

    @property
    def username(self):
        return self.__username

    @username.setter
    def username(self, username):
        if username is None:
            raise ValueError("Username can not be emtpy or None.")
        if not isinstance(username, str):
            raise ValueError("Username must be a string.")
        self.__username = username

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, password):
        if password is not None and not isinstance(password, str):
            raise ValueError("Password must be a string or None.")
        self.__password = password

    @property
    def stream(self):
        return self.__stream

    @stream.setter
    def stream(self, stream):
        if stream is not None and not isinstance(stream, bool):
            raise ValueError("stream must be a string or None.")
        self.__stream = stream

    @property
    def echo(self):
        return self.__echo

    @echo.setter
    def echo(self, echo):
        if echo is not None and not isinstance(echo, bool):
            raise ValueError("Password must be a string or None.")
        self.__echo = echo

    @property
    def auth_file(self):
        return self.__auth_file

    @auth_file.setter
    def auth_file(self, auth_file):
        # Validation logic for jks_file
        if auth_file is not None and not isinstance(auth_file, str):
            raise ValueError("JKS file path must be a string or None.")
        if not os.path.exists(auth_file):
            raise FileNotFoundError(f"Authentication file '{auth_file}' not found.")
        self.__auth_file = auth_file

    # Update method for dynamic configuration
    def update_config(self, **kwargs):
        for key, value in kwargs.items():
            setter_method = f"{key}"
            if hasattr(self, setter_method):
                try:
                    getattr(self, setter_method)(value)
                except Exception as e:
                    raise ValueError(f"Error setting {key}: {str(e)}")
            else:
                raise KeyError(f"Invalid configuration key: {key}")


class DBConnection:
    def __init__(self, config: DBConfig, logger):
        # SSH Tunnel Variables
        self.__engine = None
        self.__inspector = None
        self.__metadata = MetaData()
        self.__config = config
        self._logger = logger

        self.__create_engine()

    @property
    def inspector(self):
        if self.__inspector is None:
            self._logger.info('Database engine inspector created successfully.')
            self.__inspector = sqlalchemy.inspect(self.engine)
        return self.__inspector

    @property
    def config(self):
        return self.__config

    @config.setter
    def config(self, config: DBConfig):
        if config is not None and not isinstance(config, DBConfig):
            self._logger.error("Config must be an DBConfig instance.")
            raise TypeError("Config must be an SSHConfig instance.")
        self.__config = config

    @property
    def engine(self):
        if self.__engine is None:
            self.__create_engine()
        return self.__engine

    def __create_engine(self):
        self._logger.info(f"Creating connection to {self.config.host} on {self.config.database}...")
        try:
            conn_url = sqlalchemy.engine.url.URL(
                drivername=self.config.delicate,
                username=self.config.username,
                password=self.config.password,
                host=self.config.host,
                database=self.config.database,
                port=self.config.port,
                query=self.config.query,
            )
        except Exception as e:
            try:
                conn_url = sqlalchemy.engine.url.URL.create(
                    drivername=self.config.delicate,
                    username=self.config.username,
                    password=self.config.password,
                    host=self.config.host,
                    database=self.config.database,
                    port=self.config.port,
                    query=self.config.query,
                )
            except Exception as e:
                self._logger.error(f"Failed to build a URI for the Database.")
                raise e

        self._logger.info(f'Connection URI is: {conn_url}')

        try:
            self.__engine = create_engine(conn_url, echo=self.config.echo)
            if self.config.stream:
                self.engine.connect().execution_options(stream_results=self.config.stream)
            self._logger.info(f'Database [{self.engine.url.database}] session created...')
        except sqlalchemy.exc.SQLAlchemyError as e:
            self._logger.error(f"Failed to create engine due database error: {e}")
            raise e
        except Exception as e:
            self._logger.error(f"Failed to create engine due unknown error: {e}")
            raise e

    def schemas(self):
        try:
            schemas = self.inspector.get_schema_names()
            df = pd.DataFrame(schemas, columns=['schema name'])
            self._logger.info(f"Number of schemas: {df.shape[0]}")
            return df
        except sqlalchemy.exc.SQLAlchemyError as e:
            self._logger.error(f"Error retrieving schemas: {e}")
            raise

    def tables(self, schema: str):
        try:
            tables = self.inspector.get_table_names(schema=schema)
            df = pd.DataFrame(tables, columns=['table name'])
            self._logger.info(f"Number of tables: {df.shape[0]}")
            return df
        except sqlalchemy.exc.SQLAlchemyError as e:
            self._logger.error(f"Error retrieving tables from schema {schema}: {e}")
            raise e

    def select(self, query: str, params: Optional[dict] = None, chunk_size: Optional[int] = None):
        """
        Executes a SQL select query with optional parameterization.

        :param query: SQL query string.
        :param params: Optional dictionary of parameters to be used in the query.
        :param chunk_size: Number of rows per chunk to return for large queries.
        :return: DataFrame containing the result set.
        """
        self._logger.info(f'Executing \n{query}\n in progress...')
        try:
            query_df = pd.read_sql(
                query, self.engine, params=params, chunksize=chunk_size
            ).convert_dtypes(convert_string=False)
            self._logger.info('<> Query Successful <>')
            return query_df
        except Exception as e:
            self._logger.error(f'Unable to read SQL query: {e}')
            raise e

    def insert(self, df: pd.DataFrame, table: str, schema: str,
               if_exists: Literal['fail', 'replace', 'append'] = 'fail', chunk_size: Optional[int] = None,
               index: bool = False):
        """
        Inserts data into a database table. This method can append to, replace,
        or fail upon encountering existing data based on the 'if_exists' parameter.

        :param df: DataFrame containing the data to be inserted.
        :param table: Name of the database table.
        :param schema: Database schema.
        :param if_exists: Behavior when the table already exists ('replace', 'append', 'fail').
        :param chunk_size: Number of rows in each batch to be written at a time.
        :param index: Whether to write the DataFrame's index as a column.
        :return: True if the operation is successful.
        """
        try:
            df.to_sql(table, self.engine, schema=schema, if_exists=if_exists, chunksize=chunk_size, index=index)
            self._logger.info(f'Data inserted into [{table}] in schema {schema} successfully.')
            return True
        except sqlalchemy.exc.SQLAlchemyError as e:
            self._logger.error(f"Error inserting data into table {table}: {e}")
            raise e

    def execute(self, sql: str):
        self._logger.info(f'Executing {sql} in progress...')
        try:
            with self.engine.connect() as conn:
                res = conn.execute(text(sql))
            self._logger.info(f'<> Run SQL done Successful <>')
            return res

        except sqlalchemy.exc.SQLAlchemyError as e:
            self._logger.error(f"Error executing SQL: {e}")
            return False

    def close(self):
        if self.__engine:
            self.engine.dispose()
            self._logger.info('<> Connection Closed Successfully <>')


class DynamicTableGenerator:
    def __init__(self, connection: DBConnection, logger):
        """
        Initialize the dynamic table generator with a database connection string.

        :param connection: DBConnection connection instance.
        :param logger: logger instance.
        """
        self.__connection = connection
        self.__base = declarative_base(cls=utils.Model)
        self.__session = sessionmaker(bind=self.__connection.engine)()
        self._logger = logger

    @property
    def session(self):
        # """Provide a session context manager."""
        #
        # @contextlib.contextmanager
        # def session_scope():
        #     session = self.__session_factory()
        #     try:
        #         yield session
        #         session.commit()
        #     except Exception:
        #         session.rollback()
        #         raise
        #     finally:
        #         session.close()

        return self.__session

    def schema_exists(self, schema: str) -> bool:
        """Check if a schema exists in the database."""
        with self.__connection.engine.connect() as conn:
            return conn.dialect.has_schema(conn, schema)

    def create_schema(self, schema: str):
        """
        Create a database schema if it does not exist.

        :param schema: Name of the schema
        """
        if not self.schema_exists(schema):
            try:
                self._logger.info(f"Attempt to create schema '{schema}'.")
                with self.__connection.engine.connect() as conn:
                    conn.execute(CreateSchema(schema))
                    conn.commit()
                self._logger.info(f"Schema '{schema}' created successfully.")
                return True
            except Exception as e:
                self._logger.error(f"Error creating schema {schema}: {e}")
                raise e
        else:
            self._logger.info(f"Schema '{schema}' already exists.")
            return True

    def drop_schema(self, schema: str):
        """Drop a database schema."""
        if self.schema_exists(schema):
            try:
                self._logger.info(f"Attempt to drop schema '{schema}'.")
                with self.__connection.engine.connect() as conn:
                    conn.execute(DropSchema(schema))
                    conn.commit()
                self._logger.info(f"Schema '{schema}' dropped successfully.")
                return True
            except Exception as e:
                self._logger.error(f"Error dropping schema {schema}: {e}")
                raise
        else:
            self._logger.info(f"Schema '{schema}' does not exists.")
            return True

    def create_table_class(self, name: str, columns: dict, schema: str):
        """
        Create a SQLAlchemy table class dynamically.

        :param name: Name of the table
        :param columns: Dictionary of column names and their types
        :param schema: Schema name where the table will be created
        :return: Table class
        """
        self._logger.info(f"Initiating the table '{name}' class.")
        attrs = {
            '__tablename__': name.lower(),
            '__table_args__': {
                'extend_existing': True,
                'schema': schema
            },
        }
        attrs.update(columns)
        self._logger.info(f"'{name.capitalize()}' class created.")

        try:
            self.create_schema(schema)
            return type(name, (self.__base,), attrs)
        except Exception as e:
            self._logger.error(f"Error creating table class {name}: {e}")
            raise

    def create_tables(self):
        """
        Create all tables in the database.
        """
        try:
            self._logger.info(f"Attempt to create all provided tables.")
            self.__base.metadata.create_all(self.__connection.engine)
        except Exception as e:
            self._logger.error(f"Error creating tables: {e}")
            raise e

    def get_table_metadata(self, table: str, schema: str = None) -> dict:
        """Retrieve metadata for a specified table."""
        try:
            with self.__connection.engine.connect() as conn:
                inspector = inspect(conn)
                return inspector.get_columns(table, schema=schema)
        except Exception as e:
            self._logger.error(f"Error retrieving metadata for table {table}: {e}")
            raise e


if __name__ == "__main__":
    config = utils.load_json_file('./config.json')
    database = config['database']
    dbconfig = DBConfig(
        delicate=config['database']['delicate'],
        host=config['database']['host'],
        port=config['database']['port'],
        database=config['database']['database'],
        username=config['database']['username'],
        password=config['database']['password'],
        auth_file=config['database']['auth_file'],
        query=config['database']['query'],
        stream=config['database']['stream'],
        echo=config['database']['echo'],
    )

    print(dbconfig)
    pass
