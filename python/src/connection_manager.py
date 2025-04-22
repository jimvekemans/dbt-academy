"""
Connection manager for database connections.

This module provides an abstract base class for managing database connections
and specialized subclasses for different database types such as Snowflake and Databricks.
"""

import os
import pandas as pd

from abc import ABC, abstractmethod
from typing import List, override

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.log_manager import log, pretty_print


class ConnectionManager(ABC):
    """
    Abstract base class for managing database connections.

    Provides common functionality for automatic management of:
    - database sessions with context awareness,
    - query execution with error handling,
    - session hooks (pre-run and post-run)
    - batch query execution
    - environment variable-based configuration
    - SSL certificate handling
    - query history tracking

    ### Example usage:
    ```python
    connection_details = {
        "type": "postgres",
        "user": "myuser",
        "pass": "mypass",
        "host": "localhost",
    }
    with PostgresConnectionManager(connection_details) as connection_manager:
        connection_manager.execute_query("SELECT * FROM table_name")
    ```

    ### Or create from pre-defined environment variables:
    ```python
    os.environ["POSTGRES_USER"] = "myuser"
    os.environ["POSTGRES_PASS"] = "mypass"
    os.environ["POSTGRES_HOST"] = "localhost"
    with PostgresConnectionManager.from_env() as connection_manager:
        connection_manager.execute_query("SELECT * FROM table_name")
    ```
    """

    engine: Engine
    current_session: Session

    session_query_history: List[str]
    all_query_history: List[str]

    pre_run_hooks: str
    post_run_hooks: str
    should_run_hooks: bool = False

    # This dictionary maps the expected keys to the alternative keys that can be used in the connection details.
    # "desired_key": ("default", "alt1", "alt2", ...)
    keyword_map = {
        "type": ("type", "dbtype", "db_type"),
        "host": ("host", "server", "hostname", "host_name", "url"),
        "port": ("port", "portnum", "portnr", "portnumber"),
        "user": ("user", "username", "user_name", "userid", "user_id"),
        "pass": ("pass", "password", "pwd", "passwd"),
        "database": ("database", "dbname", "db_name", "db", "database_name"),
        "schema": ("schema", "schema_name"),
        "account": ("account", "user_account", "acc"),
        "warehouse": ("warehouse", "wh"),
        "role": ("role"),
        "token": ("token"),
    }

    def __init__(
        self,
        connection_details: dict,
        pre_run_hooks: List[str] = [],
        post_run_hooks: List[str] = [],
    ) -> None:
        """
        Initialize a ConnectionManager instance.

        :param connection_details: Dictionary with connection details for the database
        :type connection_details: dict
        :param pre_run_hooks: SQL statements to execute when starting a session, defaults to []
        :type pre_run_hooks: List[str], optional
        :param post_run_hooks: SQL statements to execute when closing a session, defaults to []
        :type post_run_hooks: List[str], optional
        """
        self.set_run_hooks(pre_run_hooks=pre_run_hooks, post_run_hooks=post_run_hooks)
        self.get_new_engine(connection_details=connection_details)
        self.all_query_history = []

    def set_run_hooks(
        self,
        pre_run_hooks: List[str] = [],
        post_run_hooks: List[str] = [],
    ) -> None:
        """
        Set the SQL statements to run when starting and closing a session.

        :param pre_run_hooks: SQL statements to execute when starting a session, defaults to []
        :type pre_run_hooks: List[str], optional
        :param post_run_hooks: SQL statements to execute when closing a session, defaults to []
        :type post_run_hooks: List[str], optional
        """
        self.pre_run_hooks = " ".join(
            f"{hook};" for hook in pre_run_hooks if not hook.strip().endswith(";")
        )
        self.post_run_hooks = " ".join(
            f"{hook};" for hook in post_run_hooks if not hook.strip().endswith(";")
        )
        self.should_run_hooks = bool(self.pre_run_hooks or self.post_run_hooks)

    def __enter__(self):
        """
        Enter a context manager for the connection manager.

        :return: The connection manager instance
        :rtype: ConnectionManager
        """
        log("Starting new session with connection manager...")
        self.start_new_session()
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        """
        Exit the context manager for the session manager.
        """
        log("Closing session and exiting connection manager.")
        self.close_current_session()
        return False

    @abstractmethod
    def build_connection_string(self, connection_details: dict) -> str:
        """
        Build a connection string for the database from connection details.

        :param connection_details: Dictionary with connection details
        :type connection_details: dict
        :return: Connection string
        :rtype: str
        """
        ...

    def require_ssl_args(self, connection_details: dict) -> bool:
        """
        Check if SSL arguments are required and set them up if needed.

        :param connection_details: Dictionary with connection details
        :type connection_details: dict
        :return: True if SSL arguments are required, False otherwise
        :rtype: bool
        """
        self.ssl_args = None
        if "ssl_certificate_path" in connection_details:
            self.ssl_args = {
                "ca": connection_details.get("ssl_certificate_path"),
            }
        return self.ssl_args is not None

    def get_new_engine(self, connection_details: dict, **kwargs) -> Engine:
        """
        Create a new SQLAlchemy engine from connection details.

        :param connection_details: Dictionary with connection details
        :type connection_details: dict
        :return: SQLAlchemy engine
        :rtype: Engine
        :raises Exception: When there's an error creating the engine
        """
        connection_details = ConnectionManager.standardize_connection_details(
            connection_details
        )
        desired_type = connection_details.get("type")

        try:
            connect_args = {**kwargs}
            if self.require_ssl_args(connection_details):
                connect_args["ssl"] = self.ssl_args

            self.engine = create_engine(
                url=self.build_connection_string(connection_details),
                connect_args=connect_args,
            )
            self.engine_type = desired_type
            self.connection_details = connection_details

        except Exception as e:
            log(
                f"Error creating {desired_type} engine!{os.linesep}{e}",
                level="ERROR",
            )

        return self.engine

    def execute_query(self, query: str, continue_on_error: bool = True) -> pd.DataFrame:
        """
        Execute a SQL query and return the results as a pandas DataFrame.

        :param query: SQL query to execute
        :type query: str
        :param continue_on_error: Whether to continue execution on error, defaults to True
        :type continue_on_error: bool, optional
        :return: Query results as a pandas DataFrame
        :rtype: pd.DataFrame
        :raises Exception: When there's an error executing the query and continue_on_error is False
        """
        self.get_current_session()
        try:
            result = self.current_session.execute(text(query))
            self.current_session.commit()
            log(f"Executed query: {query}", level="DEBUG")
            self.session_query_history.append(query)
            return pd.DataFrame.from_records(result.fetchall(), columns=result.keys())
        except Exception as e:
            pretty_print(query, "FAILED QUERY")
            self.current_session.rollback()
            log(
                f"Error executing query: {os.linesep}{e}",
                level="ERROR" if not continue_on_error else "WARN",
            )
            return str(e) if continue_on_error else e

    def start_new_session(self) -> None:
        """
        Start a new database session.
        - Checks if there is  an existing engine
        - Closes the current session if it exists
        - Create a new session
        - Execute pre-run hooks if they are defined

        :return: None
        """
        if not hasattr(self, "engine"):
            self.get_new_engine()
        elif hasattr(self, "current_session"):
            self.close_current_session()
        self.current_session = sessionmaker(bind=self.engine)()
        self.session_query_history = []
        if self.should_run_hooks:
            self.execute_query(self.post_run_hooks)
            self.execute_query(self.pre_run_hooks)
            log("Executed pre-run hooks for new session.")
            self.should_run_hooks = True
        log("Started a new database session.")

    def get_current_session(self) -> Session:
        """
        Get the current database session, creating one if it doesn't exist.

        :return: Current SQLAlchemy session
        :rtype: Session
        """
        if not hasattr(self, "current_session"):
            self.start_new_session()
        return self.current_session

    def close_current_session(self) -> None:
        """
        Close the current database session.
        """
        if hasattr(self, "current_session"):
            if self.should_run_hooks:
                self.current_session.execute_query(self.post_run_hooks)
                self.should_run_hooks = False
            self.current_session.close()
            del self.current_session
            log("Closed and removed current database session.")
            self.all_query_history.extend(self.session_query_history)

    def batch_execute_queries(
        self, queries: List[str], continue_on_error: bool = True
    ) -> dict[str : pd.DataFrame]:
        """
        Execute multiple SQL queries in batch mode.

        :param queries: List of SQL queries to execute
        :type queries: List[str]
        :return: Dictionary mapping queries to their results as pandas DataFrames
        :rtype: dict[str: pd.DataFrame]
        """
        results = {}
        self.start_new_session()
        for query in queries:
            results[query] = self.execute_query(
                query, continue_on_error=continue_on_error
            )
        self.close_current_session()
        return results

    @staticmethod
    def standardize_connection_details(connection_details: dict) -> dict:
        """
        Standardizes database connection details by mapping alternative key names to expected key names.

        This function takes a dictionary of connection details that may have varied key naming
        conventions and converts them to a standardized format based on the keyword_map.

        :param connection_details: Dictionary containing database connection parameters
        :type connection_details: dict
        :raises KeyError: When a required connection parameter is missing
        :return: Dictionary with standardized connection detail keys
        :rtype: dict
        """
        standardized_connection_details = {}

        for key in connection_details.keys():
            # Keep a set a keys that weren't found in the alternative keys.
            extra_keys = set()

            # If the key is already in the expected format, add it to the standardized connection details.
            if key.lower() in ConnectionManager.keyword_map.keys():
                standardized_connection_details[key.lower()] = connection_details[key]

            # If the key is not in the expected format, check if it is in the alternative format.
            else:
                for alternative_key_set in ConnectionManager.keyword_map.values():
                    if key.lower() in alternative_key_set:
                        # If the key is in the alternative format, add it to the standardized connection details.
                        standardized_connection_details[alternative_key_set[0]] = (
                            connection_details[key]
                        )

                        # If the key was found in the alternative format, remove it from the extra keys set.
                        if key in extra_keys:
                            extra_keys.remove(key)

                        break

                    # If the key is not one of the known keys, add it anyway and check later if the base keys are all present.
                    else:
                        extra_keys.add(key)

            # If there are extra keys, add them to the standardized connection details.
            for extra_key in extra_keys:
                standardized_connection_details[extra_key] = connection_details[
                    extra_key
                ]

                log("Extra key: {} in connection details.".format(key))

        # This check makes sure all keys from alternative connection keys should be in connection details
        # base_keys = set(keyword_map.keys())

        # This alternate check is simpler - update to include Snowflake-specific keys
        base_keys = set(["type"])

        # Add required keys based on database type
        if (
            "type" in connection_details
            and connection_details["type"].lower() == "snowflake"
        ):
            base_keys.update(["account", "user", "pass", "database", "warehouse"])
        else:
            base_keys.update(["host", "user", "pass"])

        if not base_keys.issubset(standardized_connection_details.keys()):
            for key in base_keys:
                if key not in standardized_connection_details.keys():
                    log(
                        "Invalid config: Info for {} not found in connection details.".format(
                            key
                        ),
                        level="ERROR",
                    )

            log(
                "Invalid connection details provided. Expected keys: {}.".format(
                    base_keys
                ),
                level="ERROR",
            )

        return standardized_connection_details

    @classmethod
    def from_env(
        cls, prefix: str = None, connector_type: str = "database"
    ) -> "ConnectionManager":
        """
        Create a ConnectionManager instance from environment variables.

        :param prefix: Prefix for environment variables, defaults to None
        :type prefix: str, optional
        :param connector_type: Type of connector to use, defaults to "database"
        :type connector_type: str, optional
        :return: A ConnectionManager instance
        :rtype: ConnectionManager
        """
        connector_type = connector_type.upper()
        env_prefix = (
            f"{connector_type}_{prefix.upper()}"
            if prefix is not None
            else connector_type
        )

        connection_details = {
            key.replace(f"{env_prefix}_", "").lower(): value
            for key, value in os.environ.items()
            if key.startswith(env_prefix)
        }
        connection_details["type"] = connector_type

        log("Creating session manager from environment variables...")
        return cls(connection_details=connection_details)


class SingleStoreConnectionManager(ConnectionManager):
    """
    Connection manager for SingleStore database connections.

    Extends ConnectionManager with SingleStore-specific implementations.
    """

    # https://pypi.org/project/singledb-sqlalchemy/
    @override
    def build_connection_string(self, connection_details: dict) -> str:
        """
        Build a SingleStore connection string from connection details.

        :param connection_details: Dictionary with SingleStore connection details
        :type connection_details: dict
        :return: SingleStore connection string
        :rtype: str
        """
        return "mysql://{}:{}@{}:{}/{}".format(
            connection_details["user"],
            connection_details["pass"],
            connection_details["host"],
            connection_details["port"],
            (
                connection_details["database"]
                if "database" in connection_details.keys()
                else None
            ),
        )

    @override
    def get_new_engine(self, connection_details, **kwargs):
        """
        Create a new SQLAlchemy engine from connection details.
        Uses the pymysql client flag for multi-statement support.

        :param connection_details: Dictionary with connection details
        :type connection_details: dict
        :return: SQLAlchemy engine
        :rtype: Engine
        """
        #! This needs to change in case we use mysqlclient instead of pymysql
        #from pymysql.constants import CLIENT

        return super().get_new_engine(
            connection_details, #client_flag=CLIENT.MULTI_STATEMENTS
        )

    @classmethod
    def from_env(cls, prefix: str = None) -> "SingleStoreConnectionManager":
        """
        Create a SingleStoreConnectionManager instance from environment variables.

        :param prefix: Prefix for environment variables, defaults to SINGLESTORE
        :type prefix: str, optional
        :return: A SingleStoreConnectionManager instance
        :rtype: SingleStoreConnectionManager
        """
        return super().from_env(prefix=prefix, connector_type="SINGLESTORE")
