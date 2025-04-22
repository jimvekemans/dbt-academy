"""
DBT Manager class for managing DBT projects and configurations.

This class provides an interface for interacting with DBT projects,
including access to project configuration, profiles, and directory paths.
"""

from src.log_manager import log
from src.secret_manager import dbtSecretManager as SecretManager
from src.connection_manager import SingleStoreConnectionManager as ConnectionManager

import os
import pandas as pd
from pathlib import Path
import yaml


class dbtMetadataManager:
    """
    Manages metadata for dbt projects.

    This class handles loading and accessing dbt project configuration files,
    including project and profile settings.

    :param project_home: Path to the dbt project directory
    :type project_home: str
    """

    profiles_yml_data: dict
    project_yml_data: dict

    def __init__(self, project_home: str) -> None:
        """
        Initialize the dbt metadata manager for a given project home path.

        :param project_home: Path to the dbt project directory
        :type project_home: str
        """
        self.project_home = project_home
        self.dbt_project_yml_path = os.path.join(self.project_home, "dbt_project.yml")
        self.dbt_profiles_yml_path = os.path.join(self.project_home, "profiles.yml")
        self.get_project_data()
        log("Retrieved content of dbt_project.yml.")
        self.get_profiles_data()
        log("Retrieved content of profiles.yml.")

    @staticmethod
    def yaml_to_dict(yml_file_path: str) -> dict:
        """
        Loads a YAML file and converts it to a Python dictionary.

        :param yml_file_path: Path to the YAML file
        :type yml_file_path: str
        :raises AssertionError: When the YAML file does not exist
        :return: Dictionary representation of the YAML file
        :rtype: dict
        """
        if not os.path.exists(yml_file_path):
            log(f"Could not find metadata file: {yml_file_path}", level="ERROR")
        return yaml.safe_load(Path(yml_file_path).read_text())

    def get_profiles_data(self) -> dict:
        """
        Load and return the raw profiles.yml data as a dictionary.

        :return: Dictionary representation of profiles.yml
        :rtype: dict
        """
        if not hasattr(self, "profiles_yml_data"):
            self.profiles_yml_data = self.yaml_to_dict(self.dbt_profiles_yml_path)
        return self.profiles_yml_data

    def get_project_data(self) -> dict:
        """
        Load and return the raw dbt_project.yml data as a dictionary.

        :return: Dictionary representation of dbt_project.yml
        :rtype: dict
        """
        if not hasattr(self, "project_yml_data"):
            self.project_yml_data = self.yaml_to_dict(self.dbt_project_yml_path)
        return self.project_yml_data


class dbtSource:
    """
    Represents a DBT source configuration.

    This class provides an interface for interacting with DBT source configurations,
    including loading and validating source data.
    """

    filename: str = "sources.yml"
    name: str
    sources_dir: str
    tables: list
    REQUIRED_SOURCE_FIELDS: set = {"database", "schema", "tables"}
    REQUIRED_TABLE_FIELDS: set = {"schema_name", "table_name", "column_name"}

    def __init__(
        self,
        source_info: pd.DataFrame,
        source_dir: str = os.path.join("models", "sources"),
    ) -> None:
        self.source_info = self.parse_source_info(source_info)
        self.sources_dir = source_dir
        # self.name = self.source_info.get(
        #     "source_name", self.source_info.get("name", None)
        # )

    def parse_source_info(self, source_info: pd.DataFrame) -> dict:
        """
        Parses the source information and returns a dictionary representation.

        :param source_info: Source information to parse
        :type source_info: pd.DataFrame
        :return: Parsed source information as a dictionary
        :rtype: dict
        """
        if not self.validate_source_info(source_info):
            raise ValueError("Invalid source information provided.")

        self.source_info = source_info

        return self.source_info

    def validate_source_info(self, source_info: dict) -> bool:
        """
        Validates the source information dictionary.

        :param source_info: Source information to validate
        :type source_info: dict
        :raises ValueError: If the source information is invalid
        :raises TypeError: If the source information is not a dictionary
        :raises KeyError: If required fields are missing
        :return: True if the source information is valid, False otherwise
        :rtype: bool
        """

        #! TODO raise exceptions
        # if not self.REQUIRED_SOURCE_FIELDS.issubset(source_info[0].keys()):
        #     return False
        # if tables := source_info.get("tables"):
        #     if not isinstance(tables, list):
        #         return False
        #     elif len(tables) == 0:
        #         self.tables = []
        #         return True
        #     else:
        #         self.tables = tables
        return self.REQUIRED_TABLE_FIELDS.issubset(source_info.columns)

    def generate_sources_yml(self, source_db: str) -> None:
        if not hasattr(self, "source_info"):
            log(
                "Source info has not been set. Please provide source info before generating sources.",
                level="ERROR",
            )

        sources_filename = f"sources_{source_db}.yml"
        yml_file_path = os.path.join(self.sources_dir, sources_filename)

        log("Creating sources YAML file for source {}".format(source_db))

        source_schema = source_db

        sources = {
            "name": source_db,
            "database": source_db,
            "schema": source_schema,
            "tables": [],
        }

        table_column_info = {}
        for row in self.source_info.to_dict(orient="records"):
            current_col_info = {"name": row.get("column_name")}
            if "column_description" in row.keys():
                if len(row.get("column_description")) > 0:
                    current_col_info["description"] = row.get("column_description", "")
            if "data_type" in row.keys():
                if len(row.get("data_type")) > 0:
                    current_col_info["data_type"] = row.get("data_type")
            if "meta" in row.keys():
                if len(row.get("meta")) > 0:
                    current_col_info["meta"] = {
                        "contains_pii": row.get("column_contains_pii"),
                    }
            if any([row.get("is_nullable") == 1, row.get("is_unique") == 1]):
                current_col_info["tests"] = []
                if row.get("is_nullable") == 1:
                    current_col_info["tests"].append("not_null")
                if row.get("is_unique") == 1:
                    current_col_info["tests"].append("unique")

            if row.get("table_name") in table_column_info.keys():
                table_column_info[row.get("table_name")].append(current_col_info)
            else:
                table_column_info[row.get("table_name")] = [current_col_info]

        for table_name in self.source_info.table_name.unique():
            sources["tables"].append(
                {"name": table_name, "columns": table_column_info.get(table_name)}
            )

        with open(yml_file_path, "w") as file:
            yaml.dump(
                {"version": 2, "sources": [sources]},
                file,
                default_flow_style=False,
                sort_keys=False,
            )


class dbtManager:
    """
    Manages DBT project configuration, metadata, and operations.

    This class provides an interface for interacting with DBT projects,
    including access to project configuration, profiles, and directory paths.
    """

    metadata_manager: dbtMetadataManager

    project_home: str
    project_name: str
    project_version: str

    dbt_project_info: dict
    profiles_file: dict

    config: dict
    target_name: str
    target: dict

    models_dir: str
    models_config: dict

    snapshots_dir: str
    snapshots_config: dict

    macros_dir: str
    seeds_dir: str
    tests_dir: str
    assets_dir: str
    analysis_dir: str

    log_path: str
    clean_targets: list
    packages_install_path: str

    vars_config: dict
    debug_mode: bool = False

    def __init__(self, project_home: str, target: str = None) -> None:
        """
        Initialize a dbtManager instance.

        :param project_home: Path to the project's home directory
        :type project_home: str
        :param target: Target profile to use from profiles.yml, defaults to None
        :type target: str, optional
        """
        self.project_home = project_home
        self.target_name = target
        self.metadata_manager = dbtMetadataManager(project_home)

        self.dbt_project_info = self.metadata_manager.get_project_data()
        self.parse_dbt_project_info()

        self.profiles_info = self.metadata_manager.get_profiles_data()
        self.parse_profiles_info(target)

        log("Initialized DBT Manager for project '{}'".format(self.project_name))

    def attach_secret_manager(self, secret_manager: SecretManager) -> SecretManager:
        """
        Attaches a Secret manager to the DBT manager instance.

        :return: The Secret manager instance
        :rtype: SecretManager
        """
        self.secret_manager = secret_manager or SecretManager(self.project_home)
        log("Secret manager attached to DBT manager for {}".format(self.project_name))
        return self.secret_manager

    def attach_connection_manager(self, connection_manager: ConnectionManager) -> None:
        """
        Attaches a Connection manager to the DBT manager instance.

        :param connection_manager: The Connection manager instance
        :type connection_manager: ConnectionManager
        """
        self.connection_manager = connection_manager
        log(
            "Connection manager attached to DBT manager for {}".format(
                self.project_name
            )
        )
        return self.connection_manager

    def parse_dbt_project_info(self, dbt_project_info: dict = None) -> dict:
        """
        Parses the DBT project configuration and sets up directory paths.

        :param dbt_project_info: DBT project configuration dictionary, defaults to None
        :type dbt_project_info: dict, optional
        :return: The parsed DBT project information
        :rtype: dict
        """
        if not dbt_project_info and hasattr(self, "metadata_manager"):
            self.dbt_project_info = self.metadata_manager.get_project_data()
        self.project_name = self.dbt_project_info.get("name")
        self.project_version = self.dbt_project_info.get("version")

        # Extract paths from dbt_project.yml
        self.models_dir = os.path.join(
            self.project_home,
            self.dbt_project_info.get("model-paths", ["models"]).pop(),
        )
        self.macros_dir = os.path.join(
            self.project_home,
            self.dbt_project_info.get("macro-paths", ["macros"]).pop(),
        )
        self.macros_dir = self.macros_dir if os.path.exists(self.macros_dir) else None

        # Optional directories
        self.analysis_dir = os.path.join(
            self.project_home,
            self.dbt_project_info.get("analysis-paths", ["analysis"]).pop(),
        )
        self.analysis_dir = (
            self.analysis_dir if os.path.exists(self.analysis_dir) else None
        )
        self.seeds_dir = os.path.join(
            self.project_home,
            self.dbt_project_info.get("seed-paths", ["seeds"]).pop(),
        )
        self.seeds_dir = self.seeds_dir if os.path.exists(self.seeds_dir) else None
        self.tests_dir = os.path.join(
            self.project_home,
            self.dbt_project_info.get("test-paths", ["tests"]).pop(),
        )
        self.tests_dir = self.tests_dir if os.path.exists(self.tests_dir) else None
        self.snapshots_dir = os.path.join(
            self.project_home,
            self.dbt_project_info.get("snapshot-paths", ["snapshots"]).pop(),
        )
        self.snapshots_dir = (
            self.snapshots_dir if os.path.exists(self.snapshots_dir) else None
        )
        self.assets_dir = os.path.join(
            self.project_home,
            self.dbt_project_info.get("asset-paths", ["assets"]).pop(),
        )
        self.assets_dir = self.assets_dir if os.path.exists(self.assets_dir) else None

        # Artifact metadata
        self.log_path = self.dbt_project_info.get("log-path", "logs")
        self.clean_targets = self.dbt_project_info.get("clean-targets", ["target"])
        self.packages_install_path = self.dbt_project_info.get(
            "packages-install-path", "dbt_packages"
        )

        # Model configurations and vars
        self.models_config = self.dbt_project_info.get("models", {})
        self.snapshots_config = self.dbt_project_info.get("snapshots", {})
        self.vars_config = self.dbt_project_info.get("vars", {})
        return self.dbt_project_info

    def parse_profiles_info(self, profiles_info: dict = None) -> dict:
        """
        Parses the profiles configuration and sets the target profile.

        :param profiles_info: Profiles configuration dictionary, defaults to None
        :type profiles_info: dict, optional
        :return: The parsed profiles information
        :rtype: dict
        """
        if not profiles_info and hasattr(self, "metadata_manager"):
            self.profiles_info = self.metadata_manager.get_profiles_data()
        self.target_name = (
            self.profiles_info.get(self.project_name).get("target") or self.target_name
            if hasattr(self, "target_name")
            else None
        )
        self.target_profile = (
            self.profiles_info.get(self.project_name)
            .get("outputs")
            .get(self.target_name)
        )
        return self.profiles_info

    def get_target(self) -> dict:
        """
        Returns the current target profile configuration.

        :return: Target profile configuration dictionary
        :rtype: dict
        """
        return self.target

    def refresh_source(
        self, source_info: pd.DataFrame, source_name: str = "landing"
    ) -> None:
        """
        Refreshes the source metadata.

        :param metadata: Metadata DataFrame to refresh
        :type metadata: pd.DataFrame
        """
        source = dbtSource(
            source_info, source_dir=os.path.join(self.models_dir, "sources")
        )

        if not hasattr(self, "sources"):
            self.sources = {source_name: source}
        else:
            self.sources[source_name] = source

        self.sources[source_name].generate_sources_yml(source_name)
