"""
╭─ Project Manager ─────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ The `ProjectManager` class is a high-level manager for a project, based on the idea of generic building blocks.       │
│ It is designed to be used as a context manager, so that it can be easily integrated into other scripts and workflows. │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

The `ProjectManager` class provides the following functionality:
- Manages project configuration and metadata
- Provides access to secrets and connection details
- Provides access to DBT and Vault managers
- Runs subprocesses with logging
"""

import os
from typing import Optional

from src.log_manager import LogManager, log, pretty_print

from src.dbt_manager import dbtManager
from src.connection_manager import SingleStoreConnectionManager as ConnectionManager
from src.secret_manager import dbtSecretManager as SecretManager


class ProjectManager:
    project_home: str
    project_name: str

    def __init__(
        self,
        project_home: Optional[str] = None,
        project_name: Optional[str] = None,
    ):
        """
        Initialize a ProjectManager instance.

        :param project_home: Path to the project's home directory, defaults to None
        :type project_home: Optional[str], optional
        :param project_name: Name of the project, defaults to None (derived from project_home)
        :type project_name: Optional[str], optional
        """
        if project_home is None and __name__ == "__main__":
            self.project_home = os.path.join(*__file__.split(os.sep)[:-3])
        elif project_home is None:
            self.project_home = os.path.join(*__file__.split(os.sep)[:-2])
        else:
            self.project_home = project_home
        self.project_name = project_name or os.path.basename(project_home)
        self.log_directory = os.path.join(self.project_home, "logs")
        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)
        self.log_manager = LogManager(
            logs_directory=os.path.join(self.project_home, "logs"),
            project_name=self.project_name,
        )

    def __enter__(self):
        """
        Context manager enter method.

        :return: The ProjectManager instance
        :rtype: ProjectManager
        """
        log("Starting project manager for {}".format(self.project_name))
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        """
        Context manager exit method.

        :return: False to allow exceptions to propagate
        :rtype: bool
        """
        log("Finished running project manager for {}".format(self.project_name))
        self.log_manager.save_logs_to_json()
        return False

    def get_secret_manager(self) -> SecretManager:
        """
        Creates and attaches a local secret manager to the project manager.
        The secret manager will attempt to gather secrets from the local .env file,
        as well as any additional secrets provided.

        :return: The Secret manager instance
        :rtype: SecretManager
        """
        self.secret_manager = SecretManager(project_home=self.project_home)
        log(f"Secret manager attached to project manager for {self.project_name}")
        return self.secret_manager

    def get_connection_manager(self, connection_details: Optional[dict] = None):
        """
        Creates and attaches a Connection manager to the project manager.
        The connection manager is used to manage database connections.
        If no connection details are provided, it will try to load them from the environment.

        :param connection_details: Dictionary containing connection details, defaults to None
        :type connection_details: dict, optional
        :return: The Connection manager instance
        :rtype: ConnectionManager
        """
        self.connection_manager = (
            ConnectionManager(connection_details=connection_details)
            if connection_details
            else ConnectionManager.from_env()
        )
        log(f"Connection manager attached to project manager for {self.project_name}")
        return self.connection_manager

    def get_dbt_manager(self, target_profile: Optional[str] = None) -> dbtManager:
        """
        Creates and attaches a DBT manager to the project manager.
        The dbt manager holds the DBT project information and manages DBT operations.
        If no target profile is provided, it will use the default target set in profiles.yml.

        :param target_profile: The target profile to use for DBT operations, defaults to target in profiles.yml
        :type target_profile: str, optional
        :return: The DBT manager instance
        :rtype: dbtManager
        """
        self.dbt_manager = dbtManager(
            project_home=self.project_home, target=target_profile
        )
        log(f"DBT manager attached to project manager for {self.project_name}")
        return self.dbt_manager

    @staticmethod
    def run_subprocess(
        command: str,
        type: str = "",
        env: dict = None,
        log_subcommand_errors: bool = True,
    ):
        """
        Runs a subprocess with the specified command and environment.

        :param command: The command to execute
        :type command: str
        :param type: The command type prefix, defaults to ""
        :type type: str, optional
        :param env: Environment variables to set for the subprocess, defaults to None
        :type env: dict, optional
        :param log_subcommand_errors: Whether to log subprocess errors, defaults to True
        :type log_subcommand_errors: bool, optional
        :raises subprocess.SubprocessError: If the subprocess execution fails
        """
        command = command if command.startswith(f"{type} ") else f"{type} {command}"

        import subprocess

        pretty_print(f"Running: '{command}'", "Subprocess")
        process = subprocess.Popen(
            command,
            env=os.environ.copy() if env is None else env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line buffered
            shell=True,
        )

        for line in process.stdout:
            line = line.strip()
            log(line)

        process.wait()

        if log_subcommand_errors:
            error_message = process.stderr.read()
            if error_message:
                log(error_message, level="ERROR")
