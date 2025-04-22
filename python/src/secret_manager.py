"""
Secret manager for managing secrets and environment variables for projects.

This class provides a consistent interface for managing secrets from various sources,
Provides functionality for;
- loading environment variables from a dotenv file,
- adding additional secrets to the environment,
- Jinja templating for parsing secrets.
"""

import os
import jinja2

from abc import ABC

from src.log_manager import log


class SecretManager(ABC):
    """
    Abstract base class for managing secrets and environment variables for projects.
    The goal of this class is to provide a consistent interface for loading secrets from various sources.

    Different kinds of secrets can be managed by subclasses, such as environment variables, files, or vaults.
    This base class provides functionality for loading environment variables from a dotenv file.

    Example usage:
    ```python
    secret_manager = SecretManager(project_home="/path/to/project")
    secret_manager.load_project_dotenv()
    ```

    :param project_home: Path to the root directory of the project
    :type project_home: str
    """

    project_home: str

    def __init__(self, project_home: str) -> None:
        """
        Initialize a SecretManager instance.

        :param project_home: Path to the root directory of the project
        :type project_home: str
        """
        self.project_home = project_home
        log(f"Secret manager initialized.")

    def __enter__(self):
        """
        Context manager enter method.

        :return: The SecretManager instance
        :rtype: SecretManager
        """
        log("Starting secret manager.")
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        """
        Context manager exit method.

        :return: False to allow exceptions to propagate
        :rtype: bool
        """
        log("Finished running secret manager.")
        return False

    def load_project_dotenv(self) -> dict:
        """
        Load environment variables from a .env file in the project's .devcontainer directory.

        :return: A dictionary of variables loaded from the .env file, or None if the file doesn't exist
        :rtype: dict or None
        """
        from dotenv import load_dotenv

        self.dotenv_file_path = os.path.join(self.project_home, ".devcontainer", ".env")

        if not os.path.exists(self.dotenv_file_path):
            return {}

        initial_env = dict(os.environ)
        load_dotenv(self.dotenv_file_path, override=True)

        self.vars_from_dotenv = {
            key: value
            for key, value in os.environ.items()
            if key not in initial_env or initial_env[key] != value
        }
        if len(self.vars_from_dotenv) > 0:
            log(
                f"Loaded {len(self.vars_from_dotenv)} variables from {self.dotenv_file_path} into environment."
            )
        else:
            log(
                f"Variables from {self.dotenv_file_path} were already present in environment."
            )

        return self.vars_from_dotenv

    def gather_project_secrets(self, additional_secrets: dict = {}) -> None:
        """
        Gather all project secrets from dotenv file and add additional secrets on top.

        Hierarchy of secrets:
        1. dotenv file
        2. additional secrets

        :param additional_secrets: Dictionary with additional secrets to add to environment, defaults to {}
        :type additional_secrets: dict, optional
        :return: Combined dictionary of dotenv variables and additional secrets
        :rtype: dict
        """
        self.project_secrets = self.load_project_dotenv() or {}
        self.project_secrets.update(additional_secrets)

        for key, value in additional_secrets.items():
            os.environ[key] = str(value)

        log("All secrets gathered and provided to environment.")
        return self.project_secrets

    @staticmethod
    def flatten_nested_dicts(list_of_dicts):
        """
        Flattens a list of nested dictionaries into a single flat dictionary.
        Keys are joined with forward slashes to create unique keys for the flat dictionary.

        Example:
        ```python
        input = [
            {
                "host_info": {
                    "name": "localhost",
                    "url": {
                        "localhost": "127.0.0.1",
                        "prod": {"live": "myserver.url", "backup": "backup.url"},
                    },
                }
            },
            {"host_url": {"url": "https://localhost"}},
            {"host_port": {"port": 5432}},
            {},
        ]

        output = secret_manager.flatten_nested_dicts(list_of_dicts)
        ```
        For which the output would be:
        ```python
        {
            "host_info/name": "localhost",
            "host_info/url/localhost": "127.0.0.1",
            "host_info/url/prod/live": "myserver.url",
            "host_info/url/prod/backup": "backup.url",
            "host_url/url": "https://localhost",
            "host_port/port": 5432,
        }
        ```

        :param list_of_dicts: List of dictionaries to flatten
        :type list_of_dicts: list
        :return: Flattened dictionary with unique keys
        :rtype: dict
        """
        result = {}

        def flatten_dict(d, prefix=""):
            for key, value in d.items():
                new_key = f"{prefix}/{key}" if prefix else key
                if isinstance(value, dict):
                    flatten_dict(value, new_key)
                else:
                    result[new_key] = value

        for d in list_of_dicts:
            flatten_dict(d)

        return result


class dbtSecretManager(SecretManager):
    """
    Secret manager specialized for dbt projects with Jinja template support.

    :param project_home: Path to the root directory of the dbt project
    :type project_home: str
    """

    def __init__(self, project_home: str) -> None:
        """
        Initialize a dbtSecretManager instance.

        :param project_home: Path to the root directory of the project
        :type project_home: str
        """
        super().__init__(project_home)
        self.jinja_env = self.setup_jinja_env()

    def setup_jinja_env(self) -> jinja2.Environment:
        """
        Build a Jinja environment with custom delimiters for dbt templates.

        Sets dbt-specific delimiters for Jinja templates to avoid conflicts with other templating engines.
        Adds the `env_var` function to the global namespace, which allows referencing environment variables.

        :return: Jinja environment with custom delimiters
        :rtype: jinja2.Environment
        """
        self.jinja_env = jinja2.Environment(
            block_start_string="{%",
            block_end_string="%}",
            variable_start_string="{{",
            variable_end_string="}}",
            comment_start_string="{#",
            comment_end_string="#}",
            # loader=jinja2.FileSystemLoader(self.project_home),
        )

        def env_var(var_name, default=""):
            return os.environ.get(var_name, default)

        self.jinja_env.globals["env_var"] = env_var
        return self.jinja_env

    def parse_dbt_target(self, target: dict) -> None:
        """
        Parse a dbt target configuration, replacing Jinja templates with their values.

        Example usage:
        ```python
        target = {
            "host": "{{ env_var('DBT_HOST') }}",
            "user": "{{ env_var('DBT_USER') }}",
            "pass": "{{ env_var('DBT_PASS') }}",
        }
        parsed_target = secret_manager.parse_dbt_target(target)
        ```

        :param target: Dictionary containing dbt target configuration
        :type target: dict
        :return: Parsed connection info
        :rtype: dict
        """
        return {key: self.render_jinja(value) for key, value in target.items()}

    def render_jinja(self, jinja_input: str, params: dict = {}):
        """
        Render a string containing Jinja templates with environment variables.

        :param jinja_input: String possibly containing Jinja templates
        :type jinja_input: str
        :param params: Dictionary with additional parameters for rendering, defaults to {}
        :type params: dict, optional
        :return: Rendered string with variables replaced
        :rtype: str or original type if input is not a string
        :raises jinja2.exceptions.TemplateSyntaxError: When the Jinja template contains syntax errors
        :raises jinja2.exceptions.UndefinedError: When referenced variables are undefined
        """
        if not isinstance(jinja_input, str):
            return jinja_input

        try:
            jinja_input = self.jinja_env.from_string(jinja_input).render(**params)
        except jinja2.exceptions.TemplateSyntaxError as e:
            log(f"Error in Jinja template: {e}", level="ERROR")
        except jinja2.exceptions.UndefinedError as e:
            log(f"Unknown error parsing Jinja template: {e}", level="ERROR")
        finally:
            return jinja_input
