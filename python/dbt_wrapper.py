import os
import sys

from src.log_manager import log, pretty_print

from src.project_manager import ProjectManager

project_home: str = os.sep + os.path.join(*__file__.split(os.sep)[:-2])
project_name: str = "dbt-academy" or os.path.basename(project_home)

project = ProjectManager(project_home)

log("Setting up dbt wrapper...")

dbt_arguments = sys.argv[1:]
info_message = """
--help
--version
compile
debug
run
test
"""

if len(dbt_arguments) == 0:
    pretty_print("dbt needs a command which tells it what to run.")
    pretty_print(info_message, header="dbt commands")
    sys.exit(1)

pretty_print(f"Running dbt wrapper with arguments: {dbt_arguments}")

project.log_manager.set_level("DEBUG")

with project.get_secret_manager() as secret_manager:
    secret_manager.gather_project_secrets()

dbt_command = (
    dbt_arguments[1]
    if dbt_arguments[0] == "dbt" and len(dbt_arguments) > 1
    else dbt_arguments[0]
)

ProjectManager.run_subprocess(" ".join(dbt_arguments), type="dbt")
