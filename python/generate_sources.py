import os
import sys

from src.log_manager import log, pretty_print

from src.project_manager import ProjectManager

project_home: str = os.sep + os.path.join(*__file__.split(os.sep)[:-2])
project_name: str = "dbt-swf" or os.path.basename(project_home)

project = ProjectManager(project_home)

# target = os.environ.get("DBT_DEPLOY_TARGET", "local")
# NON_PROD = target in ["LDV", "ABT"]

import argparse

parser = argparse.ArgumentParser(
    description="Connects to SingleStore and generates a sources.yml file for dbt.",
    prog="generate_sources.py",
)

parser.add_argument(
    "--database",
    type=str,
    required=False,
    help="The SingleStore database to generate sources from.",
)

parser.add_argument(
    "--tables",
    nargs="+",
    help="A custom list of tables you want to generate sources for.",
)

args = parser.parse_args()

project.log_manager.set_level("DEBUG")

# ssl_certificate_path = os.path.join(
#     project_home,
#     "assets",
#     "vdab_root_intermediate_{}prod.cert".format("non" if NON_PROD else ""),
# )

dbt_manager = project.get_dbt_manager()

with project.get_secret_manager() as secret_manager:
    secret_manager.gather_project_secrets(additional_secrets=vars(args))
    connection_details = secret_manager.parse_dbt_target(dbt_manager.target_profile)
    # connection_details["ssl_certificate_path"] = ssl_certificate_path

connection_manager = project.get_connection_manager(
    connection_details=connection_details
)

metadata_query = """
with column_info as (
	SELECT
        TABLE_SCHEMA as schema_name,
        TABLE_NAME as table_name,
        COLUMN_NAME as column_name,
        COLUMN_TYPE as data_type,
        CASE WHEN IS_NULLABLE = 'YES' THEN TRUE ELSE FALSE END as is_nullable
	FROM
        information_schema.columns
	WHERE TABLE_SCHEMA = '{database}'
),

column_constraints as (
	SELECT
        TABLE_SCHEMA as schema_name,
        TABLE_NAME as table_name,
        COLUMN_NAME as column_name,
        CASE WHEN CONSTRAINT_NAME LIKE '%UNIQUE%' THEN TRUE ELSE FALSE END as is_unique
	FROM
        information_schema.key_column_usage
	WHERE TABLE_SCHEMA = '{database}'
)

SELECT DISTINCT
    ci.schema_name,
    ci.table_name,
    ci.column_name,
    ci.data_type,
    ci.is_nullable,
    cc.is_unique
FROM
    column_info ci
LEFT JOIN column_constraints cc
    ON ci.schema_name = cc.schema_name
    AND ci.table_name = cc.table_name
    AND ci.column_name = cc.column_name
ORDER BY 1, 2, 3;
""".format(
    database=args.database or connection_manager.connection_details.get("database")
)

metadata = connection_manager.execute_query(metadata_query)

dbt_manager.refresh_source(source_info=metadata, source_name=args.database)
