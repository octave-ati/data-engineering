from pathlib import Path
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from airflow.operators.bash_operator import BashOperator

# Default DAG args
default_args = {
    "owner": "airflow",
    "catch_up": False,
}
BASE_DIR = Path(__file__).parent.parent.parent.absolute()
GE_DIR = Path(BASE_DIR, "great_expectations")
DBT_DIR = Path(BASE_DIR, "dbt_transforms")

@dag(
    dag_id="dataops",
    description="DataOps workflows",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["dataops"],
)
def dataops():
    """Production DataOps workflows"""

    # Extract + Load
    extract_and_load_projects = AirbyteTriggerSyncOperator(
        task_id="extract_and_load_projects",
        airbyte_conn_id="airbyte",
        connection_id="5d952c97-620c-44fb-8b00-fc5cb873ba23",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3,
    )
    extract_and_load_tags = AirbyteTriggerSyncOperator(
        task_id="extract_and_load_tags",
        airbyte_conn_id="airbyte",
        connection_id="26c08f6f-3b7b-4a91-9c9c-7d4fc1303138",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3,
    )

    # Validation (EL)
    validate_projects = GreatExpectationsOperator(
        task_id="validate_projects",
        checkpoint_name="projects",
        data_context_root_dir=GE_DIR,
        fail_task_on_validation_failure=True,
    )
    validate_tags = GreatExpectationsOperator(
        task_id="validate_tags",
        checkpoint_name="tags",
        data_context_root_dir=GE_DIR,
        fail_task_on_validation_failure=True,
    )

    # Transform
    transform = BashOperator(task_id="transform", bash_command=f"cd '{DBT_DIR}' && dbt run && dbt test")

    # Validate transform
    validate_transforms = GreatExpectationsOperator(
        task_id="validate_transforms",
        checkpoint_name="labeled_projects",
        data_context_root_dir=GE_ROOT_DIR,
        fail_task_on_validation_failure=True,
    )

    # Define DAG
    extract_and_load_projects >> validate_projects
    extract_and_load_tags >> validate_tags
    [validate_projects, validate_tags] >> transform >> validate_transforms

# Run DAG
do = dataops()