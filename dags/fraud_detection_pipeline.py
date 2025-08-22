from airflow.decorators import dag, task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from include.dbt.fraud.cosmos_config import  DBT_CONFIG, DBT_PROJECT_CONFIG
from airflow.models.baseoperator import chain

from datetime import datetime

AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTION_RAW = "2788db0b-80b5-4d8a-a7af-a7da3dba8846"
AIRBYTE_JOB_ID_LOAD_LABELED_TRANSACTION_RAW = "92d00e7c-0949-48d9-9481-1d79577101da"
AIRBYTE_JOB_ID_RAW_TO_STAGING = "49e72702-f110-4d36-98da-c2667a5ba98a"

@dag(
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['airbyte', 'risk']
)

def fraud_detection_pipeline():
    load_customer_transactions_raw = AirbyteTriggerSyncOperator(
        task_id = "load_customer_transactions_raw",
        airbyte_conn_id = "airbyte",
        connection_id = AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTION_RAW
    )

    load_labeled_transactions_raw = AirbyteTriggerSyncOperator(
        task_id = "load_labeled_transactions_raw",
        airbyte_conn_id = "airbyte",
        connection_id = AIRBYTE_JOB_ID_LOAD_LABELED_TRANSACTION_RAW
    )

    write_to_staging = AirbyteTriggerSyncOperator(
        task_id = "write_to_staging",
        airbyte_conn_id = "airbyte",
        connection_id = AIRBYTE_JOB_ID_RAW_TO_STAGING
    )

    @task
    def airbyte_job_done():
        return True
    
    @task.external_python(python='/opt/airflow/soda_env/bin/python')
    def audit_customer_transactions(scan_name="customer_transactions",
                                    checks_subpath="tables",
                                    data_source="staging"):
        from include.soda.helpers import check
        check(scan_name, checks_subpath, data_source)

    @task.external_python(python='/opt/airflow/soda_env/bin/python')
    def audit_labeled_transactions(scan_name="labeled_transactions",
                                    checks_subpath="tables",
                                    data_source="staging"):
        from include.soda.helpers import check
        check(scan_name, checks_subpath, data_source)

    @task
    def quality_checks_done():
        return True
    
    publish = DbtTaskGroup(
        group_id="publish",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:models"]
        )
    )

    chain(
        [load_customer_transactions_raw, load_labeled_transactions_raw],
        write_to_staging,
        airbyte_job_done(),
        [audit_customer_transactions(), audit_labeled_transactions()],
        quality_checks_done(),
        publish
    )


fraud_detection_pipeline()

