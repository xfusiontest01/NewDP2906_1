from datetime import datetime
from airflow import DAG
from udp_datafilter_operator import UDPDataFilterOperator

with DAG(dag_id="Clinical-Data-pipeline",schedule_interval="0 * * * *",start_date=datetime(2020,11,1),concurrency=16,max_active_runs=16,dagrun_timeout=None,orientation="LR",catchup=True,is_paused_upon_creation=False) as dag:

    TASK_arrow-spark = UDPDataFilterOperator(email_on_retry=False,email_on_failure=False,retries=0,retry_exponential_backoff=False,depends_on_past=False,wait_for_downstream=False,priority_weight=1,pool_slots=1,task_concurrency=None,do_xcom_push=True,conn_id="spark_conn",total_executor_cores=None,executor_cores=None,name="arrow-spark",num_executors=None,status_poll_interval=1,verbose=False)

