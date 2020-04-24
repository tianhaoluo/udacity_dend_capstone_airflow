from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators.stage_redshift import StageToRedshiftOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    "owner": "Tianhao",
    "start_date": days_ago(1)
}

dag = DAG(dag_id="my_sample_dag", 
            default_args = args, 
            schedule_interval=None,
            template_searchpath=['/usr/local/airflow'] )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    CREATE TABLE IF NOT EXISTS staging_users (
        user_id int8 NOT NULL,
        gender varchar(32),
        age int4,
        occupation_id int4,
        zipcode int8
    );
    CREATE TABLE IF NOT EXISTS staging_ratings (
        user_id int8 NOT NULL,
        movie_id int8 NOT NULL,
        rating int4 NOT NULL,
        timestamp int8
    );
    CREATE TABLE IF NOT EXISTS staging_movies (
        movie_id int8,
        title varchar(32),

    );
    """
)

stage_users_to_redshift = StageToRedshiftOperator(
    task_id='Stage_users',
    dag=dag,
    table="staging_users",
    s3_bucket="udacitytluo",
    s3_key="ml-1m/users.dat"
)

stage_ratings_to_redshift = StageToRedshiftOperator(
    task_id='Stage_ratings',
    dag=dag,
    table="staging_ratings",
    s3_bucket="udacitytluo",
    s3_key="ml-1m/ratings.dat"
)

stage_movies_to_redshift = StageToRedshiftOperator(
    task_id='Stage_movies',
    dag=dag,
    table="staging_movies",
    s3_bucket="udacitytluo",
    s3_key="ml-1m/movies.dat"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables >> [stage_users_to_redshift, stage_ratings_to_redshift, stage_movies_to_redshift] 
[stage_users_to_redshift, stage_ratings_to_redshift, stage_movies_to_redshift] >> end_operator