import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are
# examples of tasks created by instantiating the Postgres Operator

with DAG(
        dag_id="postgres_operator_dag",
        start_date=datetime.datetime(2022, 4, 27),
        schedule_interval="@once",
        catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="postgres_default",
        sql="sql/pet_schema.sql",
    )

    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        postgres_conn_id="postgres_default",
        sql="sql/pet_schema.sql",
    )

    get_all_pets = PostgresOperator(
        task_id="get_all_pets", sql="SELECT * FROM pet;")

    get_birth_date = PostgresOperator(
        task_id="get_birth_date",
        sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
        parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        runtime_parameters={'statement_timeout': '3000ms'},
    )

    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date

