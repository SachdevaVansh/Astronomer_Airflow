from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime 

## Define the functions for each task 

def start_number(**context):    
    context['ti'].xcom_push(key='current_val',value=10)
    print("Starting number 10")

def add_five(**context):
    current_val=context['ti'].xcom_pull(key='current_val',task_ids='start_task')
    new_val=current_val+5
    context['ti'].xcom_push(key='current_val',value=new_val)
    print(f"Add 5:{current_val}+5={new_val}")

def multiply_by_two(**context):
    current_val=context['ti'].xcom_pull(key="current_val",task_ids="add_five_task")
    new_val=current_val*2
    context['ti'].xcom_push(key='current_val',value=new_val)
    print(f"Multiply by 2:{current_val}*2={new_val}")

def subtract_three(**context):
    current_val=context['ti'].xcom_pull(key="current_val",task_ids="multiply_by_two_task")
    new_val=current_val-3
    context['ti'].xcom_push(key='current_val',value=new_val)
    print(f"Subtract 3:{current_val}-3={new_val}")

def square_number(**context):
    current_val=context['ti'].xcom_pull(key="current_val",task_ids="subtract_task")
    new_val=current_val**2
    context['ti'].xcom_push(key='current_val',value=new_val)
    print(f"Square:{current_val}^2={new_val}")

## Define the DAG 
with DAG(
    dag_id='math_sequence_dag',
    start_date=(2025,5,11),
    schedule='@once',
    catchup=False
) as dag:
    
    start_task=PythonOperator(task_id='start_task',python_callable=start_number,provide_context=True)
    add_five_task=PythonOperator(task_id='add_five_task',python_callable=add_five,provide_context=True)
    multiply_by_two_task=PythonOperator(task_id='multiply_by_two_task',python_callable=multiply_by_two,provide_context=True)
    subtract_task=PythonOperator(task_id='subtract_task',python_callable=subtract_three,provide_context=True)
    square_number_task=PythonOperator(task_id='square_number_task',python_callable=square_number,provide_context=True)

    ##Dependencies
    start_task >> add_five_task >> multiply_by_two_task >> subtract_task >> square_number_task