from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime 

## Define the function for task-1
def preprocess_data():
    print("Prprocessing the data...")

## Define the function for task2
def train_model():
    print("Training the model...")

## Define the function for task 3
def evaluate_model():
    print("Evaluation of model...")

# Define DAG

with DAG(
    'ml_pipeline',
    start_date=datetime(2024,1,1,),
    schedule='@weekly',
    ) as dag:
    
    ## Define the task 
    preprocess= PythonOperator(task_id="preprocess_task",python_callable=preprocess_data)
    train=PythonOperator(task_id="train_task",python_callable=train_model)
    evaluate=PythonOperator(task_id="evaluate_task",python_callable=evaluate_model)

    ## Set dependencies 
    preprocess >> train >> evaluate