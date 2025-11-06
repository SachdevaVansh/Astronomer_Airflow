from airflow import DAG 
from airflow.decorators import task 
from datetime import datetime 

## Define the DAG 

with DAG(
    dag_id='math_sequence_with_taskflow',
    start_date=datetime(2023,1,1),
    schedule='@once',
    catchup=False,
) as dag:
    
    ## Task-1 Start with the initial numbers 
    @task 
    def start_number():
        initial_value=10
        print(f"Startig with {initial_value}")
        return initial_value
    
    #Task-2 Add 5 to the number 
    @task
    def add_five(number):
        new_value=number+5
        print(f"Add 5:{number} + 5 ={new_value}")
        return new_value
    
    #Task-3 Multiply 2 to the number 
    @task
    def multiply_two(number):
        new_value=number*2
        print(f"Multiply by 2:{number} *2 ={new_value}")
        return new_value
    
    #Task-4 Subtract 3 from the number 
    @task
    def subtract_three(number):
        new_value=number-3
        print(f"Subtarct 3:{number} - 3 ={new_value}")
        return new_value
    
    ## Dependencies 
    start_value=start_number()
    added_value=add_five(start_value)
    multiplied_value=multiply_two(added_value)
    subtracted_value=subtract_three(multiplied_value)
    