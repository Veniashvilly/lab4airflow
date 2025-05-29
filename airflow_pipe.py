import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OrdinalEncoder, OneHotEncoder, PowerTransformer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer # т.н. преобразователь колонок
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import root_mean_squared_error
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from pathlib import Path
import os
from datetime import timedelta
from train import train

def download_data():
    df = pd.read_csv("https://raw.githubusercontent.com/Veniashvilly/dtset/refs/heads/main/Car_sales.csv")
    return df

def clear_data():
    df = download_data()

    # Оставим только нужные колонки и удалим строки с пропущенными значениями
    df = df[[
        'Manufacturer', 'Model', 'Vehicle_type',
        'Sales_in_thousands', 'Engine_size', 'Horsepower',
        'Curb_weight', 'Fuel_efficiency', 'Price_in_thousands'
    ]].dropna().reset_index(drop=True)

    # Закодируем категориальные признаки
    cat_columns = ['Manufacturer', 'Model', 'Vehicle_type']
    encoder = OrdinalEncoder()
    df[cat_columns] = encoder.fit_transform(df[cat_columns])


    df.to_csv("df_clear.csv", index=False)

dag = DAG(
    dag_id="train_pipe1",
    start_date=datetime(2025, 2, 3),
    schedule=timedelta(minutes=5),
    catchup=False,
)
download_task = PythonOperator(python_callable=download_data, task_id = "download_cars", dag = dag)
clear_task = PythonOperator(python_callable=clear_data, task_id = "clear_cars", dag = dag)
train_task = PythonOperator(python_callable=train, task_id = "train_cars", dag = dag)
download_task >> clear_task >> train_task
