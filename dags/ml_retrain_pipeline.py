import os
import random
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException

MODEL_VERSION = os.getenv("MODEL_VERSION", "v2.0.0-pro")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "...")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "...")

ACCURACY_THRESHOLD = 0.80
F1_THRESHOLD = 0.75

def validate_data(**context):

    data_drift = random.random()
    print(f"Проверка дрейфа данных: {data_drift:.2f}")
    if data_drift > 0.95:
        raise AirflowFailException("Обнаружен дрейф данных.")
    print("Данные валидированы.")

def train_variant(variant_name, **context):

    print(f"Запуск эксперимента: {variant_name}")
    
    #  расчет метрик
    metrics = {
        "variant": variant_name,
        "accuracy": random.uniform(0.70, 0.95),
        "precision": random.uniform(0.70, 0.95),
        "recall": random.uniform(0.70, 0.95),
        "f1": random.uniform(0.70, 0.95)
    }
    
    print(f"Эксперимент {variant_name} завершен. Metrics: {metrics}")
    context['ti'].xcom_push(key=f'metrics_{variant_name}', value=metrics)

def select_best_model(**context):
    ti = context['ti']
    rf_metrics = ti.xcom_pull(key='metrics_RandomForest', task_ids='train_RF')
    gb_metrics = ti.xcom_pull(key='metrics_GradientBoosting', task_ids='train_GB')
    
    best = rf_metrics if rf_metrics['f1'] > gb_metrics['f1'] else gb_metrics
    print(f"Лучшая модель: {best['variant']} с F1={best['f1']:.4f}")
    ti.xcom_push(key='best_model_metrics', value=best)
    
    if best['accuracy'] >= ACCURACY_THRESHOLD and best['f1'] >= F1_THRESHOLD:
        return 'deploy_model'
    return 'skip_deployment'

def deploy_model(**context):
    best_metrics = context['ti'].xcom_pull(key='best_model_metrics', task_ids='model_selection')
    print(f"Развертывание версии {MODEL_VERSION}. Тип: {best_metrics['variant']}")

def send_telegram_report(**context):
    best_metrics = context['ti'].xcom_pull(key='best_model_metrics', task_ids='model_selection')
    
    message = (
        f"*Model Deployed: {MODEL_VERSION}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"*Best Variant:* {best_metrics['variant']}\n"
        f"*Accuracy:* {best_metrics['accuracy']:.4f}\n"
        f"*F1-Score:* {best_metrics['f1']:.4f}\n"
        f"*Precision:* {best_metrics['precision']:.4f}\n"
        f"*Recall:* {best_metrics['recall']:.4f}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Статус: Система обновлена."
    )
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"})



with DAG(
    dag_id='ml_retrain_complex_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['mlops', 'advanced']
) as dag:

    data_validation = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )


    train_rf = PythonOperator(
        task_id='train_RF',
        python_callable=train_variant,
        op_kwargs={'variant_name': 'RandomForest'}
    )

    train_gb = PythonOperator(
        task_id='train_GB',
        python_callable=train_variant,
        op_kwargs={'variant_name': 'GradientBoosting'}
    )

    model_selection = BranchPythonOperator(
        task_id='model_selection',
        python_callable=select_best_model
    )

    deploy = PythonOperator(
        task_id='deploy_model',
        python_callable=deploy_model
    )

    skip_deploy = EmptyOperator(task_id='skip_deployment')

    notify = PythonOperator(
        task_id='notify_success',
        python_callable=send_telegram_report,
        trigger_rule='none_failed_min_one_success'
    )

    # Зависимости
    data_validation >> [train_rf, train_gb] >> model_selection
    model_selection >> deploy >> notify
    model_selection >> skip_deploy