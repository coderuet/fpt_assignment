import requests
from airflow.models import Variable

DISCORD_WEBHOOK_URL = Variable.get("DISCORD_WEBHOOK_URL")

def send_discord_message(username: str, message: str):
    data = {
        "username": username,
        "content": message
    }
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to send Discord webhook: {e}")

def on_success_callback(context):
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")

    message = f"✅ Task `{task_id}` in DAG `{dag_id}` succeeded at `{execution_date}`"
    send_discord_message("Airflow Success Bot", message)

def on_failure_callback(context):
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    error = context.get("exception")

    message = f"❌ Task `{task_id}` in DAG `{dag_id}` failed at `{execution_date}`.\nError: `{error}`"
    send_discord_message("Airflow Failure Bot", message)
