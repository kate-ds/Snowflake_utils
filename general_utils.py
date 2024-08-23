import os
import requests
import pandas as pd
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor


def run_notebook(notebool_path):
    """
    Execute the notebook
    """
    with open(notebool_path) as f:
        nb = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=-1)
    ep.preprocess(nb, {'metadata': {'path': '.'}})
    with open(notebool_path, 'w', encoding='utf-8') as f:
        nbformat.write(nb, f)

    return None


def send_slack_notif(message, url):

    data = '''{"text":"''' + message + '''"}'''
    requests.post(url, data = data, json = 'Content-type: application/json' )


def send_dict_to_slack(message, url):
    """
    Send dict to slack channel by url
    """
    formatted_message = ""
    for key, value in message.items():
        formatted_message += f"*{key}*\n"
        if isinstance(value, dict):
            for inner_key, inner_value in value.items():
                formatted_message += f"\t*_{inner_key}_*: \t{inner_value}\n"
        else:
            formatted_message += f"{value}\n"
        formatted_message += "\n"

    data = {"text": formatted_message}
    headers = {"Content-type": "application/json"}
    requests.post(url, data=json.dumps(data), headers=headers)
