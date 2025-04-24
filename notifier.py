# notifier.py
import requests

SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"

def send_slack_message(message: str):
    payload = {"text": message}
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code != 200:
            print(f"Slack알림 실페: {response.text}")
    except Exception as e:
        print(f"Slack정송중 오류 발생: {e}")
