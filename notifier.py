import requests
import os
from dotenv import load_dotenv

# .env 파일을 로드
load_dotenv()

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

def send_slack_message(message: str):
    payload = {"text": message}
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code != 200:
            print(f"Slack 알림 실패: {response.text}")
    except Exception as e:
        print(f"Slack 전송 중 오류 발생: {e}")
