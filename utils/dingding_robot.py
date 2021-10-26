# -*- coding: utf-8 -*-

import requests
import json


def send_warning(content, phone_numbers=None, url='https://oapi.dingtalk.com/robot/send'):
    if phone_numbers is None:
        phone_numbers = ['123456']
    headers = {'Content-Type': 'application/json'}
    data = {
        "msgtype": "text",
        "text": {
            "content": content
        },
        "at": {
            "atMobiles": phone_numbers,
            "isAtAll": False
        }
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    return

