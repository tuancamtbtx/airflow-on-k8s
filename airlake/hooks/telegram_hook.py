import json

from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException


class TelegramHook(HttpHook):
    """
       Interact with Telegram.

       https://core.telegram.org/bots#6-botfather
    """

    def __init__(self, telegram_conn_id):

        conn = self.get_connection(telegram_conn_id)
        self.token = conn.password
        extra = conn.extra_dejson

        try:
            self.chat_id = extra['chat_id']
        except:
            raise AirflowException('No Telegram chat_id supplied')

        proxy = extra.get('proxy', {})
        self.proxy = {'https': proxy}

        super().__init__(method='POST', http_conn_id=telegram_conn_id)

    def send(self, text='ping', parse_mode='Markdown'):
        payload = {
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': parse_mode
        }

        endpoint = 'bot{}/sendMessage'.format(self.token)
        return self.run(
                endpoint=endpoint,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'},
                extra_options={'proxies': self.proxy})
