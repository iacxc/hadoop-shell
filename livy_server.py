
import requests
import time


class LivySession(object):
    HEADERS = {'Content-Type': 'application/json'}
    @staticmethod
    def get_sessions(livy_url):
        if not livy_url.startswith('http://'):
            livy_url = 'http://' + livy_url
        url = f'{livy_url}/sessions'
        r = requests.get(url)
        return r.json()

    def __init__(self, livy_url, name, py_files='', conf=None):
        if not livy_url.startswith('http://'):
            livy_url = 'http://' + livy_url

        self.__livy_url = livy_url
        self.__session_id = None

        self._create(name, py_files, conf)

    def _create(self, name, py_files, conf):
        if conf is None:
            conf = {}

        url = f'{self.__livy_url}/sessions'
        post_data = {'name': name,
                     'kind': 'pyspark',
                     }
        if conf:
            post_data['conf'] = conf
        r = requests.post(url, json=post_data)
        self.__session_id = r.json()['id']

    @property
    def session_id(self):
        return self.__session_id

    @property
    def state(self):
        url = f'{self.__livy_url}/sessions/{self.session_id}/state'
        r = requests.get(url)
        return r.json()['state']

    def query(self):
        url = f'{self.__livy_url}/sessions/{self.session_id}'
        r = requests.get(url)
        return r.json()

    def delete(self):
        url = f'{self.__livy_url}/sessions/{self.session_id}'
        r = requests.delete(url)
        return r.json()

    def run_code(self, code):
        url = f'{self.__livy_url}/sessions/{self.session_id}/statements'

        r = requests.post(url, json={'code': code})
        state = r.json()['state']
        while state in ('waiting', 'running'):
            time.sleep(0.1)
            r = requests.post(url, json={'code': code})
            state = r.json()['state']

        return state

    def wait_for(self, wait_for_states: tuple, interval: float = 0.1):
        if not isinstance(wait_for_states, (list, tuple)):
            wait_for_states = (wait_for_states,)
        while True:
            resp = requests.get(f'{self.__livy_url}', headers=self.HEADERS)
            rr = resp.json()
            if rr['state'] in wait_for_states:
                return rr
            time.sleep(interval)
