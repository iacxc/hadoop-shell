
import requests
import time


class LivySession(object):
    HEADERS = {'Content-Type': 'application/json'}
    @classmethod
    def find_session(cls, host='localhost', port=8998, name=None):
        if name is None:
            raise ValueError('name can not be empty')
        livy_url = f'http://{host}:{port}'
        url = f'{livy_url}/sessions'
        resp = requests.get(url, headers=cls.HEADERS)
        rr = resp.json()
        for s in rr.get('sessions', []):
            if s.get('name') == name:
                return cls(livy_url, s['id'])
        return None

    @classmethod
    def create_session(cls, host='localhost', port=8998, name=None, **kwargs):
        if name is None:
            raise ValueError('name can not be empty')

        livy_url = f'http://{host}:{port}'
        url = f'{livy_url}/sessions'
        data = kwargs
        data.update(name=name, kind='pyspark')

        resp = requests.post(url, json=data, headers=cls.HEADERS)
        rr = resp.json()

        livy = cls(livy_url, rr['id'])
        while livy.state in ('not_started', 'starting'):
            time.sleep(1)
        return livy

    @classmethod
    def get_session(cls, host='localhost', port=8998, name=None, **kwargs):
        return cls.find_session(host, port, name) or \
                   cls.create_session(host, port, name, **kwargs)

    def __init__(self, livy_url, session_id):
        self.__url = livy_url
        self.__id = session_id

    def __repr__(self):
        return f'<LivySession: {self.__url}, {self.__id}>'

    @property
    def url(self):
        return self.__url

    @property
    def session_id(self):
        return self.__id

    @property
    def state(self):
        url = f'{self.url}/sessions/{self.session_id}/state'
        resp = requests.get(url)
        return resp.json()['state']

    def query(self):
        url = f'{self.url}/sessions/{self.session_id}'
        resp = requests.get(url)
        return resp.json()

    def delete(self):
        url = f'{self.url}/sessions/{self.session_id}'
        resp = requests.delete(url)
        return resp.json()

    def query_stmt(self, stmt_id):
        url = f'{self.url}/sessions/{self.session_id}/statements/{stmt_id}'
        resp = requests.get(url)
        return resp.json()

    def run_code(self, code, batch=False, interval: float = 1):
        url = f'{self.url}/sessions/{self.session_id}/statements'

        resp = requests.post(url, json={'code': code})
        rr = resp.json()
       
        if batch:
            return rr
        stmt_id = rr['id']
        state = rr['state']
        while state in ('waiting', 'running'):
            time.sleep(interval)
            rr = self.query_stmt(stmt_id)
            state = rr['state']

        return rr

