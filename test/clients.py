
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
        self.url = livy_url
        self.session_id = session_id

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.url}, {self.session_id}>'

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


class Catalog(object):
    def __init__(self, host, port=5000):
        self.__host = host
        self.__port = port

    @property
    def url(self):
        return f'http://{self.__host}:{self.__port}/api/catalog'

    def get_apps(self):
        resp = requests.get(f'{self.url}/apps')
        return resp.json()

    def get_dbs(self):
        resp = requests.get(f'{self.url}/datasets')
        return resp.json()
