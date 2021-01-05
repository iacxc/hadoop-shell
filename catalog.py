
import requests


class Catalog(object):
    def __init__(self, host, port=5000):
        self.__host = host
        self.__port = port

    @property
    def url():
        return f'http://{self.__host}:{self.__port}/api/catalog'

    def get_apps():
        resp = requests.get(f'{self.url}/apps')
        return resp.json()

    def get_dbs():
        resp = requests.get(f'{self.url}/datasets')
        return resp.json()
