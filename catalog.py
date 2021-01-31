import pandas as pd
import requests


class Catalog(object):
    HEADERS = {'accept': 'application/json'}
    def __init__(self, host, port=5000):
        self.__host = host
        self.__port = port

    @property
    def url(self):
        return f'http://{self.__host}:{self.__port}/api/catalog'

    def get_apps(self):
        url = f'{self.url}/app'
        resp = requests.get(url, headers=self.HEADERS)
        return resp.json()

    def get_dbs(self):
        url = f'{self.url}/dataset'
        resp = requests.get(url, headers=self.HEADERS)
        rows = []
        for db in resp.json():
            apps = [app['app_name'] for app in db['apps']]
            rows.append((db['db_id'], db['design'], db['stage'],
                         db['run'], db['guid'], db['create_time'], apps))
        df = pd.DataFrame(rows,
                          columns=["ID", "Name", "Stage", "Run", "GUID",
                                   "Create Time", "Apps"]
                          )
        return df
