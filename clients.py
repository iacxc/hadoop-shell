import json
import requests
import time
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

USER = "mtest"
PASS = "Cadence"

HEADERS = {'accept': 'application/json'}

KNOX = "https://vlsj-mozart-poc2.cadence.com:8443/gateway"
TOKEN_URL = f"{KNOX}/cdp-proxy-api/knoxtoken/api/v1/token"

ATLAS_URL = "http://vlsj-mozart-poc5.cadence.com:31000/api/atlas"
ATLAS_URL = "http://shmlbdlnx4.cadence.com:31000/api/atlas"
KNOX_ATLAS_URL = f"{KNOX}/cdp-proxy/atlas/api/atlas"

def get_token(token_url, auth=(USER, PASS)):
    resp = requests.get(token_url, auth=auth, verify=False)
    rr = resp.json()
    print(rr['access_token'])
    return rr['access_token']

class Atlas(object):
    VER = "v2"
    def __init__(self, url, using_sso=True):
        self.api_url = f'{url}/{self.VER}'
        self.auth = (USER, PASS)
        self.using_sso = using_sso
        if using_sso:
            self.token = get_token(TOKEN_URL, (USER, PASS))

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.api_url}>'

    def _get(self, path):
        if self.using_sso:
            cookietext = {'hadoop-jwt': self.token}
            resp = requests.get(f'{self.api_url}/{path}', cookies=cookietext,
                                timeout=0.9, headers=HEADERS, verify=False)
        else:
            resp = requests.get(f'{self.api_url}/{path}', auth=self.auth,
                                timeout=0.9, headers=HEADERS)
        return resp

    def _post(self, path, data):
        if self.using_sso:
            cookietext = {'hadoop-jwt': self.token}
            resp = requests.post(f'{self.api_url}/{path}', cookies=cookietext,
                                 timeout=0.9, json=data, headers=HEADERS, 
                                 verify=False)
        else:
            resp = requests.post(f'{self.api_url}/{path}', auth=self.auth,
                                 timeout=0.9, json=data, headers=HEADERS)
        return resp

    def _delete(self, path):
        if self.using_sso:
            cookietext = {'hadoop-jwt': self.token}
            resp = requests.delete(f'{self.api_url}/{path}', cookies=cookietext,
                                   headers=HEADERS, verify=False)
        else:
            resp = requests.delete(f'{self.api_url}/{path}', auth=self.auth,
                                   headers=HEADERS)
        return resp

    def get_type_by_name(self, type_name):
        resp = self._get(f'types/typedef/name/{type_name}')
        return resp.status_code, resp.json()

    def get_types(self):
        resp = self._get(f'types/typedefs')
        return resp.status_code, resp.json()

    def get_type_attrs(self, type_name):
        _, data = self.get_type_by_name(type_name)
        return [attr['name'] for attr in data['attributeDefs']]

    def delete_entity(self, guid):
        resp = self._delete(f'entity/guid/{guid}')
        return resp.status_code, resp.json()

    def add_entity(self, entity_type, **kwargs):
        data = {'entity': {'typeName': entity_type,
                           'attributes': kwargs}
               }
        resp = self._post('entity', data)
        return resp.status_code, resp.json()

    def get_entity_by_guid(self, guid):
        resp = self._get(f'entity/guid/{guid}')
        return resp.status_code, resp.json()

    def get_entities(self, entity_type, *, attrs=None, **kwargs):
        conditions = [{'attributeName': key,
                       'operator': '=',
                       'attributeValue': val} for key, val in kwargs.items()]
        if attrs is None:
            attrs = self.get_type_attrs(entity_type)
        data = {'typeName': entity_type,
                'excludeDeletedEntities': 'true',
                'attributes': attrs
               }
        if conditions:
            if len(conditions) == 1:
                data['entityFilters'] = conditions[0]
            else:
                data['entityFilters'] = {'condition': 'AND',
                                     'criterion': conditions}

        resp = self._post(f'search/basic', data)
        return resp.status_code, resp.json()


def add_path(altas, db_id):
    return atlas.add_entity('hdfs_path',
                            path=f'/data/warehouse/cui.db/{db_id}',
                            qualifiedName=f'{db_id}:/data/warehouse/cui.db/{db_id}',
                            name=f'{db_id}:/data/warehouse/cui.db/{db_id}')


def update_fs(altas, entity, fileSystem):
    attrs = entity['attributes']
    return altas.add_entity('CDNSDataSet',
                            fileSystem=fileSystem,
                            qualifiedName=attrs['qualifiedName'],
                            name=attrs['name'],
                            db_id=attrs['db_id'],
                            guid=entity['guid'],
                            )


def update_dir(altas, entity):
    attrs = entity['attributes']
    db_id = attrs['db_id']
    try:
        _, data = atlas.get_entities('hdfs_path',
                                      qualifiedName=f'{db_id}:/data/warehouse/cui.db/{db_id}'
                                     )
        if 'entities' not in data:
            print(f'path for {db_id} not found')
            return None

        directory = [{'guid': data['entities'][0]['guid']}]
        r = altas.add_entity('CDNSDataSet',
                            directory=directory,
                            qualifiedName=attrs['qualifiedName'],
                            name=attrs['name'],
                            db_id=db_id,
                            guid=entity['guid'],
                            )
        print(r)
    except requests.exceptions.ReadTimeout:
        print(f'Timeout for {db_id}')


if __name__ == '__main__':
    atlas = Atlas(KNOX_ATLAS_URL)
#    atlas = Atlas(ATLAS_URL, False)

    _, data = atlas.get_entities('CDNSDataSet', attrs=['db_id', 'directory'])
    print(json.dumps(data, indent=4))

