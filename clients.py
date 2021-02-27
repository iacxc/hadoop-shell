import json
import requests
import sys
import time
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

USER = "mtest"
PASS = "Cadence"
HEADERS = {'accept': 'application/json'}


def log_trace(msg):
    print(msg, file=sys.stderr)


def get_token(token_url, auth=(USER, PASS)):
    log_trace(f'Get {token_url}')
    resp = requests.get(token_url, auth=auth, verify=False)
    rr = resp.json()
    return rr['access_token']


class RestServer(object):
    def __init__(self, url, auth=(USER, PASS), using_sso=True, token_url=None):
        self.api_url = url
        self.auth = auth
        self.using_sso = using_sso
        self.token_url = token_url
        if self.using_sso:
            self.token = get_token(self.token_url, self.auth)

    def _get(self, path):
        url = f'{self.api_url}/{path}'
        log_trace(f'Get {url}')
        if self.using_sso:
            cookietext = {'hadoop-jwt': self.token}
            log_trace(f'Token: {self.token}')
            resp = requests.get(url, cookies=cookietext,
                                headers=HEADERS, verify=False)
        else:
            resp = requests.get(url, auth=self.auth,
                                headers=HEADERS)
        return resp

    def _post(self, path, data):
        url = f'{self.api_url}/{path}'
        log_trace(f'Post {url}')
        if self.using_sso:
            cookietext = {'hadoop-jwt': self.token}
            resp = requests.post(url, cookies=cookietext,
                                 json=data, headers=HEADERS, verify=False)
        else:
            resp = requests.post(url, auth=self.auth,
                                 json=data, headers=HEADERS)
        return resp

    def _delete(self, path):
        url = f'{self.api_url}/{path}'
        log_trace(f'Delete {url}')
        if self.using_sso:
            cookietext = {'hadoop-jwt': self.token}
            resp = requests.delete(url, cookies=cookietext,
                                   headers=HEADERS, verify=False)
        else:
            resp = requests.delete(url, auth=self.auth,
                                   headers=HEADERS)
        return resp



class ClouderaManager(RestServer):
    VER = "v41"
    def __init__(self, host, port=7180, auth=('admin', 'admin')):
        super().__init__(f'http://{host}:{port}/api/{self.VER}',
                         auth=auth, using_sso=False)

    @property
    def name(self):
        resp = self._get('clusters')
        rr = resp.json()
        return rr['items'][0]['name']

    @property
    def knox_host(self):
        resp = self._get(f'clusters/{self.name}/services/knox/roles')
        rr = resp.json()
        return rr['items'][0]['hostRef']['hostname']

    @property
    def ranger_host(self):
        resp = self._get(f'clusters/{self.name}/services/ranger/roles')
        rr = resp.json()
        return rr['items'][0]['hostRef']['hostname']

    @property
    def atlas_host(self):
        resp = self._get(f'clusters/{self.name}/services/atlas/roles')
        rr = resp.json()
        return rr['items'][0]['hostRef']['hostname']

    def get_token_url(self):
        return f'https://{self.knox_host}:8443/gateway/cdp-proxy-api/knoxtoken/api/v1/token'

    def get_atlas_url(self):
        return f'https://{self.atlas_host}:31000/api/atlas'

    def get_atlas_knox_url(self):
        return f'https://{self.knox_host}:8443/gateway/cdp-proxy/atlas/api/atlas'

    def get_ranger_knox_url(self):
        return f'https://{self.knox_host}:8443/gateway/cdp-proxy/ranger'


class Ranger(RestServer):
    def __init__(self, url, auth=(USER, PASS), using_sso=True, token_url=None):
        super().__init__(url, auth=auth,
                         using_sso=using_sso, token_url=token_url)

    def get_user_by_name(self, user):
        resp = self._get(f'service/xusers/users/userName/{user}')
        rr = resp.json()
        return rr


class Atlas(RestServer):
    VER = "v2"
    def __init__(self, url, auth=(USER, PASS), using_sso=True, token_url=None):
        super().__init__(f'{url}/{self.VER}', auth=auth,
                         using_sso=using_sso, token_url=token_url)

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.api_url}>'

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


def add_path(atlas, db_id):
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
            log_trace(f'path for {db_id} not found')
            return None

        directory = [{'guid': data['entities'][0]['guid']}]
        r = altas.add_entity('CDNSDataSet',
                            directory=directory,
                            qualifiedName=attrs['qualifiedName'],
                            name=attrs['name'],
                            db_id=db_id,
                            guid=entity['guid'],
                            )
        log_trace(r)
    except requests.exceptions.ReadTimeout:
        print(f'Timeout for {db_id}')


if __name__ == '__main__':
    if len(sys.argv) > 1:
        cm_host = sys.argv[1]
    else:
        cm_host = 'vlsj-mozart-poc1.cadence.com'
    cm = ClouderaManager(cm_host)
    print(f'Cluster: {cm.name}')
    print(f'    Knox: {cm.knox_host}')
    print(f'    Atlas: {cm.atlas_host}')
    print(f'    Ranger: {cm.ranger_host}')
