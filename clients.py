import json
import requests
import sys
import time
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

USER, PASS = "mtest", "Cadence"
HEADERS = {'accept': 'application/json'}


def log_trace(msg):
    print(msg, file=sys.stderr)


def get_token(auth, token_url: str = None):
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
            self.token = get_token(self.auth, self.token_url)

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
        return f'http://{self.atlas_host}:31000/api/atlas'

    def get_atlas_knox_url(self):
        return f'https://{self.knox_host}:8443/gateway/cdp-proxy/atlas/api/atlas'

    def get_ranger_url(self):
        return f'http://{self.ranger_host}:6080'

    def get_ranger_knox_url(self):
        return f'https://{self.knox_host}:8443/gateway/cdp-proxy/ranger'

    def get_livy_knox_url(self):
        return f'https://{self.knox_host}:8443/gateway/cdp-proxy-api/livy'

    def get_livy_jwt_url(self):
        return f'https://{self.knox_host}:8443/gateway/jwt/livy'


class Ranger(RestServer):
    VER = "v2"
    def __init__(self, url, auth=(USER, PASS), using_sso=True, token_url=None):
        super().__init__(url, auth=auth,
                         using_sso=using_sso, token_url=token_url)

    def get_user_by_name(self, user):
        resp = self._get(f'service/xusers/users/userName/{user}')
        rr = resp.json()
        return rr

    def get_groups_by_uid(self, uid):
        resp = self._get(f'service/xusers/{uid}/groups')
        rr = resp.json()
        return rr

    def get_policies(self, service_name=None, policy_name=None):
        if service_name is None:
            resp = self._get(f'service/public/{self.VER}/api/policy')
        else:
            if policy_name is None:
                resp = self._get(f'service/public/{self.VER}/api/service/{service_name}/policy')
            else:
                resp = self._get(f'service/public/{self.VER}/api/service/{service_name}/policy/{policy_name}')
        rr = resp.json()
        return rr


class Atlas(RestServer):
    VER = "v2"
    def __init__(self, url, auth=(USER, PASS), using_sso=True, token_url=None):
        super().__init__(f'{url}/{self.VER}', auth=auth,
                         using_sso=using_sso, token_url=token_url)

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.api_url}>'

    def setup_types(self, cfg_fname):
        atlas_types = json.load(open(cfg_fname))
        resp = self._post('types/typedefs', atlas_types)
        return resp.status_code, resp.json()

    def get_types(self):
        resp = self._get('types/typedefs')
        return resp.status_code, resp.json()

    def get_type_by_name(self, type_name):
        resp = self._get(f'types/typedef/name/{type_name}')
        return resp.status_code, resp.json()

    def get_classification_by_name(self, class_name):
        resp = self._get(f'types/classificationdef/name/{class_name}')
        return resp.status_code, resp.json()

    def get_type_attrs(self, type_name):
        _, data = self.get_type_by_name(type_name)
        return [attr['name'] for attr in data['attributeDefs']]

    def delete_entity(self, guid):
        resp = self._delete(f'entity/guid/{guid}')
        return resp.status_code, resp.json()

    def add_entity(self, entity_type, classifications=None, **kwargs):
        data = {'entity': {'typeName': entity_type,
                           'attributes': kwargs}
               }
        if classifications:
            data['entity']['classifications'] = classifications

        resp = self._post('entity', data)
        return resp.status_code, resp.json()

    def get_entity_by_guid(self, guid):
        resp = self._get(f'entity/guid/{guid}')
        return resp.status_code, resp.json()

    def get_entity_by_qualifiedName(self, entity_type, q_name):
        resp = self._get(f'entity/uniqueAttribute/type/{entity_type}?attr:qualifiedName={q_name}')
        return resp.status_code, resp.json()

    def find_entity(self, entity_type, q_name):
        _, data = self.get_entity_by_qualifiedName(entity_type, q_name)
        return data.get('entity')

    def get_entities(self, entity_type, classifications=None, *, attrs=None, **kwargs):
        conditions = [{'attributeName': key,
                       'operator': '=',
                       'attributeValue': val} for key, val in kwargs.items()]
        if attrs is None:
            attrs = self.get_type_attrs(entity_type)
        data = {'typeName': entity_type,
                'excludeDeletedEntities': 'true',
                'attributes': attrs,
                'classifications': classifications,
               }
        if conditions:
            if len(conditions) == 1:
                data['entityFilters'] = conditions[0]
            else:
                data['entityFilters'] = {'condition': 'AND',
                                         'criterion': conditions}
        print(json.dumps(data, indent=4))
        resp = self._post(f'search/basic', data)
        return resp.status_code, resp.json()

    def add_path(self, path):
        return self.add_entity('hdfs_path',
                               classifications=[{'typeName': 'dsg'}],
                               path=path,
                               qualifiedName=f'{path}@cm',
                               name=f'{path}@cm',
                               isFile='false')

    def add_app(self, prefix, identifier, name):
        return self.add_entity('CDNSApp',
                               qualifiedName=f'{prefix}:{identifier}',
                               name=f'{prefix}:{identifier}',
                               identifier=identifier,
                               prefix=prefix,
                               appName=name,
                               owner='mtest',
                               accessGroups='mozart_ml_dsg, cadence1, cadence_2'
                               )

    def add_design(self, domain, design_name):
        return self.add_entity('CDNSDesign',
                               qualifiedName=f'{domain}:{design_name}',
                               name=f'{domain}:{design_name}',
                               owner='mozart',
                               description='design_name')

    def add_run(self, domain, design_name, run_name):
        attrs = {'qualifiedName': f'{domain}:{design_name}:{run_name}',
                 'name': f'{domain}:{design_name}:{run_name}',
                 'owner': 'mozart',
                 'description': 'run'}

        design = self.find_entity('CDNSDesign', f'{domain}:{design_name}')
        if design:
            attrs['design'] = {'guid': design['guid']}
        return self.add_entity('CDNSRun', **attrs)

    def add_stage(self, domain, design_name, run_name, stage_name):
        attrs = {'qualifiedName': f'{domain}:{design_name}:{run_name}:{stage_name}',
                 'name': f'{domain}:{design_name}:{run_name}:{stage_name}',
                 'owner': 'mozart',
                 'description': 'stage'}

        run = self.find_entity('CDNSRun', f'{domain}:{design_name}:{run_name}')
        if run:
            attrs['run'] = {'guid': run['guid']}

        design = self.find_entity('CDNSDesign', f'{domain}:{design_name}')
        if design:
            attrs['design'] = {'guid': design['guid']}

        return self.add_entity('CDNSStage', **attrs)

    def add_dataset(self, domain, db_id, design_name, run_name, stage_name,
                    path, app_names):
        attrs = {'qualifiedName': ':'.join([design_name, run_name,
                                            stage_name, db_id]),
                 'name': ':'.join([design_name, run_name, stage_name, db_id]),
                 'displayText': ':'.join([design_name, run_name, stage_name, db_id]),
                 'db_id': db_id,
                 'fileSystem': 'hdfs://shmlbdlnx5.cadence.com:8020',
                 'owner': 'mozart',
                 'description': 'innovus',
                 'accessGroups': 'mozart_ml_dsg, cadence1, cadence_2',
                 'format': 'parquet',
                 'ingestStatus': 'active',
                 'stageName': stage_name,
                 'runId': run_name,
                 'designName': design_name,
                 }

        directory = self.find_entity('hdfs_path', f'{path}@cm')
        attrs['directory'] = [{'guid': directory['guid']}]

        design = self.find_entity('CDNSDesign', f'{domain}:{design_name}')
        if design:
            attrs['design'] = {'guid': design['guid']}

        run = self.find_entity('CDNSRun', f'{domain}:{design_name}:{run_name}')
        if run:
            attrs['run'] = {'guid': run['guid']}

        stage = self.find_entity('CDNSStage',
                                 f'{domain}:{design_name}:{run_name}:{stage_name}')
        if stage:
            attrs['stage'] = {'guid': stage['guid']}

        _, data = self.get_entities('CDNSApp')
        all_apps = data.get('entities', [])
        apps = []
        for app_name in app_names.split(','):
            apps.extend([{'guid': app['guid']} for app in all_apps
                                               if app['attributes']['appName'] == app_name])
        attrs['apps'] = apps
        attrs['classifications'] = [
                {'typeName': 'access',
                 'attributes': {'user': ['#mtest#'],
                                'groups': ['#mlsh#', '#mozart_ml_dsg#','#cadence_2#']}}]

        return self.add_entity('CDNSDataSet', **attrs)

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
    print(f'    TokenUrl: {cm.get_token_url()}')
    print(f'    AtlasUrl: {cm.get_atlas_url()}')
    print(f'    AtlasKnoxUrl: {cm.get_atlas_knox_url()}')
    print(f'    RangerKnoxUrl: {cm.get_ranger_knox_url()}')
    print(f'    LivyKnoxUrl: {cm.get_livy_knox_url()}')
    print(f'    LivyJwtUrl: {cm.get_livy_jwt_url()}')

