"""
   provide some utilities for hadoop cluster administration

"""

# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2013-2015 Hewlett-Packard Development Company, L.P.
#
# @@@ END COPYRIGHT @@@


import requests
from urllib import quote

STATUS_OK = requests.codes.ok                         # 200
class Ambari(object):
    def __init__(self, host, port=8080):
        self.__rooturl = "http://{0}:{1}".format(host, port)
        self.auth = ('admin', 'admin')

    def get_clusters(self):
        """ get all clusters """

        url = self.__rooturl + '/api/v1/clusters'
        resp = requests.get(url, auth=self.auth)

        clusters = []
        if resp.status_code == STATUS_OK:
            for item in resp.json()['items']:
                clusters.append({'name': item['Clusters']['cluster_name'],
                                 'version': item['Clusters']['version']})

        return clusters

    def get_hosts(self, cluster=None):
        """ get all hosts for a cluster """
        
        if cluster is None:
            url = self.__rooturl + quote('/api/v1/hosts')
        else:
            url = self.__rooturl + \
                      quote('/api/v1/clusters/{0}/hosts'.format(cluster))
    
        resp = requests.get(url, auth=self.auth)
    
        hosts = []
        if resp.status_code == STATUS_OK:
            for item in resp.json()['items']:
                hostname = item['Hosts']['host_name']
                hosts.append({'hostname' : hostname,
                              'components': self.get_components(
                                                item['Hosts']['cluster_name'],
                                                 hostname)})

        return hosts

    def get_components(self, cluster, hostname):
        """ get all components for a specific host in a specific cluster"""
        url = self.__rooturl+ \
              quote('/api/v1/clusters/{0}/hosts/{1}/host_components'.format(
                                                    cluster, hostname))
        resp = requests.get(url, auth=self.auth)
    
        if resp.status_code == STATUS_OK:
            comps = [ item['HostRoles']['component_name']
                          for item in resp.json().get('items', []) ]
        else:
            comps = []
    
        return comps

    def get_services(self, cluster):
        """ get all services for a cluster """

        url = self.__rooturl + \
            quote('/api/v1/clusters/{0}/services'.format(cluster))
        resp = requests.get(url, auth=self.auth)

        if resp.status_code == STATUS_OK:
            services = [ item['ServiceInfo']['service_name']
                             for item in resp.json().get('items', []) ]
        else:
            services = []

        return services


if __name__ == '__main__':
    import json
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option('--host', default='c0049103.itcs.hp.com',
            help='hostname of ambari server')
            
    opts, _args = parser.parse_args()        
    
    ambari = Ambari(opts.host)
    
#   all_hosts = ambari.get_hosts()
#   print 'All hosts:'
#   print json.dumps(all_hosts, indent=4)

    clusters = ambari.get_clusters()
    print 'Clusters:'
    print json.dumps(clusters, indent=4)

    for cluster in clusters:
        cluster_name = cluster['name']
        print 'Services - ({0}):'.format(cluster_name)
        services = ambari.get_services(cluster_name)
        print json.dumps(services, indent=4)
        
        hosts = ambari.get_hosts(cluster_name)
        print 'Host in {0}:'.format(cluster_name)
        print json.dumps(hosts, indent=4)
        
