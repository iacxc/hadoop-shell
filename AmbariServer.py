
from __future__ import print_function


#exports
__all__ = ("AmbariServer", )

import json
import sys

if sys.version_info.major >= 3:
    from urllib.parse import quote
else:
    from urllib import quote

from RestServer import RestServer


def get_cluster_url(cluster=None):
    if cluster is None:
        return "/clusters"
    else:
        return quote("/clusters/%s" % cluster)


def get_host_url(cluster=None):
    if cluster is None:
        return "/host"
    else:
        return quote("/clusters/%s/hosts" % cluster)


def get_service_url(cluster, service=None):
    if service is None:
        return quote("/clusters/%s/services" % cluster)
    else:
        return quote("/clusters/%s/services/%s" % (cluster, service.upper()))


def get_host_component_url(cluster, hostname, component=None):
    if component is None:
        return quote("/clusters/%s/hosts/%s/host_components" % (
            cluster, hostname))
    else:
        return quote("/clusters/%s/hosts/%s/host_components/%s" % (
            cluster, hostname, component.upper()))


def get_component_url(cluster, service, component):
    return quote("/clusters/%s/services/%s/components/%s" % (
                     cluster, service.upper(), component.upper()))


def get_alert_url(cluster, alert_id=None):
    if alert_id is None:
        return quote("/clusters/%s/alert_definitions" % cluster)
    else:
        return quote("/clusters/%s/alert_definitions/%d" % (cluster, alert_id))


url_for = {
    "cluster": get_cluster_url,
    "host": get_host_url,
    "service": get_service_url,
    "component": get_component_url,
    "host_component": get_host_component_url,
    "alert": get_alert_url,
}


class AmbariServer(RestServer):
    rootpath = "/api/v1"

    def __init__(self, opts):
        super(AmbariServer, self).__init__(opts)

        self.headers = {'X-Requested-By': 'ambari'}

    @property
    def weburl(self):
        return self.baseurl + self.rootpath

    def Get(self, url):
        return super(AmbariServer, self).Get(url, auth=self.auth)

    def Put(self, url, data=None):
        return super(AmbariServer, self).Put(url,
                                       auth=self.auth,
                                       headers=self.headers,
                                       data=data)

    def Post(self, url, data=None):
        return super(AmbariServer, self).Post(url,
                                        auth=self.auth,
                                        headers=self.headers,
                                        data=data)

    def Delete(self, url):
        return super(AmbariServer, self).Delete(url,
                                          auth=self.auth,
                                          headers=self.headers)

# command handlers
    def do_list(self, data):
        params = data.split()
        if len(params) == 0:
            params = [None]
        func = {None: self.show_cluster,
                "alerts": self.list_alerts,
                "hosts": self.list_hosts,
                "host_components": self.list_host_components,
                "services": self.list_services,
        }.get(params[0], None)

        if func:
            return func(*params[1:])

    def do_show(self, data):
        params = data.split()
        if params[0] == "component":
            return self.show_component(*params[1:])
        else:
            return self.show_cluster(*params[1:])

    def do_add(self, data):
        params = data.split()

        func = {"service": self.add_service,
                "component": self.add_component,
                "host_component": self.add_host_component,
        }.get(params[0], None)
        if func:
            return func(*params[1:])

    def do_install(self, data):
        params = data.split()

        func = {"service": self.service_action,
                "component": self.component_action,
        }.get(params[0], None)
        if func:
            return func("install", *params[1:])

    def do_start(self, data):
        params = data.split()

        func = {"service": self.service_action,
                "component": self.component_action,
        }.get(params[0], None)
        if func:
            return func("start", *params[1:])

    def do_stop(self, data):
        params = data.split()

        func = {"service": self.service_action,
                "component": self.component_action,
                }.get(params[0], None)
        if func:
            return func("stop", *params[1:])

    def do_delete(self, data):
        params = data.split()

        if params[0] == "service":
            return self.delete_service(*params[1:])

# utilities
    def show_cluster(self, cluster=None):
        url = self.weburl + url_for["cluster"](cluster)

        return self.Get(url)

    def show_component(self, cluster, service, component):
        url = self.weburl + url_for["component"](cluster, service, component)

        return self.Get(url)

    def list_alerts(self, cluster):
        """ get all alerts for a cluster """
        url = self.weburl + url_for["alert"](cluster)
        result = self.Get(url)

        lines = []
        for item in result["items"]:
            alert = item["AlertDefinition"]
            lines.append((alert["id"], alert["name"], alert["label"]))

        return [("ALERTID", "NAME", "LABEL"), lines]

    def list_hosts(self, cluster=None):
        """ get all hosts for a cluster """

        if cluster is None:
            url = self.weburl + "/hosts"
        else:
            url = self.weburl + url_for["host"](cluster)

        result = self.Get(url)

        lines = []
        for item in result["items"]:
            host = item["Hosts"]
            lines.append((host["cluster_name"], host["host_name"]))

        return [("CLUSTER", "HOST"), lines]

    def list_host_components(self, cluster, hostname=None):
        """ get all components for a specific host in a specific cluster"""
        if hostname is None:
            host_url = self.weburl + url_for["host"](cluster)
            hosts = [item["Hosts"]["host_name"]
                     for item in self.Get(host_url)["items"]]
        else:
            hosts = [hostname]

        format = "%-30s %-30s"
        lines = [format % ("HOST","COMPONENT"),
                 format % ("------------------------------",
                           "------------------------------")]
        for host in hosts:
            comp_url = self.weburl + url_for["host_component"](cluster, host)

            components = [item["HostRoles"]["component_name"]
                          for item in self.Get(comp_url)["items"]]
            lines.append(format % (host, ",".join(components)))

        return '\n'.join(lines)

    def list_services(self, cluster, service=None):
        """ get all services for a cluster """
        if service is None:
            service_url = self.weburl + url_for["service"](cluster)
            services = [item["ServiceInfo"]["service_name"]
                        for item in self.Get(service_url)["items"]]
        else:
            services = [service]

        format = "%-20s %-30s"
        lines = [format % ("SERVICE", "COMPONENT"),
                 format % ("--------------------",
                           "------------------------------", )]
        for service in services:
            url = self.weburl + url_for["service"](cluster, service)
            components = [item["ServiceComponentInfo"]["component_name"]
                          for item in self.Get(url)["components"]]
            lines.append(format % (service, ",".join(components)))

        return "\n".join(lines)

    def service_action(self, action, cluster=None, service=None):
        """ start/stop a service"""
        if not action in ("install", "start", "stop"):
            self.error("Invalid action")
            return ""

        if cluster is None:
            self.error("Cluster name missing")
            return ""

        if service is None:
            self.error("Service missing")
            return ""

        state = {"install": "INSTALLED",
                 "start": "STARTED",
                 "stop": "INSTALLED"}[action]
        op = {"install": "Install",
              "start": "Start",
              "stop": "Stop"}[action]

        url = self.weburl + url_for["service"](cluster, service)
        data = json.dumps({
            "RequestInfo": {
                "context": "%s %s via REST" % (op, service)},
            "Body": {
                "ServiceInfo": {
                    "state": state}}})

        return self.Put(url, data=data)

    def add_service(self, cluster, service):
        """ add a service"""
        url = self.weburl + url_for["service"](cluster)
        data = json.dumps({
            "ServiceInfo": {
                "service_name": service.upper()
            }
        })
        return self.Post(url, data=data)

    def add_component(self, cluster, service, component):
        """ add a component """
        url = self.weburl + url_for["component"](cluster, service, component)

        return self.Post(url)

    def add_host_component(self, cluster, host, component):
        """ add a component to a host """
        url = self.weburl + url_for["host_component"](cluster, host, component)

        return self.Post(url)

    def delete_service(self, cluster, service):
        """ delete a service"""
        url = self.weburl + url_for["service"](cluster, service)

        return self.Delete(url)

    def component_action(self, action, cluster, hostname, component):
        """ start/stop a service"""
        if not action in ('install', 'start', 'stop'):
            self.error("Invalid action")
            return ""

        state = {"start": "STARTED", 
                 "stop": "INSTALLED",
                 "install": "INSTALLED"}[action]

        url = self.weburl + \
                  url_for["host_component"](cluster, hostname, component)
        if action == "install":
            data = json.dumps({
                "RequestInfo": {
                    "context": "Install %s" % component
                },
                "Body": {
                    "HostRoles": {
                        "state": state
                    }
                }
            })
        else:
            data = json.dumps({"HostRoles": {"state": state}})

        return self.Put(url, data=data)


# ---- main ----
if __name__ == "__main__":
    print("Ambari")
