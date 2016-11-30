
from __future__ import print_function


#exports
__all__ = ("Ambari",)


import json
from urllib import quote

from HadoopUtil import HadoopUtil, CmdTuple


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


url_for = {
    "cluster": get_cluster_url,
    "host": get_host_url,
    "service": get_service_url,
    "component": get_component_url,
    "host_component": get_host_component_url,
}


class Ambari(HadoopUtil):
    rootpath = "/api/v1"
    commands = HadoopUtil.commands + [
        "",
        CmdTuple("list",                       "List all clusters"),
        CmdTuple("list hosts [cluster]",       "List all hosts"),
        CmdTuple("list host_components <cluster> [host]",
                                               "List all hosts together with components"),
        CmdTuple("list services <cluster>",    "List all services in cluster"),
        "",
        CmdTuple("show <cluster>",             "Show a cluster"),
        CmdTuple("show component <cluster> <service> <component>",
                                               "Show a component"),
        "",
        CmdTuple("add service <cluster> <service>",
                                               "Add a service"),
        CmdTuple("add component <cluster> <service> <component>",
                                               "Add a component"),
        CmdTuple("start service <cluster> <service>",
                                               "Start a service"),
        CmdTuple("stop service <cluster> <service>",
                                               "Stop a service"),
        CmdTuple("delete service <cluster> <service>",
                                               "Delete a service"),
        CmdTuple("start component <cluster> <host> <component>",
                                               "Start a component"),
        CmdTuple("stop component <cluster> <host> <component>",
                                               "Stop a component"),
        ]

    def __init__(self, host, user, passwd, debug=True):
        super(Ambari, self).__init__("http", host, 8080, user, passwd, debug)
        self.headers = {'X-Requested-By': 'ambari'}

    @property
    def banner(self):
        return "Ambari Shell"

    @property
    def weburl(self):
        return self.baseurl + self.rootpath

    def Put(self, url, **kwargs):
        return super(Ambari, self).Put(url, headers=self.headers, **kwargs)

    def Post(self, url, **kwargs):
        return super(Ambari, self).Post(url, headers=self.headers, **kwargs)

    def Delete(self, url, **kwargs):
        return super(Ambari, self).Delete(url, headers=self.headers, **kwargs)

# command handlers
    def do_list(self, data):
        params = data.split()
        if len(params) == 0:
            params = [None]
        func = {None: self.show_cluster,
                "hosts": self.list_hosts,
                "host_components": self.list_host_components,
                "services": self.list_services,
        }.get(params[0], None)

        if func:
            self.do_echo(func(*params[1:]))

    def do_show(self, data):
        params = data.split()
        if params[0] == "component":
            self.do_echo(self.show_component(*params[1:]))
        else:
            self.do_echo(self.show_cluster(*params[1:]))

    def do_add(self, data):
        params = data.split()

        func = {"service": self.add_service,
                "component": self.add_component,
        }.get(params[0], None)
        if func:
            self.do_echo(func(*params[1:]))

    def do_start(self, data):
        params = data.split()

        func = {"service": self.service_action,
                "component": self.component_action,
        }.get(params[0], None)
        if func:
            self.do_echo(func("start", *params[1:]))

    def do_stop(self, data):
        params = data.split()

        func = {"service": self.service_action,
                "component": self.component_action,
                }.get(params[0], None)
        if func:
            self.do_echo(func("stop", *params[1:]))

    def do_delete(self, data):
        params = data.split()

        if params[0] == "service":
            self.do_echo(self.delete_service(*params[1:]))

# utilities
    def show_cluster(self, cluster=None):
        url = self.weburl + url_for["cluster"](cluster)

        return self.Get(url)

    def show_component(self, cluster, service, component):
        url = self.weburl + url_for["component"](cluster, service, component)

        return self.Get(url)

    def list_hosts(self, cluster=None):
        """ get all hosts for a cluster """

        if cluster is None:
            url = self.weburl + "/hosts"
        else:
            url = self.weburl + url_for["host"](cluster)

        result = self.Get(url)

        format = "%-20s %-30s"
        lines = [format % ("CLUSTER","HOST"),
                 format % ("--------------------",
                           "------------------------------")]
        for item in result["items"]:
            host = item["Hosts"]
            lines.append(format % (host["cluster_name"], host["host_name"]))

        return "\n".join(lines)

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
        if not action in ('start', 'stop'):
            self.error("Invalid action")
            return ""

        if cluster is None:
            self.error("Cluster name missing")
            return ""

        if service is None:
            self.error("Service missing")
            return ""

        state = {"start": "STARTED", "stop": "INSTALLED"}[action]
        op = {"start": "Start", "stop": "Stop"}[action]
        service = service

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

    def delete_service(self, cluster, service):
        """ delete a service"""
        url = self.weburl + url_for["service"](cluster, service)

        return self.Delete(url)

    def component_action(self, action, cluster, hostname, component):
        """ start/stop a service"""
        if not action in ('start', 'stop'):
            self.error("Invalid action")
            return ""

        state = {"start": "STARTED", "stop": "INSTALLED"}[action]

        url = self.weburl + url_for["host_component"](cluster, hostname, component)
        data = json.dumps({"HostRoles": {"state": state}})

        return self.Put(url, data=data)


# ---- main ----
if __name__ == "__main__":
    print("Ambari")
