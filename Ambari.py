
from __future__ import print_function


#exports
__all__ = ("Ambari",)


import json
from urllib import quote

from HadoopUtil import HadoopUtil, CmdTuple


def get_host_url(cluster=None):
    if cluster is None:
        return "/host"
    else:
        return quote("/clusters/%s/hosts" % cluster)


def get_service_url(cluster, service=None):
    if service is None:
        return quote("/clusters/%s/services" % cluster)
    else:
        return quote("/clusters/%s/services/%s" % (cluster, service))


def get_component_url(cluster, hostname, component=None):
    if component is None:
        return quote("/clusters/%s/hosts/%s/host_components" % (
            cluster, hostname))
    else:
        return quote("/clusters/%s/hosts/%s/host_components/%s" % (
            cluster, hostname, component))

url_for = {
    "host": get_host_url,
    "service": get_service_url,
    "component": get_component_url,
}


class Ambari(HadoopUtil):
    rootpath = "/api/v1"
    commands = HadoopUtil.commands + [
        "",
        CmdTuple("list",                       "List all clusters"),
        CmdTuple("list hosts [cluster]",       "List all hosts"),
        CmdTuple("list services <cluster>",    "List all services in cluster"),
        CmdTuple("list components <cluster> <host>",
                                               "List all components in for a host"),
        CmdTuple("show <cluster>",             "Show a cluster"),
        "",
        CmdTuple("start service <cluster> <service>",
                                               "Start a service"),
        CmdTuple("start component <cluster> <host> <component>",
                                               "Start a component"),
        CmdTuple("stop service <cluster> <service>",
                                               "Stop a service"),
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

# command handers
    def do_list(self, data):
        params = data.split() + [None, None]
        if params[0] is None:
            self.do_echo(self.get_cluster())
        elif params[0] == "hosts":
            self.do_echo(self.list_hosts(params[1]))
        elif params[0] == "services":
            self.do_echo(self.list_services(params[1]))
        elif params[0] == "components":
            self.do_echo(self.list_components(params[1], params[2]))
        else:
            self.error("Invalid parameter")

    def do_show(self, data):
        if data:
            self.do_echo(self.get_cluster(data))
        else:
            self.error("Missing cluster name")

    def do_start(self, data):
        params = data.split()

        if params[0] == "service":
            self.do_echo(self.service_action("start", *params[1:]))
        elif params[0] == "component":
            self.do_echo(self.component_action("start", *params[1:]))

    def do_stop(self, data):
        params = data.split()

        if params[0] == "service":
            self.do_echo(self.service_action("stop", *params[1:]))
        elif params[0] == "component":
            self.do_echo(self.component_action("stop", *params[1:]))

# utilities
    def get_cluster(self, cluster=None):
        url = self.weburl + "/clusters"
        if cluster:
            url += "/%s" % cluster

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

    def list_services(self, cluster):
        """ get all services for a cluster """
        if cluster is None:
            self.error("Cluster name missing")
            return ""

        url = self.weburl + url_for["service"](cluster)

        result = self.Get(url)

        format = "%-20s"
        lines = [format % ("SERVICE"),
                 format % ("--------------------",)]
        for item in result["items"]:
            lines.append(format % item["ServiceInfo"]["service_name"])

        return "\n".join(lines)

    def list_components(self, cluster, hostname):
        """ get all components for a specific host in a specific cluster"""
        if cluster is None:
            self.error("Cluster name missing")
            return ""

        if hostname is None:
            self.error("Host name missing")
            return ""

        url = self.weburl + url_for["component"](cluster, hostname)

        format = "%-30s %-20s"
        lines = [format % ("HOST","COMPONENT"),
                 format % ("------------------------------",
                           "--------------------")]
        result = self.Get(url)
        for item in result["items"]:
            host_role = item["HostRoles"]
            lines.append(format % (host_role["host_name"],
                                   host_role["component_name"]))

        return '\n'.join(lines)

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
        service = service.upper()

        url = self.weburl + url_for["service"](cluster, service)
        data = json.dumps({
            "RequestInfo": {
                "context": "%s %s via REST" % (op, service)},
            "Body": {
                "ServiceInfo": {
                    "state": state}}})

        return self.Put(url, data=data)

    def component_action(self, action, cluster=None, hostname=None, component=None):
        """ start/stop a service"""
        if not action in ('start', 'stop'):
            self.error("Invalid action")
            return ""

        if cluster is None:
            self.error("Cluster name missing")
            return ""

        if hostname is None:
            self.error("Host name missing")
            return ""

        if component is None:
            self.error("Component name missing")
            return ""

        state = {"start": "STARTED", "stop": "INSTALLED"}[action]
        component = component.upper()

        url = self.weburl + url_for["component"](cluster, hostname, component)
        data = json.dumps({"HostRoles": {"state": state}})

        return self.Put(url, data=data)


# ---- main ----
if __name__ == "__main__":
    print("Ambari")
