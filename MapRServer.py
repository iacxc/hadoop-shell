
from __future__ import print_function


#exports
__all__ = ("MapRServer", )

import json
from urllib import quote
from RestServer import RestServer


class MapRServer(RestServer):
    rootpath = "/rest"

    def __init__(self, opts):
        super(MapRServer, self).__init__(opts)

    @property
    def weburl(self):
        return self.baseurl + self.rootpath

    def Get(self, url):
        return super(MapRServer, self).Get(url, auth=self.auth)

    def Put(self, url, data=None):
        return super(MapRServer, self).Put(url,
                                       auth=self.auth,
                                       headers=self.headers,
                                       data=data)

    def Post(self, url, data=None):
        return super(MapRServer, self).Post(url,
                                        auth=self.auth,
                                        headers=self.headers,
                                        data=data)

    def Delete(self, url):
        return super(MapRServer, self).Delete(url,
                                          auth=self.auth,
                                          headers=self.headers)

# command handlers
    def do_list(self, data):
        params = data.split()
        if len(params) == 0:
            params = [None]
        func = {"hosts": self.list_hosts,
                "services": self.list_services,
                "volumes": self.list_volumes,
                "disks": self.list_disks,
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

    def list_hosts(self):
        """ get all hosts for a cluster """

        url = self.weburl + "/node/list"

        result = self.Get(url)

        lines = []
        for item in result["data"]:
            lines.append((item["hostname"], item["ip"], item["configuredservice"]))

        return [("HOST", "IP", "SERVICE"), lines]

    def list_services(self, host=None):
        """ get all services """

        url = self.weburl + "/service/list"
        if host is not None:
            url += "?node=%s" % host

        result = self.Get(url)

        lines = []
        state_str = {0: "Not configured",
                     1: "Configured",
                     2: "Running",
                     3: "Stopped",
                     4: "Failed",
                     5: "Stand by"}
        for item in result["data"]:
            lines.append((item["name"],
                          item["displayname"],
                          state_str[item["state"]],
                          item["logpath"]))

        return [("NAME", "DISPLAYNAME", "STATE", "LOGPATH"), lines]

    def list_volumes(self):
        """ get all volumes """

        url = self.weburl + "/volume/list"
        result = self.Get(url)

        lines = []
        for item in result["data"]:
            lines.append((item["volumename"], item["mountdir"], item["rackpath"]))

        return [("NAME", "MOUNTDIR", "RACKPATH"), lines]

    def list_disks(self, host):
        """ get all disks for a host """

        url = self.weburl + "/disk/list?host=%s" % host
        result = self.Get(url)

        lines = []
        for item in result["data"]:
            lines.append((item["diskname"],
                          item["totalspace"],
                          item.get("fstype", ""),
                          item["status"]))

        return [("NAME", "CAPACITY", "FILESYSTEM", "STATUS"), lines]

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
    print("MapR")
