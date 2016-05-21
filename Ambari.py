

#exports
__all__ = ("Ambari",)

from urllib import quote
from HadoopUtil import HadoopUtil, Request, CmdTuple


class Ambari(HadoopUtil):
    rootpath = "/api/v1"
    commands = HadoopUtil.commands + [
        "",
        CmdTuple("list",                       "List all clusters"),
        CmdTuple("list hosts [cluster]",       "List all hosts"),
        CmdTuple("list services <cluster>",    "List all services in cluster"),
        CmdTuple("show <cluster>",             "Show a cluster"),
        ]

    def __init__(self, host, user, passwd):
        HadoopUtil.__init__(self, "http", host, 8080, user, passwd)


    @property
    def banner(self):
        return "Ambari Shell"

    @property
    def weburl(self):
        return self.baseurl + self.rootpath


    @staticmethod
    def Get(url, auth, params=None, curl=False):
        return Request("GET", url, auth=auth, params=params, curl=curl)

#    @staticmethod
#    def Delete(url, auth, curl=False):
#        return Request("DELETE", url, auth=auth, text=True,
#                       curl=curl, expected=(STATUS_NOCONTENT,))
#
#    @staticmethod
#    def Post(url, auth, data, curl=False):
#        return Request("POST", url, auth=auth,
#                       data=data,
#                       headers={"Content-Type" : "Application/json"},
#                       curl=curl)
#
#    @staticmethod
#    def Put(url, auth, data, curl=False):
#        return Request("PUT", url, auth=auth,
#                       data=data,
#                       headers={"Content-Type" : "Application/json"},
#                       curl=curl)


# command handers
    def do_list(self, data):
        params = data.split() + [None]
        if params[0] is None:
            self.do_echo(self.get_cluster())
        elif params[0] == "hosts":
            self.do_echo(self.get_hosts(params[1]))
        elif params[0] == "services":
            self.do_echo(self.get_services(params[1]))
        else:
            self.error("Invalid parameter")


    def do_show(self, data):
        if data:
            self.do_echo(self.get_cluster(data))
        else:
            self.error("Missing cluster name")


# utilities
    def get_cluster(self, cluster=None):
        url = self.weburl + "/clusters"
        if cluster:
            url += "/%s" % cluster

        return self.Get(url, self.auth, curl=self.curl)


    def get_hosts(self, cluster=None):
        """ get all hosts for a cluster """

        if cluster is None:
            url = self.weburl + quote("/hosts")
        else:
            url = self.weburl + \
                      quote("/clusters/{0}/hosts".format(cluster))

        result = self.Get(url, self.auth, curl=self.curl)

        for item in result["items"]:
             item["Components"] = self.get_components(
                 item["Hosts"]["cluster_name"], item["Hosts"]["host_name"])

        return result


    def get_components(self, cluster, hostname):
        """ get all components for a specific host in a specific cluster"""
        url = self.weburl+ \
              quote("/clusters/{0}/hosts/{1}/host_components".format(
                                                    cluster, hostname))
        return self.Get(url, self.auth, curl=self.curl)


    def get_services(self, cluster):
        """ get all services for a cluster """
        if cluster is None:
            self.error("Cluster name missing")
            return ""

        url = self.weburl + quote('/clusters/{0}/services'.format(cluster))

        return self.Get(url, self.auth, curl=self.curl)


# ---- main ----
if __name__ == "__main__":
    print "Ambari"
