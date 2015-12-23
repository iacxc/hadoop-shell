

#exports
__all__ = ('Ambari',)

from HadoopUtil import HadoopUtil, Request, CmdTuple

class Ambari(HadoopUtil):
    rootpath = "/api/v1"
    commands = HadoopUtil.commands + [
        "",
        CmdTuple("list",                         "List all cluster"),
        CmdTuple("show <cluster>",               "Show a cluster"),
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
#       params = data.split() + [None]
        self.do_echo(self.get_cluster())
#       if params[0] == 'cluster':
#           self.do_echo(self.get_repository(params[1]))
#       elif params[0] == 'policy':
#           self.do_echo(self.get_policy(params[1]))
#       else:
#           self.error("Invalid parameter '%s'" % data)


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

#
# ---- main ----
if __name__ == "__main__":
    print "Ambari"
