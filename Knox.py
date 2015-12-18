

#exports
__all__ = ("Knox",)


from HadoopUtil import HadoopUtil, Request, CmdTuple, \
                       gen_fileinfo
from WebHdfs import WebHdfs
from WebHCat import WebHCat
#from ResourceManager import ResourceManager


class Knox(HadoopUtil):
#                  "cluster_info", "cluster_metrics", "get_node",

    rootpath = "/api/v1"
    commands = HadoopUtil.commands + [
        "",
        CmdTuple("version",                 "Show the version"),
        CmdTuple("ls <file|dir>",           "List files/directoies"),
        CmdTuple("cat <file>",              "Show a text file"),
        CmdTuple("cluster [cluster_name]",  "Set or show the cluster name"),
        CmdTuple("show topo [topo_name]",   "Show the topologies or a topology"),
        CmdTuple("show databases",          "List all databases"),
        CmdTuple("show tables <db>",        "List all tables"),
        CmdTuple("desc database <db>",      "Show detals of current database"),
        CmdTuple("desc table <db> <table>", "Show detals of a table"),
    ]
    def __init__(self, host, user, password, cluster="default"):
        HadoopUtil.__init__(self, "https", host, 8443, user, password)
        self.__cluster = cluster


    @property
    def weburl(self):
        return self.baseurl + "/gateway/" + self.__cluster

    @property
    def hdfsurl(self):
        return  self.weburl + WebHdfs.rootpath

    @property
    def hcaturl(self):
        return self.weburl + WebHCat.rootpath

    @property
    def rmurl(self):
        return self.weburl + ResourceManager.rootpath

    @property
    def apiurl(self):
        return self.weburl + self.rootpath


    @staticmethod
    def Get(url, auth=None, params=None, curl=False):
        return Request("GET", url, auth=auth, params=params, curl=curl)


    def do_cluster(self, cluster):
        if cluster:
            self.__cluster = cluster
        else:
            self.do_echo(self.__cluster)



    def do_version(self, data):
        self.do_echo(self.Get(self.apiurl + "/version",
                        auth=self.auth, curl=self.curl))


    def do_ls(self, dirname):
        if not dirname:
            self.do_echo("Missing file/dir name")
            return

        r = WebHdfs.Get(self.hdfsurl + dirname, "ls",
                        auth=self.auth, curl=self.curl)
        if r is not None:
            if r.get("FileStatuses"):               
                fs_list = r["FileStatuses"]["FileStatus"]
                self.do_echo("\n".join(gen_fileinfo(fs) for fs in fs_list))
            else:
                self.do_echo(r)


    def do_cat(self, filename):
        if filename:
            self.do_echo(WebHdfs.Get(self.hdfsurl + filename, "cat", 
                                     auth=self.auth, curl=self.curl, text=True))


    def do_show(self, data):
        if data:
            params = data.split()
            if len(params) > 0 and params[0].startswith("topo"):
                self.do_echo(self.get_topo("".join(params[1:])))
            elif len(params) == 1 and params[0] == "databases":
                self.do_echo(self.get_database())
            elif len(params) == 2 and params[0] == "tables":
                self.do_echo(self.get_table(params[1]))
            else:
                self.do_echo("Invalid parameter '%s'" % data)


    def do_desc(self, data):
        if data:
            params = data.split()
            if len(params) == 2  and params[0] == "database":
                self.do_echo(self.get_database(params[1]))
            elif len(params) == 3 and params[0] == "table":
                self.do_echo(self.get_table(params[1], params[2]))
            else:
                self.do_echo("Invalid parameter '%s'" % params[0])


    def get_topo(self, topology):
        url = self.apiurl + "/topologies"
        if topology:
            url += "/%s" % topology

        return self.Get(url, auth=self.auth, curl=self.curl)


    def get_cluster_info(self):
        return ResourceManager.Get(self.rmurl + "/info", 
                                   auth=self.auth, curl=self.__curl)


    def get_cluster_metrics(self):
        return ResourceManager.Get(self.rmurl + "/metrics", 
                                   auth=self.auth, curl=self.__curl)


    def get_node(self, nodeid=None):
        url = self.rmurl + "/nodes"
        if nodeid is not None:
            url += "/%s" % nodeid
       
        return ResourceManager.Get(url, auth=self.auth, curl=self.__curl)



    def get_database(self, dbname=None):
        url = self.hcaturl + "/ddl/database"
        if dbname is not None:
            url += "/" + dbname

        return WebHCat.Get(url, auth=self.auth)


    def get_table(self, dbname, tablename=None):
        url = self.hcaturl + "/ddl/database/%s/table" % dbname 
        if tablename is not None:
            url += "/" + tablename

        return WebHCat.Get(url, auth=self.auth)


#
# ---- main ----
if __name__ == "__main__":
    print "Knox"
#    import sys
#    import json
#    from optparse import OptionParser
#
#    parser = OptionParser()
#    parser.add_option("--host", default="localhost")
#    parser.add_option("--curl", action="store_true", default=False)
#    parser.add_option("-u", "--user", default="caiche")
#    parser.add_option("-p", "--password", default="caiche-password")
#
#    opts, args = parser.parse_args()
#
#    if __debug__: print opts, args
#
#    if len(args) < 1:
#        print "Missing arguments, supported methods are", Knox.operations
#        sys.exit(1)
#
#    knox = Knox(opts.host, opts.user, opts.password, curl=opts.curl)
#
#    method = args[0]
#    if not method in knox:
#        print "Unsupported method '%s'" % method
#        sys.exit(1)
#
#    try:
#        fun = getattr(knox, method)
#        result = fun(*args[1:])
#        if isinstance(result, (dict, list)):
#            print json.dumps(result, indent=4)
#        else:
#            print result
#
#    except AttributeError as e:
#        print e
#
