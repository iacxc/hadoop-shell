

#exports
__all__ = ('ResourceManager',)


from HadoopUtil import HadoopUtil, Request, \
                       gen_fileinfo, \
                       STATUS_OK, STATUS_CREATED

class ResourceManager(HadoopUtil):
    operations = ("cluster_info", "cluster_metrics",
                  "get_node", "get_app")
    rootpath = "/ws/v1/cluster"

    def __init__(self, host, user, curl=False):
        super(ResourceManager, self).__init__("http", host, 8088)
        self.__user = user
        self.__curl = curl


    def __iter__(self):
        return iter(self.operations)


    @property
    def weburl(self):
        return self.baseurl + self.rootpath


    @staticmethod
    def Get(url, user=None, auth=None, params=None, curl=False, 
                 text=False):
        return Request("GET", url, user, auth, params, curl=curl, text=text)



#-- operations
    def cluster_info(self):
        return self.Get(self.weburl + "/info", curl=self.__curl)


    def cluster_metrics(self):
        return self.Get(self.weburl + "/metrics", curl=self.__curl)


    def get_node(self, nodeid=None):
        url = self.weburl + "/nodes"
        if nodeid is not None:
            url += "/%s" % nodeid
       
        return self.Get(url, curl=self.__curl)


    def get_app(self, appid=None, params=None):
        url = self.weburl + "/apps" if appid in (None, "None") \
                   else "%s/apps/%s" % (self.weburl, appid)
        return self.Get(url, params=params, curl=self.__curl)


#
# ---- main ----
if __name__ == "__main__":
    import sys
    import json
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("--host", default="localhost")
    parser.add_option("--curl", action="store_true", default=False)
    parser.add_option("-u", "--user", default="caiche")

    opts, args = parser.parse_args()

    if __debug__: print opts, args

    if len(args) < 1:
        print "Missing arguments, supported methods are", ResourceManager.operations
        sys.exit(1)

    hdfs = ResourceManager(opts.host, opts.user, opts.curl)

    method = args[0]
    if not method in hdfs:
        print "Unsupported method '%s'" % method
        sys.exit(1)

    try:
        fun = getattr(hdfs, method)
        result = fun(*args[1:])
        if isinstance(result, (dict, list)):
            print json.dumps(result, indent=4)
        else:
            print result

    except AttributeError as e:
        print e


