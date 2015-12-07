

#exports
__all__ = ("Knox",)


from HadoopUtil import HadoopUtil, Request, \
                       gen_fileinfo
from WebHdfs import WebHdfs
from WebHCat import WebHCat
from ResourceManager import ResourceManager


class Knox(HadoopUtil):
    operations = ("ls", "cat", 
                  "cluster_info", "cluster_metrics", "get_node",
                  "get_database", "get_table",)

    def __init__(self, host, user, password, topo="default", curl=False):
        super(Knox, self).__init__("https", host, 8443)
        self.__user = user
        self.__password = password
        self.__curl = curl

        self.weburl = self.baseurl + "/gateway/" + topo
        self.hdfsurl = self.weburl + WebHdfs.rootpath
        self.hcaturl = self.weburl + WebHCat.rootpath
        self.rmurl = self.weburl + ResourceManager.rootpath


    def __iter__(self):
        return iter(self.operations)


    @property
    def auth(self):
        return (self.__user, self.__password)


    def ls(self, dirname):
        r = WebHdfs.Get(self.hdfsurl + dirname, "ls",
                        auth=self.auth, curl=self.__curl)

        if r is not None:
            fs_list = r["FileStatuses"]["FileStatus"]
            return [gen_fileinfo(fs) for fs in fs_list]


    def cat(self, filename):
        return WebHdfs.Get(self.hdfsurl + filename, "cat", 
                        auth=self.auth, curl=self.__curl, text=True)


    def cluster_info(self):
        return ResourceManager.Get(self.rmurl + "/info", 
                                   auth=self.auth, curl=self.__curl)


    def cluster_metrics(self):
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
    import sys
    import json
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("--host", default="localhost")
    parser.add_option("--curl", action="store_true", default=False)
    parser.add_option("-u", "--user", default="caiche")
    parser.add_option("-p", "--password", default="caiche-password")

    opts, args = parser.parse_args()

    if __debug__: print opts, args

    if len(args) < 1:
        print "Missing arguments, supported methods are", Knox.operations
        sys.exit(1)

    knox = Knox(opts.host, opts.user, opts.password, curl=opts.curl)

    method = args[0]
    if not method in knox:
        print "Unsupported method '%s'" % method
        sys.exit(1)

    try:
        fun = getattr(knox, method)
        result = fun(*args[1:])
        if isinstance(result, (dict, list)):
            print json.dumps(result, indent=4)
        else:
            print result

    except AttributeError as e:
        print e

