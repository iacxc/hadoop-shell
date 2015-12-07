

#exports
__all__ = ("WebHCat",)

from HadoopUtil import HadoopUtil, Request

class WebHCat(HadoopUtil):
    operations = ("status", 
                  "version",
                  "runddl",
                  "get_database",
                  "get_table",)
    rootpath = "/templeton/v1"

    def __init__(self, host, user, curl=False):
        super(WebHCat, self).__init__("http", host, 50111)
        self.__user = user
        self.__curl = curl

    def __iter__(self):
        return iter(self.operations)


    @staticmethod
    def Get(url, user=None, auth=None, params=None, curl=False):
        return Request("GET", url, user, auth, params, curl=curl)

    @staticmethod
    def Post(url, user=None, auth=None, data=None, curl=False):
        return Request("POST", url, user, auth, data=data, curl=curl)

    @property
    def weburl(self):
        return self.baseurl + self.rootpath


    def status(self):
        return self.Get(self.weburl + "/status" , 
                        self.__user, curl=self.__curl)


    def version(self, component=None):
        url = self.weburl + "/version"
        if component is not None:
            url += "/%s" % component

        return self.Get(url, self.__user, curl=self.__curl)


    def runddl(self, ddl):
        result = self.Post(self.weburl + "/ddl", self.__user,
                           data="exec=%s" % ddl, 
                           curl=self.__curl)
        return result
            

    def get_database(self, dbname=None):
        url = self.weburl + "/ddl/database"
        if dbname is not None:
            url += "/%s" % dbname

        return self.Get(url, self.__user, curl=self.__curl)


    def get_table(self, dbname, tablename=None):
        url = self.weburl + "/ddl/database/%s/table" % dbname 
        if tablename is not None:
            url += "/%s" % tablename

        return self.Get(url, self.__user, curl=self.__curl)


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
        print "Missing arguments, supported methods are", WebHCat.operations
        sys.exit(1)

    hcatalog = WebHCat(opts.host, opts.user, opts.curl)

    method = args[0]
    if not method in hcatalog:
        print "Unsupported method '%s'" % method
        sys.exit(1)

    try:
        fun = getattr(hcatalog, method)
        result = fun(*args[1:])
        if isinstance(result, (dict, list)):
            print json.dumps(result, indent=4)
        else:
            print result

    except AttributeError as e:
        print e

