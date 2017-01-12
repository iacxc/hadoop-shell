from __future__ import print_function

#exports
__all__ = ("HCatServer", )

from RestServer import RestServer

class HCatServer(RestServer):
    rootpath = "/templeton/v1"
    def __init__(self, opts):
        super(HCatServer, self).__init__(opts)
        self.__db = "default"

    @property
    def weburl(self):
        return self.baseurl + self.rootpath

    @property
    def db(self):
        return self.__db

    def Get(self, url):
        return super(HCatServer, self).Get(url, user=self.user)

    def Post(self, url, data):
        return super(HCatServer, self).Post(url, data=data)

    def do_status(self, data):
        return self.Get(self.weburl + "/status")

    def do_version(self, component=""):
        url = self.weburl + "/version"
        if component:
            url += "/%s" % component

        return self.Get(url)

    def do_use(self, data):
        if data:
            self.__db = data
        else:
            return self.db

    def do_runddl(self, ddl=""):
        if not ddl:
            return "Missing ddl command"
        else:
            return self.Post(self.weburl + "/ddl", "exec=%s" % ddl) 
            
    def do_show(self, data):
        if data == "databases":
            return self.get_database()
        elif data == "tables":
            return self.get_table(self.db)
        else:
            return "Invalid parameter '%s'" % data

    def do_desc(self, data):
        params = data.split()
        if len(params) < 2:
            return "Not enough parameters"

        if params[0] == "database":
            return self.get_database(params[1])
        elif params[0] == "table":
            return self.get_table(self.db, params[1])
        else:
            return "Invalid parameter '%s'" % params[0]

    def get_database(self, dbname=None):
        url = self.weburl + "/ddl/database"
        if dbname is not None:
            url += "/%s" % dbname

        return self.Get(url)

    def get_table(self, dbname, tablename=None):
        url = self.weburl + "/ddl/database/%s/table" % dbname 
        if tablename is not None:
            url += "/%s" % tablename

        return self.Get(url)


#
# ---- main ----
if __name__ == "__main__":
    print("HCatalog Server")
