

#exports
__all__ = ("WebHCat",)

from HadoopUtil import HadoopUtil, Request, CmdTuple

class WebHCat(HadoopUtil):
    operations = ("status", 
                  "version",
                  "runddl",
                  "get_database",
                  "get_table",)
    rootpath = "/templeton/v1"
    commands = HadoopUtil.commands + [
        "",
        CmdTuple("status",                 "Get status"),
        CmdTuple("version [component]",    "Show the version"),
        CmdTuple("use <dbname>",           "Change the current db"),
        CmdTuple("runddl <command>",       "Run ddl command"),
        CmdTuple("show databases",          "List all databases"),
        CmdTuple("show tables",             "List all tables"),
        CmdTuple("desc database <dbname>", "Show detals of current database"),
        CmdTuple("desc table <table>",     "Show detals of a table"),
    ]

    def __init__(self, host, user):
        HadoopUtil.__init__(self, "http", host, 50111, user)
        self.__db = "default"


    @property
    def banner(self):
        return "HCatalog Shell"

    @property
    def weburl(self):
        return self.baseurl + self.rootpath


    @property
    def db(self):
        return self.__db


    @staticmethod
    def Get(url, user=None, auth=None, params=None, curl=False):
        return Request("GET", url, user, auth, params, curl=curl)

    @staticmethod
    def Post(url, user=None, auth=None, data=None, curl=False):
        return Request("POST", url, user, auth, data=data, curl=curl)


    def do_status(self, data):
        self.do_echo(self.Get(self.weburl + "/status" , 
                              self.user, curl=self.curl))


    def do_version(self, component=""):
        url = self.weburl + "/version"
        if component:
            url += "/%s" % component

        self.do_echo(self.Get(url, self.user, curl=self.curl))


    def do_use(self, data):
        if data:
            self.__db = data
        else:
            self.do_echo(self.db)


    def do_runddl(self, ddl=""):
        if not ddl:
            self.do_echo("Missing ddl command")
        else:
            self.do_echo(self.Post(self.weburl + "/ddl", self.user,
                                   data="exec=%s" % ddl, 
                                   curl=self.curl))
            

    def do_show(self, data):
        if data == "databases":
            self.do_echo(self.get_database())
        elif data == "tables":
            self.do_echo(self.get_table(self.db))
        else:
            self.do_echo("Invalid parameter '%s'" % data)


    def do_desc(self, data):
        params = data.split()
        if len(params) < 2:
            self.do_echo("Not enough parameters")
            return 

        if params[0] == "database":
            self.do_echo(self.get_database(params[1]))
        elif params[0] == "table":
            self.do_echo(self.get_table(self.db, params[1]))
        else:
            self.do_echo("Invalid parameter '%s'" % params[0])


    def get_database(self, dbname=None):
        url = self.weburl + "/ddl/database"
        if dbname is not None:
            url += "/%s" % dbname

        return self.Get(url, self.user, curl=self.curl)


    def get_table(self, dbname, tablename=None):
        url = self.weburl + "/ddl/database/%s/table" % dbname 
        if tablename is not None:
            url += "/%s" % tablename

        return self.Get(url, self.user, curl=self.curl)


#
# ---- main ----
if __name__ == "__main__":
    print "WebHCatalog"
