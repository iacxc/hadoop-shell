

#exports
__all__ = ('WebHdfs',)


from HadoopUtil import HadoopUtil, Request, CmdTuple, \
                       gen_fileinfo, \
                       STATUS_OK, STATUS_CREATED
import os

def get_opstr(op):
    return {"ls"    : "LISTSTATUS",
            "cat"   : "OPEN",
            "mkdir" : "MKDIRS",
            "cp"    : "CREATE",
            "append": "APPEND",
            "chmod" : "SETPERMISSION",
            "chown" : "SETOWNER",
            "delete": "DELETE",
            "rename": "RENAME"}[op]


class WebHdfs(HadoopUtil):
    rootpath = "/webhdfs/v1"
    commands = HadoopUtil.commands + [
        "",
        CmdTuple("lls",    "",  "List local files/directoies"),
        CmdTuple("ls",     "",  "List files/directoies"),
        CmdTuple("pwd",    "",  "Show the current dir"),
        CmdTuple("cd",     "",  "Change the current dir"),
        CmdTuple("cat",    "",  "Type a text file"),
        CmdTuple("mkdir",  "",  "Show a policy"),
        CmdTuple("cp",     "",  "Create a repository"),
        CmdTuple("append", "",  "Create a policy"),
        CmdTuple("chmod",  "",  "Update a repository"),
        CmdTuple("chown",  "",  "Update a policy"),
        CmdTuple("delete", "",  "Delete a repository"),
        CmdTuple("rename", "",  "Delete a policy"),
        ]


    def __init__(self, host, user):
        HadoopUtil.__init__(self, "http", host, 50070, user)
        self.__curdir = self.home


    @property
    def weburl(self):
        return self.baseurl + self.rootpath


    @property
    def home(self):
        return "/user/%s" % self.user


    @staticmethod
    def Get(url, op, user=None, auth=None, params=None, curl=False, 
                 text=False):
        if params is None: params = {}
        params["op"] = get_opstr(op)

        return Request("GET", url, user, auth, params, curl=curl, text=text)


    @staticmethod
    def Delete(url, user=None, auth=None, curl=False):
        params = {"op" : "DELETE"}
        return Request("DELETE", url, user, auth, params=params, curl=curl)


    @staticmethod
    def Put(url, op, user=None, auth=None, params=None, data=None, 
                 curl=False, text=False, expected=(STATUS_OK,)):

        if params is None: params = {}
        params["op"] = get_opstr(op)

        return Request("PUT", url, user, auth, params, data, 
                              curl=curl, text=text, expected = expected)


    @staticmethod
    def Post(url, op, user=None, auth=None, params=None, data=None, 
                  curl=False):
        if params is None: params = {}
        params["op"] = get_opstr(op)
        return Request("POST", url, user, auth, params, data, curl=curl)


#-- operations
    def do_lls(self, data):
        os.system("ls %s" % data)


    def do_pwd(self, data):
        print self.__curdir


    def do_cd(self, data):
        if data:
            if data == "..":
                self.__curdir = os.path.dirname(self.__curdir)
            elif data[0] == "/":
                self.__curdir = data
            else:
                self.__curdir = "%s/%s" % (self.__curdir, data)
        else:
            self.__curdir = self.home


    def do_ls(self, filename):
        if len(filename) == 0:
            filename = self.__curdir

        result = self.Get(self.weburl + filename, "ls", self.user, 
                          curl=self.curl)

        if result is not None:
            if result.get("FileStatuses"):
                fs_list = result["FileStatuses"]["FileStatus"]
                print "\n".join(gen_fileinfo(fs) for fs in fs_list)
            else:
                print "File not found"


    def do_mkdir(self, data):
        params = data.split()
        if len(params) < 1:
            print "Incorrect parameters"
            return

        dirname = params[0]
        if dirname[0] != "/":
            dirname = "%s/%s" % (self.__curdir, dirname)

        perm = params[1] if len(params) > 1 else "777"
        print self.Put(self.weburl + dirname, "mkdir", self.user, 
                        params={"permission" : perm},
                        curl=self.curl)


    def do_cp(self, data):
        params = data.split()
        if len(params) != 2:
            print "Incorrect parameters"
            return

        localfile, remotefile = params
        if remotefile[0] != "/":
            remotefile = "%s/%s" % (self.__curdir, remotefile)
        with file(localfile) as f:
            result = self.Put(self.weburl + remotefile, "cp", self.user, 
                              params={"overwrite" : "true"}, 
                              data=f.read(),
                              curl=self.curl, 
                              expected=(STATUS_CREATED,))

            if result is not None:
                print {"status" : "OK"}
            else:
                print "Failed"


    def do_append(self, data):
        params = data.split()
        if len(params) != 2:
            print "Incorrect parameters"
            return

        localfile, remotefile = params
        if remotefile[0] != "/":
            remotefile = "%s/%s" % (self.__curdir, remotefile)
        with file(localfile) as f:
            result = self.Post(self.weburl + remotefile, "append", self.user,
                               data=f.read(),
                               curl=self.curl)

            print {"status" : "OK"} if r is not None else "Failed"


    def do_delete(self, filename) : 
        if filename:
            if filename[0] != "/":
                filename = "/user/%s/%s" % (self.user, filename)
            print self.Delete(self.weburl + filename, self.user, curl=self.curl)
        else:
            print "Missing filename"


    def do_cat(self, filename):
        if filename:
            if filename[0] != "/":
                filename = "/user/%s/%s" % (self.user, filename)
            print self.Get(self.weburl + filename, "cat", self.user,
                        curl=self.curl, text=True)
        else:
            print "Missing filename"


    def do_rename(self, data):
        params = data.split()
        if len(params) != 2:
            print "Incorrect parameters"
            return

        srcname, destname = params
        if srcname[0] != "/":
            srcname = "/user/%s/%s" % (self.user, srcname)
        if destname[0] != "/":
            destname = "/user/%s/%s" % (self.user, destname)
        return self.Put(self.weburl + srcname, "rename", self.user,
                        params={"destination" : destname}, 
                        curl=self.curl)


    def do_chmod(self, data):
        params = data.split()
        if len(params) != 2:
            print "Incorrect parameters"
            return

        perm, filename = params
        if filename[0] != "/":
            filename = "%s/%s" % (self.__curdir, filename)
        r = self.Put(self.weburl + filename, "chmod", self.user,
                     params={"permission" : perm}, curl=self.curl)

        print {"status" : "OK"} if r is not None else "Failed"


    def do_chown(self, data):
        params = data.split()
        if len(params) != 2:
            print "Incorrect parameters"
            return

        owner, filename = params
        if filename[0] != "/":
            filename = "%s/%s" % (self.__curdir, filename)
        group = None
        if owner.count(":") == 1:
            owner, group = owner.split(":")

        r = self.Put(self.weburl + filename, "chown", self.user,
                     params={"owner" : owner, "group" : group}, 
                     curl=self.curl)

        print {"status" : "OK"} if r is not None else "Failed"


#
# ---- main ----
if __name__ == "__main__":
    print "WebHdfs"
#    import sys
#    import json
#    from optparse import OptionParser
#
#    parser = OptionParser()
#    parser.add_option("--host", default="localhost")
#    parser.add_option("--curl", action="store_true", default=False)
#    parser.add_option("-u", "--user", default="caiche")
#
#    opts, args = parser.parse_args()
#
#    if __debug__: print opts, args
#
#    if len(args) < 1:
#        print "Missing arguments, supported methods are", WebHdfs.operations
#        sys.exit(1)
#
#    hdfs = WebHdfs(opts.host, opts.user, opts.curl)
#
#    method = args[0]
#    if not method in hdfs:
#        print "Unsupported method '%s'" % method
#        sys.exit(1)
#
#    try:
#        fun = getattr(hdfs, method)
#        result = fun(*args[1:])
#        if isinstance(result, (dict, list)):
#            print json.dumps(result, indent=4)
#        else:
#            print result
#
#    except AttributeError as e:
#        print e
#
#
