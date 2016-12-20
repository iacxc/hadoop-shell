
from __future__ import print_function

#exports
__all__ = ('Hdfs',)


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
            "rename": "RENAME"}[op]


class Hdfs(HadoopUtil):
    rootpath = "/webhdfs/v1"
    commands = HadoopUtil.commands + [
        "",
        CmdTuple("lls",               "List local files/directoies"),
        CmdTuple("ls",                "List files/directoies"),
        CmdTuple("dir",               "List directoies"),
        CmdTuple("pwd",               "Show the current dir"),
        CmdTuple("cd",                "Change the current dir"),
        CmdTuple("cat <file>",        "Type a text file"),
        CmdTuple("mkdir <dir>",       "Create a directory"),
        CmdTuple("cp <localfile> <remotefile>",
                                      "Copy a local file to a remote file"),
        CmdTuple("append <localfile> <remotefile>",
                                     "Append a localfile to a remote file"),
        CmdTuple("chmod <permission> <file>",
                                      "Change the permission of a file"),
        CmdTuple("chown <user[:group]> file",
                                      "Change the owner of a file"),
        CmdTuple("rm <file>",         "Remove a file"),
        CmdTuple("rename <oldfile> <newfile>",   
                                      "Rename a file"),
    ]

    def __init__(self, host, user):
        HadoopUtil.__init__(self, "http", host, 50070, user)
        self.__cwd = self.home

    @property
    def banner(self):
        return "HDFS Shell"

    @property
    def weburl(self):
        return self.baseurl + self.rootpath

    @property
    def home(self):
        return "/user/%s" % self.user

    @property
    def cwd(self):
        return self.__cwd

    def Get(self, url, op, params=None, text=False):
        if params is None: params = {}
        params["op"] = get_opstr(op)

        return super(Hdfs, self).Get(url, user=self.user,
                                        params=params, text=text)

    def Delete(self, url):
        params = {"op" : "DELETE"}
        return super(Hdfs, self).Delete(url, user=self.user, params=params)

    def Put(self, url, op, params=None, data=None,
                 text=False, expected=(STATUS_OK,)):

        if params is None: params = {}
        params["op"] = get_opstr(op)

        return super(Hdfs, self).Put(url, user=self.user,
                                        params=params, data=data,
                                        text=text, expected = expected)

    def Post(self, url, op, params=None, data=None, text=False):
        if params is None: params = {}
        params["op"] = get_opstr(op)
        return super(Hdfs, self).Post(url, user=self.user,
                                         params=params, data=data,
                                         text=text)

#-- operations
    def do_lls(self, data):
        os.system("ls %s" % data)

    def do_pwd(self, data):
        self.do_echo(self.cwd)

    def do_cd(self, data):
        if data:
            if data == "..":
                self.__cwd = os.path.dirname(self.cwd)
            elif data[0] == "/":
                self.__cwd = data
            else:
                if self.cwd == "/":
                    self.__cwd = "/%s" % data
                else:
                    self.__cwd = "%s/%s" % (self.cwd, data)
        else:
            self.__cwd = self.home

    def do_ls(self, filename, filter_=lambda _:True):
        if len(filename) == 0:
            filename = self.cwd

        if filename[0] != "/":
            filename = "%s/%s" % (self.cwd, filename)

        r = self.Get(self.weburl + filename, "ls")

        if r is not None:
            if r.get("FileStatuses"):
                fs_list = r["FileStatuses"]["FileStatus"]
                self.do_echo("\n".join(filter(filter_, 
                                (gen_fileinfo(fs) for fs in fs_list))))
            else:
                self.do_echo("File not found")

    def do_dir(self, filename):
        self.do_ls(filename, lambda f: f[0] == "d")

    def do_mkdir(self, data):
        params = data.split()
        if len(params) < 1:
            self.do_echo("Incorrect parameters")
            return

        dirname = params[0]
        if dirname[0] != "/":
            dirname = "%s/%s" % (self.cwd, dirname)

        perm = params[1] if len(params) > 1 else "777"
        self.do_echo(self.Put(self.weburl + dirname, "mkdir",
                        params={"permission" : perm}))

    def do_cp(self, data):
        params = data.split()
        if len(params) != 2:
            self.do_echo("Incorrect parameters")
            return

        localfile, remotefile = params
        if remotefile[0] != "/":
            remotefile = "%s/%s" % (self.cwd, remotefile)
        with file(localfile) as f:
            r = self.Put(self.weburl + remotefile, "cp",
                         params={"overwrite" : "true"},
                         data=f.read(),
                         text=True,
                         expected=(STATUS_CREATED,))

            self.do_echo({"status": "OK"} if r is not None else "Failed")

    def do_append(self, data):
        params = data.split()
        if len(params) != 2:
            self.do_echo("Incorrect parameters")
            return

        localfile, remotefile = params
        if remotefile[0] != "/":
            remotefile = "%s/%s" % (self.cwd, remotefile)
        with file(localfile) as f:
            r = self.Post(self.weburl + remotefile, "append",
                          data=f.read(), text=True)

            self.do_echo({"status": "OK"} if r is not None else "Failed")

    def do_rm(self, filename):
        if filename:
            if filename[0] != "/":
                filename = "/user/%s/%s" % (self.user, filename)

            self.do_echo(self.Delete(self.weburl + filename))
        else:
            self.do_echo("Missing filename")

    def do_cat(self, filename):
        if filename:
            if filename[0] != "/":
                filename = "%s/%s" % (self.cwd, filename)
            self.do_echo(self.Get(self.weburl + filename, "cat", text=True))
        else:
            self.do_echo("Missing filename")

    def do_rename(self, data):
        params = data.split()
        if len(params) != 2:
            self.do_echo("Incorrect parameters")
            return

        srcname, destname = params
        if srcname[0] != "/":
            srcname = "/user/%s/%s" % (self.user, srcname)
        if destname[0] != "/":
            destname = "/user/%s/%s" % (self.user, destname)
        self.do_echo(self.Put(self.weburl + srcname, "rename",
                        params={"destination" : destname}))

    def do_chmod(self, data):
        params = data.split()
        if len(params) != 2:
            self.do_echo("Incorrect parameters")
            return

        perm, filename = params
        if filename[0] != "/":
            filename = "%s/%s" % (self.cwd, filename)
        r = self.Put(self.weburl + filename, "chmod",
                     params={"permission" : perm}, text=True)

        self.do_echo({"status" : "OK"} if r is not None else "Failed")

    def do_chown(self, data):
        params = data.split()
        if len(params) != 2:
            self.do_echo("Incorrect parameters")
            return

        owner, filename = params
        if filename[0] != "/":
            filename = "%s/%s" % (self.cwd, filename)
        group = None
        if owner.count(":") == 1:
            owner, group = owner.split(":")

        r = self.Put(self.weburl + filename, "chown",
                     params={"owner" : owner, "group" : group}, text=True)

        self.do_echo({"status" : "OK"} if r is not None else "Failed")


#
# ---- main ----
if __name__ == "__main__":
    print("Hdfs")
