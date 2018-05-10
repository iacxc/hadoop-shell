
#exports
__all__ = ('HdfsServer', )

import os

from FileUtil import fileinfo
from RestServer import RestServer, STATUS_OK, STATUS_CREATED


def get_opstr(op):
    return {
        "ls": "LISTSTATUS",
        "cat": "OPEN",
        "mkdir": "MKDIRS",
        "put": "CREATE",
        "home": "GETHOMEDIRECTORY",
        "append": "APPEND",
        "chmod": "SETPERMISSION",
        "chown": "SETOWNER",
        "rename": "RENAME",
        "stat": "GETFILESTATUS"
    }[op]


class HdfsServer(RestServer):
    rootpath = "/webhdfs/v1"

    def __init__(self, opts):
        super(HdfsServer, self).__init__(opts)

        self.__cwd = self.home

    @property
    def weburl(self):
        return self.baseurl + self.rootpath

    @property
    def home(self):
        r = self.Get(self.weburl, "home")
        return r["Path"] if r is not None else "/"

    @property
    def cwd(self):
        return self.__cwd

    def Get(self, url, op, params=None, text=False):
        if params is None: params = {}
        params["op"] = get_opstr(op)

        return super(HdfsServer, self).Get(url,
                                        user=self.user,
                                        params=params,
                                        text=text)

    def Delete(self, url):
        params = {"op": "DELETE"}
        return super(HdfsServer, self).Delete(url, user=self.user, params=params)

    def Put(self,
            url,
            op,
            params=None,
            data=None,
            text=False,
            expected=(STATUS_OK, )):

        if params is None: params = {}
        params["op"] = get_opstr(op)

        return super(HdfsServer, self).Put(url,
                                        user=self.user,
                                        params=params,
                                        data=data,
                                        text=text,
                                        expected=expected)

    def Post(self, url, op, params=None, data=None, text=False):
        if params is None: params = {}
        params["op"] = get_opstr(op)
        return super(HdfsServer, self).Post(
            url, user=self.user, params=params, data=data, text=text)


    def exist(self, path):
        if path[0] != "/":
            path = "%s/%s" % (self.cwd, path)
        r = self.Get(self.weburl + path, "stat")
        if r is None: return
        return "FileStatus" in r

    def is_dir(self, path):
        if path[0] != "/":
            path = "%s/%s" % (self.cwd, path)
        r = self.Get(self.weburl + path, "stat")
 
        if r is None: return False
        return r.get("FileStatus", {}).get("type", "") == "DIRECTORY"

#-- operations

    def do_lls(self, data=''):
        os.system("ls %s" % data)

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

    def do_ls(self, filename, filter_=lambda _: True):
        if len(filename) == 0:
            filename = self.cwd

        if filename[0] != "/":
            filename = "%s/%s" % (self.cwd, filename)

        r = self.Get(self.weburl + filename, "ls")

        if r is not None:
            if r.get("FileStatuses"):
                fs_list = r["FileStatuses"]["FileStatus"]
                return "\n".join(
                    filter(filter_, (fileinfo(fs) for fs in fs_list)))
            else:
                return "File not found"

    def do_dir(self, filename):
        return self.do_ls(filename, lambda f: f[0] == "d")

    def do_mkdir(self, data):
        params = data.split()
        if len(params) < 1:
            return "Incorrect parameters"
            return

        dirname = params[0]
        if dirname[0] != "/":
            dirname = "%s/%s" % (self.cwd, dirname)

        perm = params[1] if len(params) > 1 else "777"
        return self.Put(self.weburl + dirname,
                        "mkdir",
                        params={"permission": perm})

    def do_put(self, data):
        params = data.split()
        if len(params) == 1:
            localfile = params[0]
            remotefile = os.path.basename(localfile)
        elif len(params) == 2:
            localfile, remotefile = params
        else:
            return "Incorrect parameters"

        if remotefile[0] != "/":
            remotefile = "%s/%s" % (self.cwd, remotefile)
        if self.is_dir(remotefile):
            remotefile = remotefile + "/" + os.path.basename(localfile)
        with open(localfile) as f:
            r = self.Put(self.weburl + remotefile,
                         "put",
                         params={"overwrite": "true"},
                         data=f.read(),
                         text=True,
                         expected=(STATUS_CREATED, ))

            return {"status": "OK"} if r is not None else "Failed"

    def do_cp(self, data):
        params = data.split()
        if len(params) != 2:
            return "Incorrect parameters"

        srcfile, destfile = params
        if srcfile[0] != "/":
            srcfile = "/user/%s/%s" % (self.user, srcfile)
        if destfile[0] != "/":
            destfile = "/user/%s/%s" % (self.user, destfile)

        if srcfile == destfile:
            return "Cannot copy a file to itself"

        if self.is_dir(destfile):
            destfile = destfile + "/" + os.path.basename(srcfile)
        if self.exist(destfile):
            return "file %s already exist, cannot overwrite" % destfile

        r = self.Put(self.weburl + destfile,
                     "put",
                     data=self.Get(self.weburl + srcfile, "cat", text=True),
                     text=True,
                     expected=(STATUS_CREATED, ))

    def do_append(self, data):
        params = data.split()
        if len(params) != 2:
            self.do_echo("Incorrect parameters")
            return

        localfile, remotefile = params
        if remotefile[0] != "/":
            remotefile = "%s/%s" % (self.cwd, remotefile)
        with open(localfile) as f:
            r = self.Post(
                self.weburl + remotefile, "append", data=f.read(), text=True)

            return {"status": "OK"} if r is not None else "Failed"

    def do_rm(self, filename):
        if filename:
            if filename[0] != "/":
                filename = "/user/%s/%s" % (self.user, filename)

            return self.Delete(self.weburl + filename)
        else:
            return "Missing filename"

    def do_cat(self, filename):
        if filename:
            if filename[0] != "/":
                filename = "%s/%s" % (self.cwd, filename)
            return self.Get(self.weburl + filename, "cat", text=True)
        else:
            return "Missing filename"

    def do_get(self, data):
        params = data.split()
        if len(params) == 1:
            localfile = remotefile = params[0]
        elif len(params) == 2:
            remotefile, localfile = params
        else:
            return "Incorrect parameters"

        if remotefile[0] != "/":
            remotefile = "%s/%s" % (self.cwd, remotefile)

        with open(localfile, 'w') as fd:
            fd.write(self.Get(self.weburl + remotefile, "cat", text=True))
 
    def do_rename(self, data):
        params = data.split()
        if len(params) != 2:
            return "Incorrect parameters"

        srcname, destname = params
        if srcname[0] != "/":
            srcname = "/user/%s/%s" % (self.user, srcname)
        if destname[0] != "/":
            destname = "/user/%s/%s" % (self.user, destname)
        return self.Put(self.weburl + srcname,
                        "rename",
                        params={"destination": destname})

    def do_chmod(self, data):
        params = data.split()
        if len(params) != 2:
            return "Incorrect parameters"

        perm, filename = params
        if filename[0] != "/":
            filename = "%s/%s" % (self.cwd, filename)
        r = self.Put(self.weburl + filename,
                     "chmod",
                     params={"permission": perm},
                     text=True)

        return {"status": "OK"} if r is not None else "Failed"

    def do_chown(self, data):
        params = data.split()
        if len(params) != 2:
            return "Incorrect parameters"

        owner, filename = params
        if filename[0] != "/":
            filename = "%s/%s" % (self.cwd, filename)
        group = None
        if owner.count(":") == 1:
            owner, group = owner.split(":")

        r = self.Put(self.weburl + filename,
                     "chown",
                     params={"owner": owner,
                             "group": group},
                     text=True)

        return {"status": "OK"} if r is not None else "Failed"


#
# ---- main ----
if __name__ == "__main__":
    server = HdfsServer("g4t8720.houston.hp.com", "caiche")
    print(server.do_ls('/user'))
