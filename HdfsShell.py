
from __future__ import print_function

#exports
__all__ = ('HdfsShell', )

import os
from HadoopShell import HadoopCmd, Seperator, HadoopShell
from HdfsServer import HdfsServer


class HdfsShell(HadoopShell):
    commands = HadoopShell.commands + [
        HadoopCmd("lls", "List local files/directoies"),
        HadoopCmd("ls", "List files/directoies"),
        HadoopCmd("dir", "List directoies"),
        HadoopCmd("pwd", "Show the current dir"),
        HadoopCmd("cd", "Change the current dir"),
        HadoopCmd("cat", "Type a text file", "<file>"),
        HadoopCmd("mkdir", "Create a directory", "<dir>"),
        HadoopCmd("cp", "Copy a local file to a remote file",
                  ["<localfile>", "<remotefile>"]),
        HadoopCmd("append", "Append a localfile to a remote file",
                  ["<localfile>", "<remotefile>"]),
        HadoopCmd("chmod", "Change the permission of a file",
                  ["<permission>", "<file>"]),
        HadoopCmd("chown", "Change the owner of a file",
                  ["chown <user[:group]>" , "file"]),
        HadoopCmd("rm", "Remove a file", "<file>"),
        HadoopCmd("rename", "Rename a file", ["<oldfile", "<newfile>"]),
    ]

    def __init__(self, server):
        super(HdfsShell, self).__init__(server)

    @property
    def banner(self):
        return "HDFS Shell"

#-- operations
    def do_lls(self, data):
        self.server.do_lls(data)

    def do_pwd(self, data):
        self.do_echo(self.server.cwd)

    def do_cd(self, data):
        self.server.do_cd(data)

    def do_ls(self, filename, filter_=lambda _:True):
        self.do_echo(self.server.do_ls(filename, filter_))

    def do_dir(self, filename):
        self.do_echo(self.server.do_ls(filename, lambda f: f[0] == "d"))

    def do_mkdir(self, data):
        self.do_echo(self.server.do_mkdir(data))

    def do_cp(self, data):
        self.do_echo(self.server.do_cp(data))

    def do_append(self, data):
        self.do_echo(self.server.do_append(data))

    def do_rm(self, filename):
        self.do_echo(self.server.do_rm(filename))

    def do_cat(self, filename):
        self.do_echo(self.server.do_cat(filename))

    def do_rename(self, data):
        self.do_echo(self.server.do_rename(data))

    def do_chmod(self, data):
        self.do_echo(self.server.do_chmod(data))

    def do_chown(self, data):
        self.do_echo(self.do_chown(data))

#
# ---- main ----
if __name__ == "__main__":
    shell = HdfsShell(HdfsServer("g4t8720.houston.hp.com", "caiche"))
    shell.cmdloop()

#!/usr/bin/python

from WebHdfs import WebHdfs

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("--host")
    parser.add_option("-u", "--user", default="caiche")

    opts, args = parser.parse_args()

    if __debug__:
        print opts, args

    if opts.host is None:
        if len(args) > 0:
            opts.host = args[0]
        else:
            opts.host = 'localhost'

    hdfs = WebHdfs(opts.host, opts.user)

    hdfs.cmdloop()