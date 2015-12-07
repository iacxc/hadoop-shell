

#exports
__all__ = ("HadoopUtil", "Request",
           "gen_fileinfo",
           "STATUS_OK",
           "STATUS_CREATED",
           "STATUS_NOCONTENT",
           )


import requests
import json
from cmd import Cmd
from collections import namedtuple

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


STATUS_OK = requests.codes.ok
STATUS_CREATED = requests.codes.created
STATUS_FORBIDDEN = requests.codes.forbidden
STATUS_NOTFOUND = requests.codes.not_found
STATUS_NOTALLOW = requests.codes.not_allowed
STATUS_NOCONTENT = requests.codes.no_content


def getpermission(permission):
    getbit = lambda bit: {'0' : '---',
                          '1' : '--x',
                          '2' : '-w-',
                          '3' : '-wx',
                          '4' : 'r--',
                          '5' : 'r-x',
                          '6' : 'rw-',
                          '7' : 'rwx'}.get(bit, '---')
    return ''.join(map(getbit, permission))


def gen_fileinfo(fs):
    from datetime import datetime
    ts2str = lambda timestamp: \
           datetime.fromtimestamp(timestamp * 0.001).strftime("%m %d %H:%M")

    return "%s%-10s  %-10s%-10s%-8s%-12s%-20s" % (
         'd' if fs.get("type", "FILE") == "DIRECTORY" else '-',
         getpermission(fs.get("permission", "000")),
         fs.get("owner", "<no user>"),
         fs.get("group", "<no group>"),
         fs.get("length", 0),
         ts2str(fs.get("modificationTime", 0)),
         fs.get("pathSuffix", "<no name>"))


def Request(method, url, user=None, auth=None, params=None, 
                         data=None, headers=None, 
                         curl=False, text=False, expected=(STATUS_OK,)):
    if params is None:
        params = {}
    else:
        if isinstance(params, str):
            params = dict(p.split("=") for p in params.split("&"))

    if user:
        params["user.name"] = user

    paramstr = "&".join("%s=%s" % (k,v) for k,v in params.items())

    from urlparse import urlparse
    uri = urlparse(url)

    url = url + ("&" if len(uri.query) > 0 else "?") + paramstr

    if curl:
        print "curl -X {method}{auth}{data}{url}".format(method=method,
               auth="" if auth is None else " --user '%s:%s'" % (auth),
               data="" if data is None else " -d '%s'" % data,
               url=(" -k '%s'" if url.startswith("https") else " '%s'") % url)

    resp = requests.request(method, url, auth=auth, verify=False, 
                            data=data, headers=headers)
    try:
        if resp.status_code in expected:
            return resp.text if text else resp.json()
        elif resp.status_code == STATUS_FORBIDDEN:
            return "Forbidden" if text else {"status" : "Forbidden"}
        elif resp.status_code == STATUS_NOTFOUND:
            return "Not Found" if text else {"status" : "Not Found"}
        elif resp.status_code == STATUS_NOTALLOW:
            return "Not Allowed" if text else {"status": "Not Allowed"}
        else:
            if __debug__: print resp.status_code, resp.text
            return str(resp.status_code) if text \
                                     else {"error_code" : resp.status_code}
    except ValueError:
        return {"status" : "Format error", "text" : resp.text}


CmdTuple = namedtuple("Command", ["cmd", "params", "help"])
class HadoopUtil(Cmd):
    """ base class for a bunch of hadoop utilities """
    commands = [
        CmdTuple("help", "",               "Show this help page"),
        CmdTuple("echo", "",               "Echo the message"),
        CmdTuple("curl", "[on|off]",       "Display or set curl option"),
        CmdTuple("whoami", "",             "Display the current user"),
        CmdTuple("prefix", "[prefix]",     "Display or set the prefix"),
        CmdTuple("host", "[host]",         "Display or set the host"),
        CmdTuple("port", "[port]",         "Display or set the port"),
        CmdTuple("user", "[username]",     "Set the currentuser"),
        CmdTuple("passwd", "[password]",   "Set the password"),
        CmdTuple("quit or exit or ^D", "", "Exit the program") ]

    def __init__(self, prefix, host, port, user, passwd=None):
        Cmd.__init__(self)

        self.__prefix = prefix
        self.__host = host
        self.__port = port
        self.__user = user
        self.__passwd = None
        self.__curl = False

        self.set_prompt()

 
    @property
    def baseurl(self):
        return "%s://%s:%s" % (self.__prefix, self.__host, self.__port)


    @property
    def user(self):
        return self.__user


    @property
    def passwd(self):
        return self.__passwd


    @property
    def auth(self):
        return (self.user, self.passwd)


    @property
    def curl(self):
        return self.__curl


    def set_prompt(self):
        self.prompt = self.baseurl + "> "
 

    @property
    def banner(self):
        return "Hadoop Shell"

    def do_help(self, data):
        print
        print self.banner
        print
        print "Commands:"
        for cmditem in self.commands:
            if isinstance(cmditem, CmdTuple):
                print "    %-20s%-25s- %-s" % cmditem
            else:
                print

    do_h = do_help


    def do_echo(self, data):
        if isinstance(data, dict):
            print json.dumps(data, indent=4)
        else:
            print data


    def do_curl(self, data):
        """ Set or Display the curl option """
        if data.upper()  == "ON":
            self.__curl = True
        elif data.upper() == "OFF":
            self.__curl = False
        else:
            print "curl is %s" % ("ON" if self.__curl else "OFF")


    def do_prefix(self, data):
        if data:
            self.__prefix = data
            self.set_prompt()


    def do_host(self, data):
        if data:
            self.__host = data
            self.set_prompt()


    def do_port(self, data):
        if data:
            self.__port = int(data)
            self.set_prompt()


    def do_user(self, data):
        if data:
            self.__user = data


    def do_passwd(self, data):
        if data:
            self.__passwd = data


    def do_whoami(self, data):
        print self.user


    def do_EOF(self, data):
        print "quit"
        return True


    def do_exit(self, data):
        return True

    def do_quit(self, data):
        return True

    do_q = do_quit


    def postcmd(self, stop, line):
       return stop


    def postloop(self):
        print "Exit..."



#
# ---- main ----
if __name__ == "__main__":
    print "HadoopUtil"
