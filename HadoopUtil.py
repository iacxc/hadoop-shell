
from __future__ import print_function


#exports
__all__ = ("HadoopUtil", "Request",
           "gen_fileinfo",
           "STATUS_OK",
           "STATUS_CREATED",
           "STATUS_NOCONTENT",
           )


from collections import namedtuple
from cmd import Cmd
import json
import requests
import sys

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


STATUS_OK = requests.codes.ok                         # 200
STATUS_CREATED = requests.codes.created               # 201
STATUS_NOCONTENT = requests.codes.no_content          # 204
STATUS_UNAUTHORIZED = requests.codes.unauthorized     # 401
STATUS_FORBIDDEN = requests.codes.forbidden           # 403
STATUS_NOTFOUND = requests.codes.not_found            # 404
STATUS_NOTALLOW = requests.codes.not_allowed          # 405


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
           datetime.fromtimestamp(timestamp * 0.001).strftime("%Y-%m-%d %H:%M")

    return "%s%-10s  %-10s %-10s %8s %s %s" % (
         'd' if fs.get("type", "FILE") == "DIRECTORY" else '-',
         getpermission(fs.get("permission", "000")),
         fs.get("owner", "<no user>"),
         fs.get("group", "<no group>"),
         fs.get("length", 0),
         ts2str(fs.get("modificationTime", 0)),
         fs.get("pathSuffix", "<no name>"))


def Request(method, url, user=None, auth=None, params=None, 
                         data=None, headers=None, proxies=None,
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

    if len(params) > 0:
        url = url + ("&" if len(uri.query) > 0 else "?") + paramstr

    if curl:
        print("curl -X {method}{auth}{header}{data}{url}".format(method=method,
               auth="" if auth is None else " -u '%s:%s'" % (auth),
               header="" if headers is None else " -H '" + ",".join(
                       "%s:%s" % (k,v) for k,v in headers.items()) + "'",
               data="" if data is None else " -d '%s'" % data,
               url=(" -k '%s'" if url.startswith("https") else " '%s'") % url))

    resp = requests.request(method, url, auth=auth, verify=False, 
                            data=data, headers=headers, proxies=proxies)
    try:
        if resp.status_code in expected:
            return resp.text if text else resp.json()
        elif resp.status_code == STATUS_CREATED:
            return {"status": "created"}
        elif resp.status_code == STATUS_UNAUTHORIZED:
            return {"status": "Unauthorized"}
        elif resp.status_code == STATUS_FORBIDDEN:
            return {"status": "Forbidden"}
        elif resp.status_code == STATUS_NOTFOUND:
            return {"status": "Not Found"}
        elif resp.status_code == STATUS_NOTALLOW:
            return {"status": "Not Allowed"}
        else:
            return resp.json()
    except ValueError as e:
        print(resp.status_code)
        if resp.text:
            return {"status": "Format error",
                   "error": str(e),
                   "text": resp.text}

def print_list(titles, lines):
    """ print a list """
    sizes = [len(t) for t in titles]

    for line in lines:
        for i, s in enumerate(sizes):
            sizes[i] = max(sizes[i], len(str(line[i])))

    formats = []
    seperators = []
    for s in sizes:
        formats.append("%%-%ds" % s)
        seperators.append("-" * s)

    formatstr = "|" + "|".join(formats) + "|"
    seperator = "+" + "+".join(seperators) + "+"

    print(seperator)
    print(formatstr % titles)
    print(seperator)
    for line in lines:
        print(formatstr % line)

    print(seperator)


CmdTuple = namedtuple("Command", ["caption", "help"])
class HadoopUtil(Cmd, object):
    """ base class for a bunch of hadoop utilities """
    commands = [
        CmdTuple("help",                "Show this help page"),
        CmdTuple("echo",                "Echo the message"),
        CmdTuple("curl [on|off]",       "Display or set curl option"),
        CmdTuple("whoami",              "Display the current user"),
        CmdTuple("prefix [prefix>",     "Set the prefix"),
        CmdTuple("host [host]",         "Set the host"),
        CmdTuple("port [port]",         "Set the port"),
        CmdTuple("proxy [proxy]",       "Get/Set the proxy"),
        CmdTuple("user [username]",     "Set the currentuser"),
        CmdTuple("passwd",              "Set the password"),
        CmdTuple("get <url>",           "Get the url"),
        CmdTuple("quit or exit or ^D",  "Exit the program") ]

    def __init__(self, prefix, host, port, user, passwd=None, debug=True):
        super(HadoopUtil, self).__init__()

        self.__prefix = prefix
        self.__host = host
        self.__port = port
        self.__user = user
        self.__passwd = passwd
        self.__debug = debug

        self.proxies = {'http': None, 'https': None}
        self.curl = False

        self.set_prompt()

    @property
    def banner(self):
        return "Hadoop Shell"
 
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

    def Get(self, url, **kwargs):
        result = Request("GET", url, curl=self.curl, proxies=self.proxies,
                         **kwargs)
        return result

    def Put(self, url, **kwargs):
        result = Request("PUT", url, curl=self.curl, proxies=self.proxies,
                         **kwargs)
        return result

    def Post(self, url, **kwargs):
        result = Request("POST", url, curl=self.curl, proxies=self.proxies,
                         **kwargs)
        return result

    def Delete(self, url, **kwargs):
        result = Request("DELETE", url, curl=self.curl, proxies=self.proxies,
                         **kwargs)
        return result

    def set_prompt(self):
        self.prompt = self.baseurl + "> "

    def error(self, msg):
        self.do_echo(msg)
        self.do_echo("Please press h(elp) for more details")

    def debug(self, msg):
        if self.__debug:
            self.do_echo(msg)

    def do_echo(self, data=''):
        if type(data).__name__  == 'dict':
            print(json.dumps(data, indent=4))
        elif type(data).__name__ in ('list', 'tuple'):
            print_list(*data)
        else:
            print(data)

    def do_help(self, data):
        self.do_echo()
        self.do_echo(self.banner)
        self.do_echo()
        self.do_echo("Commands:")
        for item in self.commands:
            if isinstance(item, CmdTuple):
                self.do_echo("    %-45s\n                     - %-s" % item)
            else:
                self.do_echo()

    do_h = do_help

    def do_curl(self, data):
        """ Set or Display the curl option """
        if data.upper()  == "ON":
            self.curl = True
        elif data.upper() == "OFF":
            self.curl = False
        else:
            self.do_echo("curl is %s" % ("ON" if self.curl else "OFF"))

    def do_prefix(self, data):
        if data:
            self.prefix = data
            self.set_prompt()

    def do_host(self, data):
        if data:
            self.__host = data
            self.set_prompt()

    def do_port(self, data):
        if data:
            self.__port = int(data)
            self.set_prompt()

    def do_proxy(self, data):
        if data:
            self.proxies = json.loads(data)
        else:
            self.do_echo(self.proxies)

    def do_user(self, data):
        if data:
            self.__user = data

    def do_passwd(self, data):
        import getpass
        try:
            self.__passwd = getpass.getpass()
        except EOFError:
            pass

    def do_get(self, data):
        self.do_echo(self.Get(data))

    def do_whoami(self, data):
        self.do_echo(self.user)

    def do_EOF(self, data):
        self.do_echo("quit")
        return True

    def do_exit(self, data):
        return True

    def do_quit(self, data):
        return True

    do_q = do_quit

    def postcmd(self, stop, line):
       return stop

    def postloop(self):
        self.do_echo("Exit...")

    def cmdloop(self):
        self.do_echo("Welcome to the %s" % self.banner)
        while True:
            try:
                super(HadoopUtil, self).cmdloop()
                sys.exit(0)
            except KeyboardInterrupt:
                self.do_echo("\nControl-C")
                sys.exit(1)
            except Exception as e:
                self.do_echo(e)

#
# ---- main ----
if __name__ == "__main__":
    print("HadoopUtil")
