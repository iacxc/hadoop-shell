
from __future__ import print_function


#exports
__all__ = ("HadoopShell", 
           )


from cmd import Cmd
import json
import os
import sys

from RestServer import RestServer

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



class HadoopCmd(object):
    def __init__(self, text="", description="", parameters=None):
        self.text = text
        self.description = description
        self.parameters = parameters

    def match(self, cmdtext):
        return self.text == cmdtext

    def __repr__(self):
        return "%-20s%s" % (self.text, self.description)

    def fulltext(self):
        if self.parameters is None:
            return "%s\n    %s" % (self.text, self.description)
        elif isinstance(self.parameters, list):
            return "%s %s\n    %s" % (self.text,
                                      " ".join(self.parameters), 
                                      self.description)
        else:
            return "%s %s\n    %s" % (self.text, 
                                      self.parameters, 
                                      self.description)

class Seperator(HadoopCmd):
    def __init__(self):
        super(HadoopCmd, self).__init__()

    def match(self, cmdtext):
        return False

    def __repr__(self):
        return ""


class HadoopShell(Cmd, object):
    """ base class for a bunch of hadoop utilities """
    commands = [
        HadoopCmd("help", "Show this help page"),
        HadoopCmd("echo", "Echo the message"),
        HadoopCmd("curl", "Get/Set curl option", "[on|off]"),
        HadoopCmd("whoami", "Display the current user"),
        HadoopCmd("user", "Set the currentuser", "[username]"),
        HadoopCmd("prefix", "Set the prefix", "<prefix>"),
        HadoopCmd("host", "Set the host", "<host>"),
        HadoopCmd("port", "Set the port", "<port>"),
        HadoopCmd("proxy", "Get/Set the proxy", "[prefix url]"),
        HadoopCmd("passwd", "Set the password"),
        HadoopCmd("get", "Get the url", "<url>"),
        HadoopCmd("quit", "Exit the program"),
        HadoopCmd("exit", "Exit the program"),
        Seperator(),
    ]

    def __init__(self, debug=True):
        super(HadoopShell, self).__init__()

        self.server = None
        self.__debug = debug

    @property
    def banner(self):
        return "Hadoop Shell"
 
    def set_prompt(self):
        self.prompt = self.server.baseurl + "> "

    def error(self, msg):
        self.do_echo(msg)
        self.do_echo("Please press h(elp) for more details")

    def debug(self, msg):
        if self.__debug:
            self.do_echo(msg)

    def do_echo(self, data=""):
        if type(data).__name__  == 'dict':
            print(json.dumps(data, indent=4))
        elif type(data).__name__ in ('list', 'tuple'):
            print_list(*data)
        else:
            print(data)

    def do_help(self, data):
        if data:
            for command in self.commands:
                if command.match(data):
                    self.do_echo(command.fulltext())
                    return
            self.do_echo("Invalid command '%s'" % data)
            return

        self.do_echo()
        self.do_echo(self.banner)
        self.do_echo()
        self.do_echo("Commands:")
        for command in self.commands:
            self.do_echo(command)

    do_h = do_help

    def do_EOF(self, data):
        self.do_echo("quit")
        return True

    def do_exit(self, data):
        return True

    def do_quit(self, data):
        return True

    do_q = do_quit

    def do_curl(self, data):
        """ Set or Display the curl option """
        if data:
            self.server.do_curl(data)
        else:
            self.do_echo("curl is %s" % ("ON" if self.server.curl else "OFF"))

    def do_whoami(self, data):
        self.do_echo(self.server.user)
 
    def do_prefix(self, data):
        if data:
            self.server.do_prefix(data)
            self.set_prompt()

    def do_host(self, data):
        if data:
            self.server.do_host(data)
            self.set_prompt()

    def do_port(self, data):
        if data:
            self.server.do_port(int(data))
            self.set_prompt()

    def do_proxy(self, data):
        if data:
            self.server.do_proxy(data)
        else:
            self.do_echo(self.server.proxy)

    def do_user(self, data):
        if data:
            self.server.do_user(data)

    def do_passwd(self, data):
        self.server.do_passwd()

    def do_get(self, data):
        self.do_echo(self.server.Get(data))


    def postcmd(self, stop, line):
       return stop

    def postloop(self):
        self.do_echo("Exit...")

    def cmdloop(self):
        assert self.server is not None

        self.do_echo("Welcome to the %s" % self.banner)
        while True:
            try:
                super(HadoopShell, self).cmdloop()
                sys.exit(0)
            except KeyboardInterrupt:
                self.do_echo("\nControl-C")
                sys.exit(1)
            except Exception as e:
                self.do_echo(e)

    def get_parser(self):
        from optparse import OptionParser

        parser = OptionParser()
        parser.add_option("--prefix", default="http",
                          help="Prefix, [default: %default]")
        parser.add_option("--host", default="localhost",
                          help="Host, [default: %default]")
        parser.add_option("--port", type=int, default=8080,
                          help="Port, [default: %default]")
        parser.add_option("-u", "--user", default=os.getenv("USER"),
                          help="User, [default: %default]")

        return parser

    def start(self, Server):
        parser = self.get_parser()
        opts, args = parser.parse_args()

        if __debug__:
            print(opts, args)

        if opts.host is None:
            if len(args) > 0:
                opts.host = args[0]
            else:
                opts.host = 'localhost'

        self.server = Server(opts)
        self.set_prompt()
        self.cmdloop()


# ---- main ----
if __name__ == "__main__":
    shell = HadoopShell()
    shell.start(RestServer)
