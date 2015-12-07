

#exports
__all__ = ('Ranger',)

import json
from HadoopUtil import HadoopUtil, Request, CmdTuple, \
                       STATUS_NOCONTENT 

class Ranger(HadoopUtil):
    rootpath = "/service/public/api"
    commands = HadoopUtil.commands + [
        "",
        CmdTuple("list repository",    "",             "List all repositories"),
        CmdTuple("list policy",        "",             "List all policies"),
        CmdTuple("show repository",    "<id>",         "Show a repository"),
        CmdTuple("show policy",        "<id>",         "Show a policy"),
        CmdTuple("create repository", "<data>",        "Create a repository"),
        CmdTuple("create policy",     "<data>",        "Create a policy"),
        CmdTuple("update repository", "<id> <data>",   "Update a repository"),
        CmdTuple("update policy",     "<id> <data>",   "Update a policy"),
        CmdTuple("delete repository", "<id>",          "Delete a repository"),
        CmdTuple("delete policy",     "<id>",          "Delete a policy"),
        ]

    def __init__(self, host, user, passwd):
        HadoopUtil.__init__(self, "http", host, 6080, user, passwd)

        self.weburl = self.baseurl + self.rootpath
        self.repourl = self.weburl + "/repository"
        self.policyurl = self.weburl + "/policy"


    @staticmethod
    def Get(url, auth, params=None, curl=False):
        return Request("GET", url, auth=auth, params=params, curl=curl)

    @staticmethod
    def Delete(url, auth, curl=False):
        return Request("DELETE", url, auth=auth, curl=curl)

    @staticmethod
    def Post(url, auth, data, curl=False):
        return Request("POST", url, auth=auth, 
                       data=data, 
                       headers={"Content-Type" : "Application/json"}, curl=curl)

    @staticmethod
    def Put(url, auth, data, curl=False):
        return Request("PUT", url, auth=auth, 
                       data=data, 
                       headers={"Content-Type" : "Application/json"}, curl=curl)

    @property
    def banner(self):
        return "Ranger Shell"


    def do_list(self, data):
        if data == 'repository':
            self.do_echo(self.get_repository())
        elif data == 'policy':
            self.do_echo(self.get_policy())
        else:
            print "Invalid parameter '%s'" % data


    def do_show(self, data):
        params = data.split()
        if len(params) < 2:
            print "Not enough parameters"
            return 

        if params[0] == 'repository':
            self.do_echo(self.get_repository(params[1]))
        elif params[0] == 'policy':
            self.do_echo(self.get_policy(params[1]))
        else:
            print "Invalid parameter '%s'" % params[0]


    def do_create(self, data):
        params = data.split()
        if len(params) < 2:
            print "Not enough parameters"
            return 

        try:
            postdata = " ".join(params[1:])
            if params[0] == 'repository':
                print "Create repository", json.loads(postdata)
            elif params[0] == 'policy':
                print "Create policy", json.loads(postdata)
            else:
                print "Invalid parameter '%s'" % params[0]

        except TypeError as e:
             print e
        except ValueError as e:
             print e


    def do_update(self, data):
        params = data.split()
        if len(params) < 3:
            print "Not enough parameters"
            return 

        try:
            postdata = " ".join(params[2:])
            if params[0] == 'repository':
                print "Update repository " + params[1], json.loads(postdata)
            elif params[0] == 'policy':
                print "Update policy " + params[1], json.loads(postdata)
            else:
                print "Invalid parameter '%s'" % params[0]
        
        except TypeError as e:
             print e
        except ValueError as e:
             print e


    def do_delete(self, data):
        params = data.split()
        if len(params) < 2:
            print "Not enough parameters"

        if params[0] == 'repository':
            self.do_echo(self.delete_repository(params[1]))
        elif params[0] == 'policy':
            self.do_echo(self.delete_policy(params[1]))
        else:
            print "Invalid parameter '%s'" % params[0]


    def get_repository(self, service_id=None):
        url = self.repourl if service_id is None \
                           else "%s/%s" % (self.repourl, service_id)

        return Ranger.Get(url, auth=self.auth, curl=self.curl)


    def get_policy(self, policy_id=None, params=None):
        url = self.policyurl if policy_id in (None, "None") \
                             else "%s/%s" % (self.policyurl, policy_id)

        return Ranger.Get(url, auth=self.auth, params=params, curl=self.curl)


    def delete_repository(self, repo_id):
        r = Ranger.Delete("%s/%s" % (self.repourl, repo_id), 
                          auth=self.auth, 
                          curl=self.curl, expected=(STATUS_NOCONTENT,))
        if r is not None:
            return {"status": "ok"}


    def delete_policy(self, policy_id):
        r = Ranger.Delete("%s/%s" % (self.policyurl, policy_id), 
                          auth=self.auth, 
                          curl=self.curl, expected=(STATUS_NOCONTENT,))
        if r is not None:
            return {"status": "ok"}


    def create_policy(self, service_type, service_name, policy_name, 
                            policy_data):
        if isinstance(policy_data, str):
            policy_data = json.loads(policy_data)

        policy_data.update(policyName=policy_name,
                           repositoryName=service_name)
    
        return Ranger.Post(self.policyurl, 
                           auth=self.auth, 
                           data=json.dumps(policy_data),
                           curl=self.curl)


        dispatcher = getattr(self, "create_%s_policy" % service_type)
        return dispatcher(service_name, policy_name, policy_data)


    def update_policy(self, policy_id, policy_data):
        if isinstance(policy_data, str):
            policy_data = json.loads(policy_data)

        if __debug__:
            print json.dumps(policy_data, indent=4)

        return Ranger.Put("%s/%s" % (self.policyurl, policy_id), 
                           auth=self.auth, 
                           data=json.dumps(policy_data), 
                           curl=self.curl)


#
# ---- main ----
if __name__ == "__main__":
    print "Ranger"
#    import sys
#    import json
#    from optparse import OptionParser
#
#    parser = OptionParser()
#    parser.add_option("--host", default="localhost")
#    parser.add_option("--curl", action="store_true", default=False)
#    parser.add_option("-u", "--user", default="caiche")
#    parser.add_option("-p", "--password", default="caiche-password")
#
#    opts, args = parser.parse_args()
#
#    if __debug__: print opts, args
#
#    if len(args) < 1:
#        print "Missing arguments, supported methods are", Ranger.operations
#        sys.exit(1)
#
#    ranger = Ranger(opts.host, opts.user, opts.password, opts.curl)
#
#    method = args[0]
#    if not method in ranger:
#        print "Unsupported method '%s'" % method
#        sys.exit(1)
#
#    try:
#        fun = getattr(ranger, method)
#        print json.dumps(fun(*args[1:]), indent=4)
#
#    except AttributeError as e:
#        print e
#
