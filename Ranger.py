
from __future__ import print_function

#exports
__all__ = ('Ranger',)

import json
from Common import get_input
from HadoopUtil import HadoopUtil, CmdTuple, STATUS_NOCONTENT


class Ranger(HadoopUtil):
    rootpath = "/service/public/api"
    commands = HadoopUtil.commands + [
        "",
        CmdTuple("list repository",                 "List all repositories"),
        CmdTuple("list policy",                     "List all policies"),
        CmdTuple("list repository   <id>",          "Show a repository"),
        CmdTuple("list policy       <id>",          "Show a policy"),
        CmdTuple("create repository <data>",        "Create a repository"),
        CmdTuple("create policy     <data>",        "Create a policy"),
        CmdTuple("update repository <id> <data>",   "Update a repository"),
        CmdTuple("update policy     <id> <data>",   "Update a policy"),
        CmdTuple("delete repository <id>",          "Delete a repository"),
        CmdTuple("delete policy     <id>",          "Delete a policy"),
        ]

    def __init__(self, host, user, passwd):
        HadoopUtil.__init__(self, "http", host, 6080, user, passwd)

    @property
    def banner(self):
        return "Ranger Shell"

    @property
    def weburl(self):
        return self.baseurl + self.rootpath

    @property
    def repourl(self):
        return self.weburl + "/repository"

    @property
    def policyurl(self):
        return self.weburl + "/policy"

    def Get(self, url):
        return super(Ranger, self).Get(url, auth=self.auth)

    def Delete(self, url):
        return super(Ranger, self).Delete(url, text=True, expected=(STATUS_NOCONTENT,))

    def Post(self, url, data):
        return super(Ranger, self).Post(url, data=data,
                       headers={"Content-Type": "Application/json"})

    def Put(self, url, auth, data, curl=False):
        return super(Ranger, self).Put(url, data=data,
                       headers={"Content-Type": "Application/json"})

    @staticmethod
    def get_permission_map(*perms):
        to_list = lambda s: s.replace(",", " ").split()

        print("Please input a permission map:")

        permlist = "/".join(perms)
        perm_map ={"userList" : get_input("User(s)",
                                          hint="comma seperated list",
                                          default="",
                                          convert=to_list),
                   "groupList" : get_input("Group(s)",
                                           hint="comma seperated list",
                                           default="",
                                           convert=to_list),
                   "permList": get_input("Permissions",
                                         hint="comma seperated list of %s" % permlist,
                                         default="",
                                         convert=to_list)
                   }

        return perm_map

# command handers
    def do_list(self, data):
        params = data.split() + [None]
        if params[0] == 'repository':
            self.do_echo(self.get_repository(params[1]))
        elif params[0] == 'policy':
            self.do_echo(self.get_policy(params[1]))
        else:
            self.error("Invalid parameter '%s'" % data)

    def do_create(self, data):
        params = data.split() + [None]

        if not (params[0] in ("repository", "policy")):
            self.error("Invalid parameter '%s'" % params[0])
            return

        try:
            handler_name = "create_%s_%s" % (params[1], params[0])
            handler = getattr(self, handler_name)
            handler()
        except AttributeError:
            self.error("Invalid parameter '%s'%" % params[1])

    def do_update(self, data):
        params = data.split() + [None]
        if not (params[0] in ("repository", "policy")):
            self.error("Invalid parameter '%s'" % params[0])
            return

        if params[1] is not None:
            fetcher = getattr(self, "get_" + params[0])
            olddata = fetcher(params[1])

            if str(olddata.get("id", None)) != params[1]:
                self.error("Invalid repository/policy id: '%s'" % params[1])
                return

            update = {"repository" : self.update_repository,
                      "policy" : self.update_policy
                      }.get(params[0])
            update(olddata)

    def do_delete(self, data):
        params = data.split() + [None]

        if params[0] == 'repository':
            self.do_echo(self.delete_repository(params[1]))
        elif params[0] == 'policy':
            self.do_echo(self.delete_policy(params[1]))
        else:
            self.error("Invalid parameter '%s'" % params[0])

# utilities
    def get_repository(self, service_id=None):
        url = self.repourl if service_id is None \
                           else "%s/%s" % (self.repourl, service_id)

        return self.Get(url)

    def get_policy(self, policy_id=None):
        url = self.policyurl if policy_id in (None, "None") \
                             else "%s/%s" % (self.policyurl, policy_id)

        return self.Get(url)


    def delete_repository(self, repo_id):
        return self.Delete("%s/%s" % (self.repourl, repo_id))

    def delete_policy(self, policy_id):
        return self.Delete("%s/%s" % (self.policyurl, policy_id))


    def update_repository(self, repo_data):
        print(repo_data)


    def update_policy(self, policy_data):
        print(policy_data)


    def create_hbase_policy(self):
        print("Creating hbase policy, please input")

        def trueOrfalse(input_str):
            if input_str.lower() in ("yes", "y"):
                return True
            else:
                return False

        try:
            policy_data = {
                "repositoryType" : "hbase",
                "repositoryName" : get_input("Repository name"),
                "policyName" : get_input("Policy name"),
                "tables" : get_input("Table name(s)",
                                     hint="comma seperated list",
                                     default="*"),
                "columnFamilies" : get_input("Column familie(s)",
                                             hint="comma seperated list",
                                             default="*"),
                "columns" : get_input("Column(s)",
                                      hint="comma seperated list",
                                      default="*"),
                "description": get_input("Description", default=""),
                "isEnabled": get_input("Enabled?", hint="Yes/No",
                                       default="Yes", convert=trueOrfalse),
                "isAuditEnabled":get_input("Auditable?", hint="Yes/No",
                                       default="Yes", convert=trueOrfalse),
                "isRecursive": get_input("Recursive?", hint="Yes/No",
                                       default="No", convert=trueOrfalse),

                "tableType": "Inclusion",
                "columnType": "Inclusion",
                }

            permmap_list = []
            while True:
                perm_map = self.get_permission_map("Read", "Write",
                                                  "Create", "Admin")

                if len(perm_map["permList"]) == 0:
                    break

                permmap_list.append(perm_map)

            policy_data["permMapList"] = permmap_list

            self.do_echo( self.__create_policy(policy_data))

        except KeyboardInterrupt:
            print("Control-C")

        except Exception as e:
            print(e)


    def __create_policy(self, policy_data):
        return self.Post(self.policyurl, data=json.dumps(policy_data))

    def __update_policy(self, policy_id, policy_data):
        return self.Put("%s/%s" % (self.policyurl, policy_id),
                          data=json.dumps(policy_data))

#
# ---- main ----
if __name__ == "__main__":
    print("Ranger")
