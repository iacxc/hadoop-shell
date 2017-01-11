from __future__ import print_function

#exports
__all__ = (
    "RestServer",
    "STATUS_OK",
    "STATUS_CREATED",
    "STATUS_NOCONTENT", )

import json
import re
import requests
import sys

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

STATUS_OK = requests.codes.ok  # 200
STATUS_CREATED = requests.codes.created  # 201
STATUS_NOCONTENT = requests.codes.no_content  # 204
STATUS_UNAUTHORIZED = requests.codes.unauthorized  # 401
STATUS_FORBIDDEN = requests.codes.forbidden  # 403
STATUS_NOTFOUND = requests.codes.not_found  # 404
STATUS_NOTALLOW = requests.codes.not_allowed  # 405


def Request(method,
            url,
            user=None,
            auth=None,
            params=None,
            data=None,
            headers=None,
            proxies=None,
            curl=False,
            text=False,
            expected=(STATUS_OK, )):
    if params is None:
        params = {}
    else:
        if isinstance(params, str):
            params = dict(p.split("=") for p in params.split("&"))

    if user:
        params["user.name"] = user

    paramstr = "&".join("%s=%s" % (k, v) for k, v in params.items())

    from urlparse import urlparse
    uri = urlparse(url)

    if len(params) > 0:
        url = url + ("&" if len(uri.query) > 0 else "?") + paramstr

    if curl:
        print("curl -X {method}{auth}{header}{data}{url}".format(
            method=method,
            auth="" if auth is None else " -u '%s:%s'" % (auth),
            header="" if headers is None else " -H '" + ",".join("%s:%s" % (
                k, v) for k, v in headers.items()) + "'",
            data="" if data is None else " -d '%s'" % data,
            url=(" -k '%s'" if url.startswith("https") else " '%s'") % url))

    resp = requests.request(
        method,
        url,
        auth=auth,
        verify=False,
        data=data,
        headers=headers,
        proxies=proxies)
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
            return {
                "status": "Format error",
                "error": str(e),
                "text": resp.text
            }


class RestServer(object):
    """ base class for all rest client """

    def __init__(self,
                 prefix="http",
                 host="localhost",
                 port=8080,
                 user=None,
                 passwd=None):
        self.prefix = prefix
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd

        self.proxies = {'http': None, 'https': None}
        self.curl = False

    @property
    def auth(self):
        return (self.user, self.passwd)

    @property
    def baseurl(self):
        return "%s://%s:%d" % (self.prefix, self.host, self.port)

    @property
    def proxy(self):
        return json.dumps(self.proxies)

    def Request(self, method, url, **kwargs):
        return Request(
            method, url, curl=self.curl, proxies=self.proxies, **kwargs)

    def Get(self, url, **kwargs):
        return self.Request("GET", url, **kwargs)

    def Put(self, url, **kwargs):
        return self.Request("PUT", url, **kwargs)

    def Post(self, url, **kwargs):
        return self.Request("POST", url, **kwargs)

    def Delete(self, url, **kwargs):
        return self.Request("DELETE", url, **kwargs)

# operations
    def do_prefix(self, data):
        self.prefix = data

    def do_host(self, data):
        self.host = data

    def do_port(self, data):
        self.port = data

    def do_proxy(self, data):
        prefix, url = re.split(r"\s+", data)
        self.proxies[prefix] = url

    def do_user(self, data):
        self.user = data

    def do_passwd(self):
        import getpass
        try:
            self.passwd = getpass.getpass()
        except EOFError:
            pass

    def do_get(self, data):
        self.Get(data)

    def do_curl(self, data):
        self.curl = data.upper() == "ON"


if __name__ == "__main__":
    server = RestServer()
