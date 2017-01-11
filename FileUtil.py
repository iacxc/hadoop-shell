

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


def fileinfo(fs):
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


