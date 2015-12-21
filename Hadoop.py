"""
   provide some utitlies using 'hadoop/hdfs' cmdline tool
"""

__all__ = ("get_distro",
           "disk_usage",
           "list_files",
           )

import Common
import glob


HADOOPDISTRO = Common.make_enum( "HADOOPDISTRO",
        HORTONWORK     = "horton",
        CLOUDERA       = "cloudera",
        MAPR           = "mapr",
        APACHE         = "apache",
)


def get_distro():
    """ find the distribution of hadoop """

    dirs_to_search = {HADOOPDISTRO.HORTONWORK : "/var/run/ambari-*",
                      HADOOPDISTRO.CLOUDERA : "/var/run/cloudera-*",
                      HADOOPDISTRO.MAPR : "/etc/rc*/init*/*mapr-warden*"}

    for distro, path in dirs_to_search.items():
        if glob.glob(path):
            return distro
    else:
        return HADOOPDISTRO.APACHE


def disk_usage(hdfs_path):
    """ return a map of file name to size, using hdfs command line """
    cmdstr = " ".join(["hdfs", "fs", "-du", hdfs_path])
    retcode, output = Common.run_cmd(cmdstr)

    if retcode == Common.SUCCESS:
        usage = {}
        for line in output.split("\n"):
            if len(line.strip()) == 0:
                continue
            items = line.strip().split()
            usage[items[-1]] = int(items[0])

        return usage
    else:
        return {}


def list_files(hdfs_path, recursive=False):
    """ return a list of file objects , using hdfs command line"""
    ls_opt = "-R" if recursive else ""

    cmdstr = " ".join(["hdfs", "fs", "-ls", ls_opt, hdfs_path])

    retcode, output = Common.run_cmd(cmdstr)

    if retcode == Common.SUCCESS:
        if len(output.strip()) == 0:
            return []

        files = []
        for line in output.split("\n"):
            if len(line.strip()) == 0:  # empty line
                continue

            fields = line.strip().split()
            if len(fields) < 4:  # title line: Found XXX rows
                continue

            a_file = {"name" : fields[-1],
                      "mode" : fields[0],
                      "size" : int(fields[4]),
                      "owner": fields[2],
                      "group": fields[3]}
            files.append(a_file)

        return files
    else:
        return []


