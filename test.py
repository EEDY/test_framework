
import sys
import logging
import logging.config

from multiprocessing import Process, Queue
from datetime import datetime
import time
import random
import signal
import os
import platform
import re
import subprocess
import copy


from os.path import abspath, dirname
__currdir = dirname(abspath(__file__))
sys.path.append("%s/lib" % (__currdir))

from common import *
from common.threadpool import *


TEST_HOME = os.environ['HOME'] + "/log/"


logging.config.fileConfig("./conf/logging.conf")
logger = logging.getLogger(__name__)


def get_option(usage, version):
    from optparse import OptionParser

    parser = OptionParser(usage=usage, version=version)

    parser.add_option("-n", "--nodes", dest="nodes", default="nodes.conf",
                      help="a file contains all hostnames of available nodes to generate data, default is 'nodes.conf'")
    parser.add_option("-d", "--directory", dest="dirs", default="dirs.conf",
                      help="a file contains directories where generate data locates, default is 'dirs.conf'")
    parser.add_option("-p", "--parallel", dest="parallel", type="int", default=1,
                      help="total generation parallel number")
    parser.add_option("-e", "--excel-ddl", dest="excel", default=None,
                      help="input DDL excel file")
    parser.add_option("-c", "--row-count", dest="rcount", type="int", default=None,
                      help="total Row count for the given table")
    parser.add_option("--delimiter", dest="deli", default='|', 
                      help="set output field separator")
    '''parser.add_option("-H", "--hdfs-dir", dest="hdfs", default=None,
                      help="a file contains destination hdfs directories")'''
    parser.add_option("-s", "--seed", dest="seed", default=20161111,
                      help="seed for random, default is 20161111")

    options, args = parser.parse_args()

    return (options, args)


def cluster_scp(src_path, nodes, target_path):
  """ Copy source file or directory specified by src_path to the target_path on all nodes """
  for node in nodes:
    logger.info("Copying " + src_path + " to " + target_path)
    cmd = "mkdir -p " + target_path
    run_linux_cmd(cmd, node)
    cmd = "scp -rp " + src_path + " " + node + ":" + target_path
    run_linux_cmd(cmd)


def run_linux_cmd(cmd, node=None, info=False, user=None):
  """ Run any linux build-in cmd or executable on local node or specified remote node """
  if cmd is None:
    logger.error("You must specify a command to run")

  if user is not None:
    cmd = " su - " + user + " -c " + cmd 
  if node is not None:
    cmd = "ssh -q -n " + node + " '" + cmd + "'"

  logger.debug("Run external command: " + cmd)
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

  output = p.stdout.readlines()

  retval = p.wait()
  if retval != 0:
    logger.error("*** ERROR *** Cmd run into error: " + cmd)
    logger.error("*** ERROR *** Return Value: %d" % retval)
    logger.error("detail error: %s" % ('\n'.join(output)))
  elif info is True:
    logger.info("*** run_linux_cmd *** Cmd executed successfully: " + cmd)

  return (retval, output)


def main():
    """ Main function """
    global options, args
    global data_dir_list, data_dir_num, is_hdfs
    global sheet_name
    global delimiter
    options, args = get_option("%prog [Options]", "%prog 1.0")

    """ Your scripts """
    nodes = read_file(options.nodes)
    logger.info(nodes)
    
    fail, output = run_linux_cmd("sqcheck", nodes[1], user="trafodion")

    return


if __name__ == "__main__":

  main()
