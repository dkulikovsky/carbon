#!/usr/bin/env python
"""
Performs rollup for Ceres databases
"""
import sys, os, time, fcntl
from multiprocessing import Pool
from os.path import join, exists
from optparse import OptionParser
import logging
import time

FORMAT = '%(asctime)-15s %(message)s'

parser = OptionParser()
parser.add_option('--root', default='/var/lib/graphite/storage/ceres/',
                  help="Specify ceres-tree path (default: /var/lib/graphite/storage/ceres/)")
parser.add_option('--lock', default='/var/tmp/simple-rollup.lock',
                  help="Specify were lockfile will be created (default: /var/tmp/simple-rollup.lock)")
parser.add_option('--graphiteroot', default='/var/lib/graphite/',
                  help="Specify were graphite root is (default: /var/lib/graphite/)")
parser.add_option('--config',
                  default='/var/lib/graphite/conf/carbon-daemons/carbon-writer-st01/',
                  help="Specify were carbon config is (default: /var/lib/graphite/conf/carbon-daemons/carbon-writer-st01/)")
parser.add_option('--metric',
                  help="Specify path to the metric you want to rollup")
parser.add_option('--workers',
                  default=4,
                  help="Number of workers to run (default: 4)")
parser.add_option('-d', action="store_true", default=False, dest="debug", help="Run with verbose logging")

options, args = parser.parse_args()

lock_file = options.lock
# Make carbon imports available for some functionality
root_dir = options.graphiteroot
sys.path.append(join(root_dir, "plugins/maintenance"))
sys.path.append(join(root_dir, "lib"))

if (options.debug):
	logging.basicConfig(level=logging.DEBUG,
	                    format=FORMAT)
else:
	logging.basicConfig(level=logging.INFO,
	                    format=FORMAT)
logger = logging.getLogger("rollup-main")


try:
    from carbon.conf import settings
    from ceres import getTree
except ImportError:
    logger.fatal("Failed to import carbon, specify your installation location "
                 "with the GRAPHITE_ROOT environment variable.")
    sys.exit(1)

# set up config
settings.use_config_directory(options.config)
from rollup_simple import node_found

root = options.root

if __name__ == '__main__':
    start_time = time.time()
    lock_timeout = 60
    got_lock = False
    lock = None
    while lock_timeout:
        try:
            lock = open(lock_file, 'w')
            fcntl.lockf(lock, fcntl.LOCK_EX)
            got_lock = True
            break
        except IOError as e:
            if e.args[0] == 11:
                lock_timeout -= 1
                time.sleep(1)
            else:
                logger.fatal("Can't get lock, reason: %s", e.args[1])
                sys.exit(1)
        except Exception as e:
            logger.fatal("Failed to get lock for some unknown reason: %s", e)
            sys.exit(1)

    if not got_lock:
        logger.fatal("Failed to get lock for 60s")
        sys.exit(1)

    tree = getTree(root)
    if tree is None:
        logger.fatal("%s is not inside a CeresTree", root)
        sys.exit(1)

    logger.info("Starting rollup")
    nodes_found = 0
    exec_time = time.time()
    rollup_time = int(exec_time)
    if options.metric:
        node_found(options.metric, root, rollup_time)
    else:
        proc_pool = Pool(processes=int(options.workers))
        for current_dir, subdirs, files in os.walk(root):
            for subdir in subdirs:
                if subdir == '.ceres-tree':
                    continue
                path = join(current_dir, subdir)
                if os.listdir(path):
                    if exists(join(path, '.ceres-node')):
                        proc_pool.apply_async(node_found, (path, root, rollup_time,))
                        nodes_found += 1
        proc_pool.close()
        proc_pool.join()
        logger.info("Found %s nodes", nodes_found)
    logger.info("All work is done in %s", time.time() - start_time)
    fcntl.lockf(lock, fcntl.LOCK_UN)
