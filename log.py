"""
@author:
    biek <hellobiek@163.com>
@desc:
    logging wrapper. default dir: "/tmp/", DEBUG level not print in term.
"""
import sys
import os
import logging

#logging.raiseExceptions = False

class InfoFilter(logging.Filter):
    def __init__(self, level):
        self.level = level
    def filter(self, record):
        return record.levelno == self.level

def getLogger(name, save_dir="/tmp/"):
    if name == "__main__":
        name = os.path.split(os.path.abspath(sys.argv[0]))[1]
    if name.find("/") != -1:
        name = name.split("/")[-1]
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(module)s.%(funcName)s:%(lineno)s - %(levelname)s - %(message)s")
    logger    = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # All log write to file
    f = "%s/%s.log" % (save_dir, name)
    if not os.path.exists(f):
        open(f, "w").close()
        os.chmod(f, 0o777)

    # DEBUG not output
    ch = logging.FileHandler(f)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # INFO print stdout
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    ch.addFilter(InfoFilter(logging.INFO))
    logger.addHandler(ch)

    # WARNING print stderr
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.propagate = False # duplicate log
    return logger

if __name__ == "__main__":
    logger = getLogger(__name__)
    logger.info("info...")
    logger.debug("debug...")
    logger.error("this is error")
