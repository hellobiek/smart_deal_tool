# encoding=utf-8
import sys,os,logging
class InfoFilter(logging.Filter):
    def __init__(self, level):
        self.level = level
    def filter(self, record):
        return record.levelno == self.level

def init_name(name):
    if name == "__main__": name = os.path.split(os.path.abspath(sys.argv[0]))[1]
    if name.find("/") != -1: name = name.split("/")[-1]
    return name

def getLogger(name, log_dir = '/tmp'):
    if not os.path.exists(log_dir): os.makedirs(log_dir)
    name = init_name(name)
    # 文件的命名
    logname = os.path.join(log_dir, '%s.log' % name)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = 0
    # 日志输出格式
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(module)s.%(funcName)s:%(lineno)s - %(levelname)s - %(message)s")
    if len(logger.handlers) != 4:
        # 为了避免日志输出重复问题
        logger.handlers = []
        # 创建一个FileHandler，用于写到本地
        fh = logging.FileHandler(logname, 'a', encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        ## 关闭打开的文件
        #fh.close()//频繁打开和关闭文件，太耗时，这里会导致，程序已知打开文件的问题。

        # 创建一个StreamHandler,用于将Info输出到stdout
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        ch.addFilter(InfoFilter(logging.INFO))
        logger.addHandler(ch)

        # 创建一个StreamHandler,用于将Warning输出到stderr
        ch = logging.StreamHandler()
        ch.setLevel(logging.WARNING)
        ch.setFormatter(formatter)
        ch.addFilter(InfoFilter(logging.WARNING))
        logger.addHandler(ch)

        # 创建一个StreamHandler,用于将ERROR输出到stderr
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        ch.setFormatter(formatter)
        ch.addFilter(InfoFilter(logging.ERROR))
        logger.addHandler(ch)
    return logger

if __name__ == "__main__":
    logger = getLogger(__name__)
    logger.debug("debug...")
    logger.info("info...")
    logger.error("this is error")
