#encoding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import time
import redis
from base.clog import getLogger
logger = getLogger(__name__)
class CRedis:
    def __init__(self, host, port, decode_responses):
        mpool = redis.ConnectionPool(host = host, port = port, decode_responses = decode_responses)
        self.client = redis.StrictRedis(connection_pool = mpool)

    def execute_command(self, name, *args, **kwargs):
        try:
            if name == 'exists':
                return self.client.exists(*args, **kwargs)
            elif name == 'smembers':
                return self.client.smembers(*args, **kwargs)
            elif name == 'sadd':
                return self.client.sadd(*args, **kwargs)
            elif name == 'srem':
                return self.client.srem(*args, **kwargs)
            elif name == 'delete':
                return self.client.delete(*args, **kwargs)
            elif name == 'get':
                return self.client.get(*args, **kwargs)
            elif name == 'set':
                return self.client.set(*args, **kwargs)
            else:
                raise Exception("not supported method for redis client")
        except Exception as e:
            self.wait_redis_available()
            if name == 'smembers':
                return set()
            elif name == 'exists' or name == 'set':
                return False
            elif name == 'sadd' or name == 'srem' or name == 'delete':
                return 0
            elif name == 'get':
                return None
            else:
                return None

    def wait_redis_available(self, retry_times = 3):
        for i in range(retry_times):
            try:
                self.client.get(None)
                return
            except (redis.exceptions.ConnectionError, redis.exceptions.BusyLoadingError) as e:
                logger.debug(e)
            except Exception as e:
                logger.debug(e)
            time.sleep(i + 1)

    def exists(self, *args, **kwargs):
        return self.execute_command('exists', *args, **kwargs)

    def smembers(self, *args, **kwargs):
        return self.execute_command('smembers', *args, **kwargs)

    def sadd(self, *args, **kwargs):
        return self.execute_command('sadd', *args, **kwargs)

    def srem(self, *args, **kwargs):
        return self.execute_command('srem', *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self.execute_command('delete', *args, **kwargs)

    def get(self, *args, **kwargs):
        return self.execute_command('get', *args, **kwargs)

    def set(self, *args, **kwargs):
        return self.execute_command('set', *args, **kwargs)

if __name__ == '__main__':
    client = CRedis('127.0.0.1', 6379, False)
    test_key = 'ABCDEFG'
    db = 'test'
    db1 = 'test1'
    print(client.exists(test_key))
    print(client.sadd(test_key, db))
    print(client.exists(test_key))
    print(client.smembers(test_key))
    print(client.srem(test_key, db))
    print(client.sadd(test_key, db))
    print(client.set(test_key, db1))
    print(client.get(test_key))
    print(client.delete(test_key))
    print(client.exists(test_key))
