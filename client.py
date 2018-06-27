import gear
import json
import const as ct
from log import getLogger
logger = getLogger(__name__)
# connect to gearmand
client = gear.Client()
client.addServer(host='gearmand-container', port=4730)
client.waitForServer(timeout=0.5)
# prepare job data
data = {
    'code': "a3",
    'name': "a1",
    'industry': "a2",
    'area': "a4",
    'pe': 16
}
logger.info("AAAAAAAAAAAAAAAA1")
# gearman communicates using bytes
job_data = json.dumps(data).encode(encoding="utf-8")
logger.info("AAAAAAAAAAAAAAAA2")
# submit job to queue pipe (e.g. divider)
job = gear.Job(ct.SYNCSTOCK2REDIS, job_data)
client.submitJob(job)
