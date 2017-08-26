#!/home/tops/bin/python2.7
# -*- coding: utf-8 -*-
#(w) hellobiek@gmail.com

'''
agent for stock daemon.
'''

import os
import sys
import imp
import json
import urllib
import socket
import getopt
import httplib
import urlparse
import commands
import datetime
import curses
import traceback
from time import sleep
from dayu.include import log
from dayu.include.conf_reader import ConfReader
from dayu.include.libme import LibMe, GeneralError, get_agip, get_localip
from dayu.include.common import CalculateMd5,WriteFile
from dayu.include.check_builds_helper import CheckBuildinfoHelper   
TIMEOUT = 30
try:
    er = imp.load_source("errcode", "/home/admin/dayu/include/errcode")
except Exception, e:
    print "Error, %s" %e
    sys.exit(228) #可能import errorcode失败

def Usage(exitarg):
    print __doc__
    sys.exit(exitarg)

class RoledefErrException(Exception):
    pass

class Cadidate:
    def __init__(self, ip, local_ip, logger):
        self.hn = socket.getfqdn()
        self.ip = local_ip
        self.server = ip
        self.params = {
        'ip': self.ip,
        'hostname':self.hn
        }
        self.data = urllib.urlencode(self.params)
        self.port = '8000'
        self.uri = 'http://%s:%s' %(self.server,self.port)
        self.logger = logger

    def put(self, url, body):
        if not isinstance(body, str):
            body = json.dumps(body, indent = 2)
        conn = self.__getconn(url)
        method = 'PUT'
        conn.request(method, url, body)
        return self.__getresponse(conn, method, url)

    def get(self, url, timeout = TIMEOUT):
        conn = self.__getconn(url, timeout = timeout)
        method = 'GET'
        conn.request(method, url)
        return self.__getresponse(conn, method, url)

    def __getconn(self, url, timeout = TIMEOUT):
        (_, netloc, _, _, _, _) = urlparse.urlparse(url)
        return httplib.HTTPConnection(netloc, timeout = timeout)

    def __getresponse(self, conn, method, url):
        r = conn.getresponse()
        body = r.read()
        if r.status / 100 != 2:
            raise Exception("response failed.method:%s, url:%s, status:%s, reason:%s, body:%s" % (method, url, r.status, r.reason, body))
        if len(body) == 0:
            return {}
        return json.loads(body)

    def url_handler(self, url, method = 'get', data = None):
        try:
            if method != 'get' and method != 'put':
                self.logger.error("unsupport methods. %s" % str(method))
                sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
            self.logger.debug("request url:%s" % url)
            if 'get' == method:
                return self.get(url)
            else:
                if not data:
                    self.logger.error("put data error: %s" % str(data))
                    sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
                return self.put(url, data)
        except httplib.HTTPException, e:
            self.logger.error("Connection Failed. %s" %e)
            sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
        except Exception, e:
            self.logger.error("Connection Unexpected. %s" %e)
            sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)

    def Vaildate(self):
        try:
            url = '%s/server/verify?%s' %(self.uri,self.data)
            Ret = self.url_handler(url)
            if Ret['code']:
                self.logger.error(Ret['msg'])
                sys.exit(er.ERR_AG_VERIFY_FAILED)
            self.logger.info("Succ, %s."%Ret['msg'])
        except Exception,e:
            self.logger.error("Unexpected, %s" %e)
            sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)

    def Security(self):
        StrCmd = 'md5sum /apsara/security/key/1'
        Ret = commands.getstatusoutput(StrCmd)
        if Ret[0]:
            self.logger.error('get local md5 for security key failed.')
            sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
        localmd5 = Ret[1].split()[0].strip()
        url = '%s/server/security_md5' %self.uri
        try:
            Ret = self.url_handler(url)
            if Ret['code'] == 0:
                if localmd5 in Ret['data']:
                    self.logger.debug("Succ, %s."%Ret['msg'])
                    sys.exit(0)
                else:
                    self.logger.debug('Get remote md5 faile. %s' %Ret)
                    sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
            else:
                self.logger.debug(Ret['msg'])
                sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
        except Exception,e:
            self.logger.debug("Unexpected, %s" %e)
            sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)

    def Brother(self,action,dip=''):
        strout = ''
        try:
            if action == 'delete' and dip == '':
                self.logger.error("No IP Found In Action Delete.")
                sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
            url = {
            'get':'%s/brother/get?%s' %(self.uri,self.data),
            'add':'%s/brother/add?%s' %(self.uri,self.data),
            'free':'%s/brother/free?%s' %(self.uri,self.data), # Free只需要自己的IP
            'delete':'%s/brother/delete?ip=%s' %(self.uri,dip), # Delete 需要删除的IP
            'status':'%s/brother/status' %self.uri
            }[action]
        except KeyError:
            self.logger.error("Action %s invailed." %action)
            sys.exit(er.ERR_SCRIPT_INPUTVALUE_ERROR)
        try:
            Ret = self.url_handler(url)
            if Ret['code'] == 0:
                if action == 'get':
                    sys.stdout.write(Ret['data'][0])
                    sys.exit(0)
                elif action == 'status':
                    strout = "|%-15s|%-20s|%-20s|%-5s|\n" %("IP","LastUpdateTime","JoinInTime","Used")
                    strout += "=" * 75 + '\n'
                    for line in Ret['data']:
                        strout += "|%-15s|%-20s|%-20s|%-5s|\n" %(line['ip'],line['scantime'],line['addtime'],line['used'])
                    sys.stdout.write(strout)
                    sys.exit(0)
                else:
                    self.logger.debug("Succ, %s" %Ret['msg'])
                    print Ret['msg']
                    sys.exit(0)
            else:
                self.logger.error(Ret['msg'])
                sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
        except Exception,e:
            self.logger.error("Unexpected Action %s, %s" %(action,e))
            sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)

    def Expan(self,action,status='',iplist=''):
        try:
            if action == 'update' and status == '':
                self.logger.error('input status is null.')  
                sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
            elif action == 'status':
                if not os.path.isfile(iplist):
                    self.logger.error('%s is not exist.' %iplist)
                    sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
            url = {
            'update': '%s/expan/update?%s' %(self.uri,urllib.urlencode({"ip": self.params['ip'],"status":"%s" %status})),
            'status': '%s/expan/status?iplist=%s' %(self.uri,iplist),
            'clean': '%s/expan/clean' %self.uri
            }[action]
        except KeyError:
            self.logger.error("Action %s invailed." %action)
            sys.exit(er.ERR_SCRIPT_INPUTVALUE_ERROR)
        try:
            Ret = self.url_handler(url)
            if Ret['code'] == 0:
                if action == 'status':
                    strout = "|%-15s|%-20s|%-20s|\n" %("IP","Status","LastUpdateTime")
                    for line in Ret['data']:
                        strout += "|%-15s|%-20s|%-20s|\n" %(line['ip'],line['status'],line['update_time'])
                    sys.stdout.write(strout)
                else:
                    self.logger.info("%s Succ." %action)
                sys.exit(0)
            else:
                self.logger.error("Failed Action, %s" %Ret['msg'])
                sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
        except Exception,e:
            self.logger.error("Unexpected Action %s, %s" %(action,e))
            sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)

    def CheckConf(self):
        localip, clustername = ConfReader.GetId()
        if (localip == None) or (clustername == None):
            self.logger.error("local machine not registered in chenxiang,\
                    local ip: %s, cluster name: %s" % (localip, clustername))
            sys.exit(er.ERR_MACHINE_NOT_REGISTERED)

        clusterinfo = ConfReader.ReadConfTable('cluster_info')
        if clusterinfo == None:
            self.logger.error("local cluster info not exists")
            sys.exit(er.ERR_MACHINE_NOT_REGISTERED)

        params = {}
        params['ip'] = localip
        params['hostname'] = self.hn
        params['clustername'] = clustername
        params['version'] = '0'
        url = '%s/chenxiang/cluster_info?%s' %(self.uri, urllib.urlencode(params))
        try:
            Ret = self.url_handler(url)
            if Ret['code'] == 0:
                self.logger.debug("get cluster info succ, %s." % Ret['clusterinfo'])
                if Ret['clusterinfo'] == clusterinfo:
                    self.logger.info("local cluster info is the same with remote cluster info")
                    sys.exit(0)
                else:
                    self.logger.warn("local cluster info :%s, remote cluster info: %s" 
                            % (clusterinfo, Ret['clusterinfo']))
                    sys.exit(er.ERR_CONF_CHECK_FAILED)
            else:
                self.logger.error("failed to get cluster info, %s." % Ret)
                sys.exit(er.ERR_CONF_CHECK_FAILED)
        except Exception,e:
            self.logger.error("Unexpected, %s" %e)
            sys.exit(er.ERR_AG_VERIFY_FAILED)

    def ExpanStatus(self,iplist):
        interval = 0.5
        timeout = 1200 #设置20分钟超时
        counter = 0
        try:
            if not os.path.isfile(iplist):
                self.logger.error('%s is not exist.' %iplist)
                sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
            url = '%s/expan/status?iplist=%s' %(self.uri,iplist)
            try:
                start = datetime.datetime.now()
                while True:
                    ongoing = 0
                    succ = 0
                    fail = 0
                    failip = []
                    na = 0
                    Ret = self.url_handler(url)
                    if Ret['code'] == 0:
                        pstrout = ' ' * 80 + '\n' #清屏
                        pstrout += "|%-15s|%-30s|%-20s|\n" %("IP","Status","LastUpdateTime")
                        for line in Ret['data']:
                            pstrout += "|%-15s|%-30s|%-20s|\n" %(line['ip'],line['status'],line['update_time'])
                            if 'FAILED' in line['status']:
                                failip.append(line['ip'])
                                fail += 1
                            elif 'OK' in line['status']: succ += 1
                            elif 'N/A' in line['status']: na += 1
                            elif 'RUN' in line['status']: ongoing += 1
                        if counter < 3:
                            counter += 1
                        else:
                            counter = 1
                        runningbar = '.' * counter + ' ' * (3 - counter) + '\r'
                        strout = 'Summary Report: Total: %d, Success: %d, Running: %d, Failed: %d, %s, N/A: %d' %(len(Ret['data']),succ,ongoing,fail,failip,na)
                        sys.stdout.write(strout + runningbar)
                        sys.stdout.flush()
                        now = datetime.datetime.now()
                        if ongoing == 0 and (now - start).seconds > 10:
                            sys.stdout.write(pstrout+strout)
                            if na == 0 and fail == 0:
                                sys.exit(0)
                            else:
                                sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
                        elif ongoing != 0 and (now - start).seconds>timeout:
                            sys.stdout.write(pstrout+strout)
                            sys.exit(er.ERR_RUN_TIME_OUT)
                        sleep(interval)
                    else:
                        self.logger.error("Query Status Failed, %s" %Ret['msg'])
                        sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
            except KeyboardInterrupt:
                #self.Expan('status','',iplist)
                self.logger.info('You Cancelled, Use -Q [iplistfile] for result.')
                sys.exit(er.ERR_SCRIPT_CANCELLED)
        except Exception,e:
            self.logger.error("Unexpected, %s" %e)
            sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
    
    def CheckRoleDef(self, args):
        try:
            incomemd5 = None
            if len(args) > 1:
                self.logger.error("error input args %s" % str(args))
                sys.stderr.write("Error input, '--checkroledef' ,or '--checkroledef md5' are accepted ")
                sys.exit(er.ERR_SCRIPT_INPUTVALUE_ERROR)
            elif len(args) == 1:
                incomemd5 = args[0]
            else:
                urlmd5 = '%s/roledef/roledefmd5' % self.uri
                jsonret = self.url_handler(urlmd5)
                if jsonret['code'] != 0:
                    msg = 'Fail to get role.def md5 from dayu master, dayu master return:\n %s' % jsonret
                    raise Exception(msg)
                incomemd5 = jsonret['content'] 

            me = LibMe()
            localmd5 = CalculateMd5(me.local_cache.roledef_path)
            if localmd5 == incomemd5:
                logger.info('Check passed, md5 is %s' % localmd5)
                return 

            raise RoledefErrException('RoleDefWrong, local md5 %s, correct md5 %s' % (localmd5, incomemd5))

            '''
            urlroledef = '%s/roledef/roledef' % self.uri
            # then get roledef and repair
            ret = self.url_handler(urlroledef)
            jsonret = json.loads(ret)
            if jsonret['code'] == 0:
                roledef = jsonret['content']
                WriteFile(roledefpath, roledef)
                raise RoledefErrException('')
            raise Exception('Error roledef get from dayu master %s' % str(ret))
            '''
        except RoledefErrException, e:
            logger.error(str(e))
            sys.exit(er.ERR_LOCAL_ROLEDEF)
        except Exception, e:
            logger.error('CheckRoleDefFailed, %s' % traceback.format_exc())
            sys.exit(er.ERR_GENERIC_ERROR)

    def CheckBuilds(self, action, datafile='',roles=None):
        '''
        check /apsara/builds md5 sum for sure of dayu builds
        ''' 
        try:
            url = {
                    'get':'%s/checkbuilds/get?%s' % (self.uri, self.data) if not roles else '%s/checkbuilds/get?%s' % \
                     (self.uri, urllib.urlencode({"ip": self.params['ip'],"hostname":self.params['hostname'],"roles":"%s" % roles})),
                    'set':'%s/checkbuilds/set?%s' % (self.uri, self.data),
                    'clear':'%s/checkbuilds/clear?%s' % (self.uri, self.data)
                    }[action]
        except KeyError:
            self.logger.error("invailed action %s" %action)
            sys.exit(er.ERR_SCRIPT_INPUTVALUE_ERROR)
        Ret = None
        if action == 'set':
            if not os.path.isfile(datafile):
                self.logger.error('%s is not exist.' % datafile)
                sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
            module2info = None
            with open(datafile, 'r') as f:
                module2info = json.load(f)
            Ret = self.url_handler(url, method = 'put', data = module2info)
        else:
            Ret = self.url_handler(url, method = 'get')
        try:
            if Ret['code'] == 0:
                if action == 'set':
                    print Ret['msg']
                    sys.exit(0)
                elif action == 'get':
                    module2info = Ret['data']
                    print json.dumps(module2info, sort_keys=True, indent=4) 
                    CheckBuildinfoHelper.init_local_md5info(module2info)
                    sys.exit(0)
                elif action == 'clear':
                    print Ret['msg']
                    sys.exit(0)
            else:
                self.logger.error(Ret['msg'])
                sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
        except Exception,e:
            self.logger.error("exception:%s, traceback:%s" % (e, traceback.format_exc()))
            sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)

if __name__ == '__main__':
    logger = log.getLogger(__name__)
    try:
        options,args = getopt.getopt(sys.argv[1:],'?cegsafd:pq:u:Q:',['version', 'checkconf', 'checkroledef', 'getbuildinfo', 'setbuildinfo=', 'clearbuildinfo'])
    except Exception, e:
        logger.error(e)
        Usage(er.ERR_SCRIPT_RUN_CMD_FAILED)
    try:
        agip = get_agip(retrytimes=5, sleeptime=3)
        local_ip = get_localip(retrytimes=5, sleeptime=3)
    except Exception as e:
        logger.error(e)
        sys.exit(er.ERR_SCRIPT_RUN_CMD_FAILED)
    if not options and not args:
        Usage(er.ERR_SCRIPT_RUN_CMD_FAILED)
    c = Cadidate(agip, local_ip, logger)
    for (switch, val) in options:
        if (switch == '-?'): Usage(0)
        elif (switch[1] in 'e'): c.Security()
        elif (switch[1] in 'c'): c.Vaildate()
        elif (switch[1] in 'g'): c.Brother('get')
        elif (switch[1] in 's'): c.Brother('status')
        elif (switch[1] in 'f'): c.Brother('free')
        elif (switch[1] in 'a'): c.Brother('add')
        elif (switch[1] in 'd'): c.Brother('delete',dip=val)
        elif (switch[1] in 'Q'): c.Expan('status','',val)
        elif (switch[1] in 'q'): c.ExpanStatus(val)
        elif (switch[1] in 'p'): c.Expan('clean')
        elif (switch[1] in 'u'): c.Expan('update',val)
        elif (switch == '--checkconf'): c.CheckConf()
        elif (switch == '--checkroledef'): c.CheckRoleDef(args)
        elif (switch == '--getbuildinfo'): c.CheckBuilds('get', roles=args)
        elif (switch == '--setbuildinfo'): c.CheckBuilds('set', datafile=val)
        elif (switch == '--clearbuildinfo'): c.CheckBuilds('clear')
        elif (switch == '--version'): Usage(0)
        else: Usage(0)
