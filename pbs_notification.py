# Copyright (c) 2017 Yonghao Jin
# All Rights Reserved
import paramiko
from optparse import OptionParser
import getpass

import xml.etree.ElementTree as ET
import time
from collections import defaultdict
from pync import Notifier


default_ip='202.38.82.66'
default_port=65531

parser = OptionParser(usage='usage: %prog [options] user',
                      version='%prog 1.0', description="Dispaly pbs job status in OSX Notification Center. Written by Yonghao Jin")


parser.add_option('-i', '--interval', action='store', type='int', dest='interval',
                  default=60,
                  help='default refresh interval (default: %d)' % 60)
parser.add_option('-I', '--Interval', action='store', type='int', dest='no_change_interval',
                default=300,
                help='default refresh interval when no status change happening (default: %d)' % 300)
parser.add_option('-P', '--password', action='store_true', dest='readpass', default=False,
                  help='read password (for key or password auth) from stdin')
parser.add_option('-s', '--server', action='store', type='string', dest='server', default=default_ip, metavar='host',
                  help='remote host ip to check pbs jobs')
parser.add_option('-p', '--port', action='store', type='int', dest='port',
                  default=default_port,
                  help='default port of the pbs server (default: %d)' % default_port)

options, args = parser.parse_args()
if len(args) != 1:
    parser.error('Incorrect number of arguments.')

g_interval = options.interval
g_no_change = options.no_change_interval

ssh=paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

if options.readpass:
    passwd = getpass.getpass('Enter SSH password: ')
else:
    passwd = ""
try:
    ssh.connect(options.server, options.port ,args[0], passwd)
except:
    print "Unable to connect to the server!"
    quit()


def checkPBS(ssh):
    def toseconds(s):
        tot = 0
        fact = 1
        for t in s.split(":").__reversed__():
            tot += fact * int(t)
            t *= 60
        return tot

    def secondstostirng(s):
        return "{0}:{1}:{2}".format(s/3600, (s/60)%60, s % 60)

    _,stdout,_ = ssh.exec_command('qstat -ext')
    content = stdout.read()
    if content == "":
        return []
    jobs = []
    jobInfo = ET.fromstring(content)
    for job in jobInfo:
        dic = defaultdict(lambda: None)
        dic['job_id'] = job.findtext('Job_Id')
        dic['job_name'] = job.findtext('Job_Name')
        rUsed = job.find('resources_used')
        if rUsed != None:
            dic['cputime'] = rUsed.findtext('cput')
            dic['walltime'] = rUsed.findtext('walltime')
            dic['processors'] = toseconds(dic['cputime'])/float(toseconds(dic['walltime']))
        dic['job_state'] = job.findtext('job_state')
        try:
            startTimeStamp = int(job.findtext('start_time'))
            dic['start'] = time.strftime("%a %H:%M:%S", time.localtime(startTimeStamp))
            dic['remaining'] = secondstostirng(int(job.find('Walltime').findtext('Remaining')))
        except:
            pass
        jobs.append(dic)
    return jobs


def formatRunning(dic):
    subtitle = "Running"
    message = "Remaining: {0}. ".format(dic['remaining'])
    if dic['cputime'] != None:
        message = "Used: {0}. CPU: {1:.1f}. Remaining: {2}".format(dic['walltime'], dic['processors'], dic['remaining'])
    return subtitle, message

# def formatQueue(dic, status):
#     return dic['job_id'], status

def displayUpdate(dic, s = True):
    title = "{0} {1}".format(dic['job_name'], dic['job_id'])
    group = dic['job_id']
    state = dic['job_state']
    if state == "R":
        subtitle, message = formatRunning(dic)
    else:
        subtitle = dic['job_id']
        message = state
        if state == "H":
            message = "BatchHeld."
        if state == "Q":
            message = "Queued."
        if state == "F":
            message = "Terminated."
    if s:
        Notifier.notify(message, group=group, title=title, subtitle=subtitle, sound="Glass")
        time.sleep(2)
    else:
        Notifier.notify(message, group=group, title=title, subtitle=subtitle)

def displayInfo(pbsdb, jobs):
    hold = pbsdb.keys()
    for rec in jobs:
        job_id = rec['job_id']
        if rec['job_state'] == "E":
            rec["job_state"] = "R"
        status = rec['job_state']
        if pbsdb.has_key(job_id):
            hold.remove(job_id)
            displayed = pbsdb[job_id]
            if status == displayed['job_state']:
                if time.time() - displayed['last'] > g_no_change:
                    displayUpdate(rec, s = False)
            else:
                displayUpdate(rec)
                displayed['job_state'] = status
                displayed['last'] = time.time()
        else:
            pbsdb[job_id] = {'last':time.time(), 'job_state':status, 'dic':rec}
            displayUpdate(rec)

    for rest in hold:
        info = pbsdb[rest]
        rec = info['dic']
        rec['job_state'] = "F"
        displayUpdate(rec)
        pbsdb.pop(rest, None)


pbsdb = {}
while True:
    try:
        jobs = checkPBS(ssh)
    except KeyboardInterrupt:
        quit()
    except:
        Notifier.notify("Cannot connect to server. Please try again.")
        quit()
    displayInfo(pbsdb, jobs)
    time.sleep(options.interval)
