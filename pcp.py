#!/usr/bin/env python

# Copy a directory recursively in a configurable number of parallel processes.
# Written as a test if parallelization would speed up copy of a huge directory
# tree with small files from MooseFS.
# It didn't... much.


import os
import sys
import getopt
from time import sleep
from subprocess import Popen


def usage():
    print '\nUsage:\n\n'
          '%s -p <processes> <dir> <to dir>\n\n'
          'Source dir will appear inside of destination dir, for example:\n'
          '`%s -p 5 srcdir dstdir`'
          'will produce dstdir/srcdir/...\n' % (sys.argv[0], sys.argv[0])

try:
    opts, args = getopt.getopt(sys.argv[1:], 'p:', ['processes='])
except getopt.GetoptError as err:
    print str(err)
    sys.exit(2)


for o, v in opts:
    if o in ('-p', '--processes'):
        PROCESSES = v
    else:
        print "Don't know about the `%s` option.' % o
        usage()
        sys.exit(2)

try:
    PROCESSES = int(PROCESSES)
    if PROCESSES <= 0:
        raise ValueError
except (ValueError, NameError) as err:
    print 'Processes number must be a positive integer'
    usage()
    sys.exit(2)

try:
    SRC_DIR, DST_DIR = args
except ValueError as err:
    usage()
    sys.exit(2)

# The larger ARG_MAX, the more args will be fitted into each cp or mkdir command.
# 200000 should be a nice estimation of maximum. But we can actually use a lower
# value, which will cause processes to be spawned more frequently and speed up 
# copy in the beginning and end of operation.
ARG_MAX = 150000



def form_cmd(cmd, objects, cmd_len):
    '''
    Move objects to cmd while resulting command does not exceed ARG_MAX 
    Returns length of resulting cmd
    '''
    # we don't use DST_DIR for mkdir, but it doesn't hurt to add anyway
    while len(objects) > 0 \
            and cmd_len + len(objects[0]) + len(DST_DIR)  <= ARG_MAX:
        cmd_len += len(objects[0])
        cmd.append(objects.pop(0))
    return cmd_len


def wait_proc():
    '''
    Block untill list of started processes is shorter that PROCESSES
    Additionally if there is a CP process that is handling objects in same dir,
    wait till it exits to avoid race conditions
    '''
    global proc_list
    while len(proc_list) >= PROCESSES:
        assert len(proc_list) <= PROCESSES, ('Saw more processes than'
                                             ' requested, aborting')
        for n, p in enumerate(proc_list):
            if p.poll() is not None: proc_list.pop(n)
        sleep(0.1)

def add_proc(cmd):
    '''
    Start process and add it to processes list.
    Will block if there are no empty process slots.
    '''
    wait_proc()
    p = Popen(cmd)
    global proc_list
    proc_list.append(p)


if SRC_DIR[-1] == '/': SRC_DIR = SRC_DIR[:-1]
WRK_DIR = os.path.dirname(SRC_DIR)
SRC_DIR = os.path.basename(SRC_DIR)
DST_DIR = os.path.abspath(DST_DIR)

os.chdir(WRK_DIR)


CP = ['cp', '-r', '-f', '--parent']
MKDIR = ['mkdir', '-p']

cp_cmd = CP[:]
cp_cmd_len = len(cp_cmd)
mkdir_cmd = MKDIR[:]
mkdir_cmd_len = len(mkdir_cmd)

proc_list = []
ptag = 0

files = []
dirs = []


for t in os.walk(SRC_DIR, onerror=lambda e: raise e):

    # symlinks to dirs show as dirs during os.walk()
    # we move them to files so that cp can handle them
    for d in t[1][:]:
        if os.path.islink('/'.join([t[0], d])):
            t[2].append(d)
            t[1].remove(d)

    # remember this dir
    dirs.append('/'.join([DST_DIR, t[0]]))

    # gather files in this dir
    files.append('/'.join([t[0], f]) for f in t[2])


while dirs:
    mkdir_cmd_len = form_cmd(mkdir_cmd, dirs, mkdir_cmd_len)
    add_proc(mkdir_cmd)
    mkdir_cmd = MKDIR[:]
    mkdir_cmd_len = len(mkdir_cmd)

while files:
    cp_cmd_len = form_cmd(cp_cmd, files, cp_cmd_len)
    cp_cmd += [DST_DIR]
    add_proc(cp_cmd)
    cp_cmd = CP[:]
    cp_cmd_len = len(cp_cmd)


for p in proc_list: p.wait()
