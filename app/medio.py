#
# Medio: Automatic media organizer for Synology Photo Station
# Copyright(c) 2016-2022 Jonathan Poland
#

import re, os, time, sys, traceback, subprocess
import pyinotify
import configparser 
import queue
import filecmp
from threading import Thread, Timer

PKGDIR="/app"
SRCDIR="/source"
DSTDIR="/dest"

ACCEPTED_EXTENSIONS = ['.jpg', '.jpeg', '.mpg', '.mp4', '.png',
                       '.mov', '.thm', '.avi', '.raw', '.arw', 
                       '.heic', '.heif', '.nef', '.3gp']

def log(msg):
    print('[%s] %s\n' % (time.ctime(), str(msg)))

class Config(object):
    DEFAULT_DSTFMT =  r'%Y/%m/%Y%m%d_%H%M%S%%-uc.%%e'

    def __init__(self):
        log('ENV FORMAT: ' + os.environ.get('FORMAT', self.DEFAULT_DSTFMT))
        log('ENV DELETE_DUPLICATE: ' + os.environ.get('DELETE_DUPLICATE', True))
        log('ENV LOCALE: ' + os.environ.get('LOCALE', 'zh_CN.utf8'))

    @property
    def UI_SRCDIR(self):
        return SRCDIR

    @property
    def UI_DSTDIR(self):
        return DSTDIR

    @property
    def UI_DSTFMT(self):
        return os.environ.get('FORMAT', self.DEFAULT_DSTFMT)

    @property
    def UI_DELETE_DUPS(self):
        return os.environ.get('DELETE_DUPLICATE', True)

    @property
    def UI_LOCALE(self):
        return os.environ.get('LOCALE', 'zh_CN.utf8')

class Spawn(object):
    """A wrapper around subprocess just to save boilerplate"""
    def __init__(self, args, shell=False, env=None):
        handle = subprocess.Popen(args, stdin=open(os.devnull, 'r'), stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, close_fds=True, text=True, shell=shell, 
                                  env=env)
        self.stdout, self.stderr = handle.communicate()
        self.retval = handle.wait()

class LoggingTimer(Timer):
    """A threading.Timer that will catch exceptions in run() and log them to our
       global log file.  Without this, threads would throw and exit, but leave
       no trace."""
    def run(self):
        try:
            Timer.run(self)
        except:
            err = traceback.format_exc(2)
            log(err)

class Worker(Thread):
    """A single thread to do most of the work.  Waits on a Queue for new work"""
    rename_re = re.compile(r"'(\S+)'\s+-->\s+'(\S+)'")
    cfg = None
    workq = None

    def __init__(self, cfg, workq):
        Thread.__init__(self)
        self.cfg = cfg
        self.workq = workq
        self.start()

    def process_file(self, path):
        """This does the bulk of the work.  Calls exiftool to do the rename and synoindex 
           to tell DSM about it."""
        # Use exiftool to do the rename
        srcfile = os.path.join(self.cfg.UI_SRCDIR, path)
        dstfmt = os.path.join(self.cfg.UI_DSTDIR, self.cfg.UI_DSTFMT)
        dstfile = None
        cmd = [os.path.join(PKGDIR, 'exiftool'), '-v', '-r', '-d', dstfmt, 
                "-filename<filemodifydate", "-filename<createdate", 
                "-filename<datetimeoriginal", srcfile]
        p = Spawn(cmd)
        if p.retval != 0:
            log('exiftool FAILED for ' + path + ': ' +  ' '.join(p.stderr.split('\n')))
            return
        for line in p.stdout.split('\n'):
            m = self.rename_re.match(line)
            if m and m.group(1) == srcfile:
                dstfile = m.group(2)
                common = os.path.commonprefix([srcfile, dstfile])
                log('Moved %s to %s' % (os.path.relpath(srcfile, common), 
                                        os.path.relpath(dstfile, common)))
                if self.cfg.UI_DSTFMT == self.cfg.DEFAULT_DSTFMT and self.cfg.UI_DELETE_DUPS and '-' in dstfile:
                    extension = os.path.splitext(dstfile)[1]
                    dupfile = dstfile.split('-')[0] + extension
                    log('Check if %s is a duplicate file of %s' % (dstfile, dupfile))
                    if (os.path.exists(dupfile) and filecmp.cmp(dstfile, dupfile, False)):
                        os.remove(dstfile)
                        log("Removed %s: a duplicate of %s" % (dstfile, dupfile))

        if dstfile is None:
            log('exiftool succeeded, but no file rename information found')

    def run(self):
        errorCount = 0
        while errorCount < 5:
            try:
                path = self.workq.get()
                self.process_file(path)
                self.workq.task_done()
            except:
                errorCount += 1
                err = traceback.format_exc(2)
                log(err)
        log('Too many errors, Worker thread exiting')

class Watcher(Thread):
    """A thread to watch files that are in transit"""
    cfg = None
    workq = None
    watchq = None
    timer = None
    active = {}

    def __init__(self, cfg, workq, watchq):
        Thread.__init__(self)
        self.cfg = cfg
        self.workq = workq
        self.watchq = watchq
        self.start()

    def check_actives(self):
        # Check all actives
        now = time.time()
        delete_keys = []
        for filepath, tstamp in self.active.items():
            if now - tstamp > 30:
                self.workq.put(filepath)
                delete_keys.append(filepath)
        for filepath in delete_keys:
            del self.active[filepath]
        # Check again soon if there's more
        if len(self.active) > 0:
            if self.timer:
                self.timer.cancel()
            self.timer = LoggingTimer(5, self.check_actives)
            self.timer.start()

    def process_file(self, path):
        filesize = os.path.exists(path) and os.stat(path).st_size or 0
        if filesize > 0:
            self.active[path] = time.time()
            self.check_actives()

    def run(self):
        errorCount = 0
        while errorCount < 5:
            try:
                path = self.watchq.get()
                self.process_file(path)
                self.watchq.task_done()
            except:
                errorCount += 1
                err = traceback.format_exc(2)
                log(err)
        log('Too many errors, Watcher thread exiting')

class EventHandler(pyinotify.ProcessEvent):
    """This class handles our file change events queueing events to our work and watch queues"""
    cfg = None
    workq = None
    watchq = None

    def __init__(self, cfg, workq, watchq):
        self.cfg = cfg
        self.workq = workq
        self.watchq = watchq
        # Check for files we may have missed and queue them
        for entry in os.listdir(self.cfg.UI_SRCDIR):
            if self.is_relevant_file(entry):
                self.watchq.put(os.path.join(self.cfg.UI_SRCDIR, entry))

    def is_relevant_file(self, path):
        """Return whether or not we care about this file type"""
        (root, ext) = os.path.splitext(path)
        if ext.lower() in ACCEPTED_EXTENSIONS:
            return True
        return False

    def process_IN_CREATE(self, event):
        """We see this when we upload via the network (NFS, AFS, SMB)"""
        if self.is_relevant_file(event.pathname):
            self.watchq.put(event.pathname)
        
    def process_IN_CLOSE_WRITE(self, event):
        """We see lots of these per file when uploading via the network"""
        if self.is_relevant_file(event.pathname):
            self.watchq.put(event.pathname)

    def process_IN_MOVED_TO(self, event):
        """We see this when the DS photo app uploads stuff or we use file manager to move
           files in from somewhere else"""
        if self.is_relevant_file(event.pathname):
            self.workq.put(event.pathname)
        
if __name__ == '__main__':
    try:
        cfg = Config()
        workq = queue.Queue()
        watchq = queue.Queue()
        worker = Worker(cfg, workq)
        watcher = Watcher(cfg, workq, watchq)
        wm = pyinotify.WatchManager()
        notifier = pyinotify.Notifier(wm, EventHandler(cfg, workq, watchq))
        mask = pyinotify.IN_CREATE | pyinotify.IN_MOVED_TO | pyinotify.IN_CLOSE_WRITE
        wdd = wm.add_watch(cfg.UI_SRCDIR, mask)
        if cfg.UI_LOCALE:
            log('Forcing LOCALE to %s' % cfg.UI_LOCALE)
            os.environ['LC_ALL'] = cfg.UI_LOCALE
        log('Source directory: %s' % cfg.UI_SRCDIR)
        log('Destination directory: %s' % cfg.UI_DSTDIR)
        log('Destination filename format: %s' % cfg.UI_DSTFMT)
        log('Watching for changes...')
        notifier.loop()
    except:
        err = traceback.format_exc(2)
        log(err)

