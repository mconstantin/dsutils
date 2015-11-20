import sys
from datetime import datetime
import time
import os
import argparse
from collections import Counter, defaultdict
from threading import Thread, Event
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

SORT_BY_TIME = 1
SORT_BY_NAME = 2
ASC = 0
DSC = 1


def TimerReset(*args, **kwargs):
    """ Global function for Timer """
    return _TimerReset(*args, **kwargs)


class _TimerReset(Thread):
    """Call a function after a specified number of seconds:

    t = TimerReset(30.0, f, args=[], kwargs={})
    t.start()
    t.cancel() # stop the timer's action if it's still waiting
    """

    def __init__(self, interval, function, args=[], kwargs={}):
        Thread.__init__(self)
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.finished = Event()
        self.resetted = True
        self.cancelled = False

    def cancel(self):
        """Stop the timer if it hasn't finished yet"""
        self.cancelled = True
        self.finished.set()

    def run(self):
        print "Time: %s - timer running..." % time.asctime()

        while not self.cancelled:
            while self.resetted:
                # print "Time: %s - timer waiting for timeout in %.2f..." % (time.asctime(), self.interval)
                self.resetted = False
                self.finished.wait(self.interval)

            if not self.finished.isSet():
                self.function(*self.args, **self.kwargs)

        self.finished.set()
        print "Time: %s - timer finished!" % time.asctime()

    def reset(self, interval=None):
        """ Reset the timer """

        if interval:
            # print "Time: %s - timer resetting to %.2f..." % (time.asctime(), interval)
            self.interval = interval
        else:
            # print "Time: %s - timer resetting..." % time.asctime()
            pass

        self.resetted = True
        self.finished.set()
        self.finished.clear()


# def make_verbose_action(sort_by='sort_by_time', sort_order='ASC'):
#     class VerboseAction(argparse.Action):
#         def __call__(self, parser, namespace, values, option_string=None):
#             setattr(namespace, 'sort_by', sort_by)
#             setattr(namespace, 'sort_order', sort_order)
#             setattr(namespace, self.dest, values)
#
#     return VerboseAction


class VerboseAction(argparse.Action):
    def __init__(self, option_strings, nargs='*', default=False, **kwargs):
        if option_strings != ['--verbose', '-v']:
            raise ValueError('VerboseAction action currently applies only to the verbose argument')

        if nargs != '*':
            raise ValueError('nargs should be 0 or more arguments')

        if default is True:
            raise ValueError('default should be false')

        super(VerboseAction, self).__init__(option_strings, nargs=nargs, default=default, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if option_string in self.option_strings:
            setattr(namespace, self.dest, True)
        sort_by, sort_order = VerboseAction._get_verbose_args(values)
        setattr(namespace, 'sort_by', sort_by)
        setattr(namespace, 'sort_order', sort_order)

    @staticmethod
    def _get_verbose_args(args):
        sort_by = None
        sort_order = None
        if type(args) == list:
            if not args:
                sort_by = SORT_BY_TIME
                sort_order = ASC
            elif len(args) == 1:
                if args[0] == 'sort_by_time':
                    sort_by = SORT_BY_TIME
                    sort_order = ASC
                elif args[0] == 'sort_by_name':
                    sort_by = SORT_BY_NAME
                    sort_order = ASC
                elif args[0] == 'ASC':
                    sort_by = SORT_BY_TIME
                    sort_order = ASC
                elif args[0] == 'DSC':
                    sort_by = SORT_BY_TIME
                    sort_order = DSC
                else:
                    raise ValueError('%s is not a valid first argument for verbose option' % args)
            elif len(args) == 2:
                if args[0] == 'sort_by_time':
                    sort_by = SORT_BY_TIME
                elif args[0] == 'sort_by_name':
                    sort_by = SORT_BY_NAME
                else:
                    raise ValueError('%s is not a valid first argument for verbose option' % args[0])
                if args[1] == 'ASC':
                    sort_order = ASC
                elif args[1] == 'DSC':
                    sort_order = DSC
                else:
                    raise ValueError('%s is not a valid second argument for verbose option' % args[1])
            else:
                raise ValueError('only 0, 1 or 2 arguments are allowed for the verbose option')
        return sort_by, sort_order


def create_parser():
    parser = argparse.ArgumentParser(add_help=True,
                                     description='monitor file system events')
    parser.add_argument('--target-dir', '-d',
                        default=os.getcwd(),
                        help='target directory')
    parser.add_argument('--recursive', '-r',
                        action='store_true',
                        help='monitor also target directory subdirectories')
    parser.add_argument('--files-only', '-fo',
                        action='store_true',
                        help='only count files events (no directories)')
    parser.add_argument('--verbose', '-v',
                        # nargs='*',
                        # default=False,
                        # action=make_verbose_action(sort_by='sort_by_name', sort_order='DSC'),
                        action=VerboseAction,
                        help='''
                        Display info about each event.
                        Optionally, add "sort by" and "sort order" arguments.
                        Valid values are sort_by_time (default) | sort_by_name, and ASC (default) | DSC, respectively.
                        ''')
    return parser


class CountingEventHandler(FileSystemEventHandler):
    class EventDetail(object):
        def __init__(self, time, event):
            self.time = time
            self.event = event

    def __init__(self, files_only=True, show_details=False, sort_by=None, sort_order=None):
        self.counts = defaultdict(int)
        if show_details:
            self.details = defaultdict(list)
        self.timer = TimerReset(5, self._timeout)
        self.start = True
        self.updated = False
        self.files_only = files_only
        self.show_details = show_details
        self.sort_by = sort_by
        self.sort_order = sort_order

    @staticmethod
    def print_events_details(events, level=1, sort_by=None, sort_order=ASC):
        def indent(tab_level):
            for i in range(tab_level):
                print '\t',

        def get_time_key(event_detail):
            return event_detail.time

        def get_source_key(event_detail):
            return event_detail.event.src_path

        if sort_by:
            if sort_by == SORT_BY_TIME:
                key = get_time_key
            elif sort_by == SORT_BY_NAME:
                key = get_source_key
            else:
                key = None
            reverse = sort_order == DSC
            events = sorted(events, key=key, reverse=reverse)

        for detail in events:
            indent(level)
            ts = detail.time.isoformat()
            evt = detail.event
            print ts + '\t',
            if hasattr(evt, 'src_path'):
                print 'src=%s' % getattr(evt, 'src_path'),
            if hasattr(detail.event, 'dest_path'):
                print ', dst=%s' % getattr(evt, 'dest_path')
            else:
                print  # newline

    def on_any_event(self, event):
        if not self.files_only or (self.files_only and not event.is_directory):
            self.counts[event.event_type] += 1
            if self.show_details:
                self.details[event.event_type].append(CountingEventHandler.EventDetail(datetime.now().time(), event))
            self.updated = True
        if self.start:
            self.timer.start()
            self.start = False
        else:
            self.timer.reset()

    def _timeout(self):
        # output results
        if self.updated:
            counter = Counter(self.counts)
            print counter.most_common()
            if self.show_details:
                # print details for each event type
                for et, events in self.details.iteritems():
                    if events:
                        print et
                        CountingEventHandler.print_events_details(events, level=1,
                                                                  sort_by=self.sort_by, sort_order=self.sort_order)
                self.details.clear()
            self.counts.clear()
            self.updated = False
        self.timer.reset()

    def shutdown(self):
        self.timer.cancel()


if __name__ == "__main__":
    parser = create_parser()
    options = parser.parse_args(sys.argv[1:])
    target = options.target_dir
    if not os.path.isabs(target):
        target = os.path.abspath(target)
    if not os.path.exists(target):
        msg = '%s does not exist' % target
        print msg
        sys.exit(-1)

    if not os.path.isdir(target):
        msg = '%s is not a directory' % target
        print msg
        sys.exit(-1)

    sort_by = None
    sort_order = None
    if options.verbose:
        sort_by = options.sort_by
        sort_order = options.sort_order

    event_handler = CountingEventHandler(files_only=options.files_only, 
                                         show_details=options.verbose, sort_by=sort_by,
                                         sort_order=sort_order)
    observer = Observer()
    observer.schedule(event_handler, options.target_dir, recursive=options.recursive)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        event_handler.shutdown()

    observer.join()