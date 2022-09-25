# backuparchive.py
# (c) cavaliba.com 2022

# usage
# python3 backuparchive.py --conf myconf.yml


import os
import sys
import yaml
import time
import datetime
import signal
import argparse
import shutil
#import glob
import socket

ARGS = {}
CONF = {}
VERSION = "backuparchive - Version 1.0 - 2022/09/25 - cavaliba.com"


THRESHOLD = {}
THRESHOLD["hour"]  =      3500
THRESHOLD["day"]   =     86200
THRESHOLD["week"]  =    604600
THRESHOLD["month"] =   2419000   # 28 days minus 200 sec.
THRESHOLD["year"]  =  31557000

INTERVALS = {}
INTERVALS["hour"]  =      3600
INTERVALS["day"]   =     86400
INTERVALS["week"]  =    604800
INTERVALS["month"] =   2678400   # 31 days
INTERVALS["year"]  =  31557600



# ----

TEMPLATE_CONF="""
---


folders:

  mybinlogs:
    path: /tmp/backuparchive/binlogs/
    extension: .binlog
    method: purge
    unit: day
    max: 15


  dbhourly:
    path: /tmp/backuparchive/hourly/
    extension: .gz
    method: rotate
    latest_dir: /tmp/backuparchive/latest/
    unit: hour
    max: 36


  dbdaily:
    path: /tmp/backuparchive/daily/
    extension:
    method: rotate
    latest_dir: /tmp/backuparchive/latest/
    unit: day
    max: 9

  dbweekly:
    path: /tmp/backuparchive/weekly/
    extension: .gz
    method: rotate
    latest_dir: /opt/backuparchive/latest/
    unit: week
    max: 4

  dbmonthly:
    path: /tmp/backuparchive/monthly/
    extension: .gz
    method: rotate
    latest_dir: /opt/backuparchive/latest/
    unit: month
    max: 12

  dbyearly:
    path: /tmp/backuparchive/yearly/
    extension: .gz
    method: append
    latest_dir: /opt/backuparchive/latest/
    unit: year
    max: 20



"""

# -----------------
def logit(line):
    now = datetime.datetime.today().strftime("%Y/%m/%d - %H:%M:%S")
    print(now + ' : ' + line)

def debug(*items):
    now = datetime.datetime.today().strftime("%Y/%m/%d - %H:%M:%S")
    if ARGS['debug']:
        print(now + ' : ' + 'DEBUG :', ' '.join(items))

# -----------------
def timeout_handler(signum, frame):
    # raise Exception("Timed out!")
    print("Timed out ! (max_execution_time)")
    sys.exit()

# -----------------
def conf_load_file(config_file):

    with open(config_file) as f:
        conf = yaml.load(f, Loader=yaml.SafeLoader)

    # verify content
    if conf is None:
        return {}

    return conf

# -----------------
class BlankLinesHelpFormatter(argparse.HelpFormatter):
   def _split_lines(self, text, width):
        return super()._split_lines(text, width) + ['']

def parse_arguments(myargs):

    parser = argparse.ArgumentParser(description='backuparchive by Cavaliba.com', formatter_class = BlankLinesHelpFormatter)

    parser.add_argument('--version', '-v', help='display current version',
                        action='store_true', required=False)

    # parser.add_argument('--cron', help='equiv to report, pager_enable, persist, short output',
    #                     action='store_true', required=False)

    parser.add_argument('--conf', '-c', help='specify yaml config file',
                        action='store', required=False)

    parser.add_argument('--debug', help='verbose/debug output',
                         action='store_true', required=False)

    # parser.add_argument('--list', '-l', help='list identified files, no backup',
    #                     action='store_true', required=False)

    parser.add_argument('--showconf', help='print the configuration and exit',
                        action='store_true', required=False)

    parser.add_argument('--template', help='print a configuration template and exit',
                        action='store_true', required=False)

    r = parser.parse_args(myargs)
    return vars(r)
    
# ----


def display_time(seconds, granularity=3):


    intervals = (
        ('weeks', 604800),  # 60 * 60 * 24 * 7
        ('days', 86400),    # 60 * 60 * 24
        ('hours', 3600),    # 60 * 60
        ('min', 60),
        ('sec', 1),
    )


    result = []

    if seconds == 0:
        return "0 sec."
    for name, count in intervals:
        value = seconds // count
        if value:
            seconds -= value * count
            if value == 1:
                name = name.rstrip('s')
            result.append("{} {}".format(value, name))
    return ' '.join(result[:granularity])



def get_newest(path,extension):

    newestfile = ""
    newestage = 0

    for file in os.listdir(path):

        fullfile = os.path.join(path,file)
        if not os.path.isfile(  fullfile ):
            continue

        if extension:            
            if not file.endswith(extension):
                continue
        
        age = int ( os.path.getmtime( fullfile ) )

        if age > newestage:
            newestage = age 
            newestfile = fullfile
      
    return newestfile
    


def get_oldest(path,extension):

    oldestfile = ""
    oldestage = -1

    for file in os.listdir(path):

        fullfile = os.path.join(path,file)
        if not os.path.isfile(  fullfile ):
            continue

        if extension:            
            if not file.endswith(extension):
                continue
        
        age = int ( os.path.getmtime( fullfile ) )

        if oldestage == -1 or age < oldestage:
            oldestage = age 
            oldestfile = fullfile
      
    return oldestfile



# ------------
# Main entry
# ------------
if __name__ == "__main__":

    if not sys.version_info >= (3, 6):
        logit("Should use Python >= 3.6")
        sys.exit()

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(60)   # seconds


    ARGS = parse_arguments(sys.argv[1:])

    if ARGS["version"]:
        print(VERSION)
        sys.exit()

    # print an empty config template
    if ARGS["template"]:
        print(TEMPLATE_CONF)
        sys.exit()

    if ARGS["conf"]:
        configfile = ARGS["conf"]
    else:
        configfile = "conf.yml"
    try:
        CONF = conf_load_file(configfile)
    except:        
        print("Could not load config file " + configfile)
        sys.exit()

    # print an empty config template
    if ARGS["showconf"]:
        print(yaml.dump(CONF))
        sys.exit()

    logit("-" * 80)
    logit("Config file: " + configfile)



    # print(display_time(0,4))
    # sys.exit()


    # --------------------------------
    # Main loop over patterns in CONF
    # --------------------------------

    for folder in CONF["folders"]:

        logit("Folder: " + folder)

          # dbmonthly:
          #   path: /tmp/backuparchive/monthly/
          #   extension: .gz
          #   method: rotate
          #   latest_dir: /opt/backuparchive/latest/
          #   unit: month
          #   maxvalue: 12

        path = CONF["folders"][folder].get("path","?")

        if path[0] != "/":
            logit("  ERROR - path must be absolute and start with a / : " + path)
            continue

        if not os.path.isdir(path):
            logit("  ERROR - path doesnt exist : " + path)
            continue

        extension = CONF["folders"][folder].get("extension", None)

        # purge, append, rotate
        method = CONF["folders"][folder].get("method", "append")

        # hour, day, week, month, year
        unit = CONF["folders"][folder].get("unit","day")

        # how many items / units
        maxvalue = CONF["folders"][folder].get("max", 0)

        logit("  conf: method=" + method + ", unit=" + unit +", max=" + str(maxvalue) )


        # APPEND NEW
        if method == "rotate" or method == "append":

            # latest_dir
            latest_dir = CONF["folders"][folder].get("latest_dir")
            if not os.path.isdir(latest_dir):
                print("   ERROR : latest_dir missing : " + latest_dir)
                continue

            newestbackup = get_newest(latest_dir,extension)
            if not os.path.isfile(newestbackup):
                continue
            newestbackup_age = int ( os.path.getmtime( newestbackup ) )
            logit ("  newest backup: " + newestbackup )


            newestarchive = get_newest(path, extension)
            if os.path.isfile(newestarchive):
                newestarchive_age = int ( os.path.getmtime( newestarchive ) )
                # check delta, continue if too recent
                delta = newestbackup_age - newestarchive_age
                logit("  latest archive: " + newestarchive + " - delta from newest backup is " + display_time(delta,4))

            else:
                logit("  no archives yet")
                #delta = 315358000
                delta = 99999999999

            if  unit not in ["hour","day","week","month","year"]:
                logit("ERROR - unkonwn unit " + str(unit) )
                continue

            delta_threshold = THRESHOLD[unit]
            # too recent, don't copy
            if delta > delta_threshold:
                shutil.copy2(newestbackup, path, follow_symlinks=True)
                logit("  copied: " + newestbackup)



        # PURGE OLD
        if method == "purge" or method == "rotate":

            oldestarchive = get_oldest(path, extension)
            if os.path.isfile(oldestarchive):
                oldestarchive_age = int ( os.path.getmtime( oldestarchive ) )
                
                now = int(time.time())
                delta = now - oldestarchive_age
                logit("  oldest archive: " + oldestarchive + " - delta from now is " + display_time(delta,4))

            else:
                logit("  no archives yet")
                delta = 0

            
            items = delta / INTERVALS[unit]
            #print(items)
            if items > maxvalue:
                os.remove(oldestarchive)
                logit("  removed: " + oldestarchive)
