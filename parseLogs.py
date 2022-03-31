import os
import re
import json
import gzip
import shutil
import hashlib
import collections
import urllib.request
from tqdm import tqdm
from urllib.parse import urlparse

def increment(key, rootURL, date, dict):
    if key in dict[date].keys():
        dict[date][key][0] += 1
    else:
        dict[date][key] = [1, rootURL]
    return dict

# Get path of script
dir_path = os.path.dirname(os.path.realpath(__file__))

# find or download bots file
userAgentsFile = os.path.join(dir_path, "crawler-user-agents.json")
if not os.path.isfile(userAgentsFile):
    urllib.request.urlretrieve("https://raw.githubusercontent.com/monperrus/crawler-user-agents/master/crawler-user-agents.json", "crawler-user-agents.json")

#read bots file
with open(userAgentsFile) as jsonFile:
    userAgents = json.load(jsonFile)
    #for entry in userAgents:
    #    if re.search(entry['pattern'], ua):
    #        print (ua)

# directory where logs live
if os.name == "nt":
    rootDir = "\\\\Lincoln\\Library\\ESPYderivatives\\log\\nginx\\test"
else:
    rootDir = "/media/Library/ESPYderivatives/log/nginx"

# format to parse nginx log lines
# help from https://gist.github.com/hreeder/f1ffe1408d296ce0591d
#lineformat = re.compile(r"""(?P<ipaddress>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] ((\"(GET|POST) )(?P<url>.+)(http\/1\.1")) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (["](?P<refferer>(\-)|(.+))["]) (["](?P<useragent>.+)["])""", re.IGNORECASE)
lineformat = re.compile(r"""(?P<ipaddress>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] ((\"(GET|POST) )(?P<url>.+)(http\/1\.1")) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (?P<refferer>-|"([^"]+)") (["](?P<useragent>[^"]+)["])""", re.IGNORECASE)

#set up uniques output directory
uniques = os.path.join(rootDir, "uniques")
if not os.path.isdir(uniques):
    os.mkdir(uniques)
    
#Set up directory to move logs after process to be scheduled for deletion
parsed = os.path.join(rootDir, "parsed")
if not os.path.isdir(parsed):
    os.mkdir(parsed)

# Read data from already parsed logs
downloads = {}
for existingFile in os.listdir(uniques):
    existingDateKey = os.path.splitext(existingFile)[0]
    print (existingDateKey)
    with open(os.path.join(uniques, existingFile)) as json_file:
        existingData = json.load(json_file)
        downloads[existingDateKey] = existingData

#count of log files processed
count = 0

for logZip in os.listdir(rootDir):
    if logZip.startswith("access.log") and logZip.endswith(".gz"):
        count += 1
        logPath = os.path.join(rootDir, logZip)
        print (logPath)

        # Used to focus on one file for testing
        #if count == 24:
        if count > 0:
            #tempDir = tempfile.mkdtemp()
            
            # Count of lines within log file
            lineCount = 0
            
            # Unzip log file
            with gzip.open(logPath, "rt") as logFile:               
                print ("parsing " + logZip)
                # use tqdm for process bar
                for l in tqdm(logFile.readlines()):
                    lineCount += 1
                    
                    if lineCount >= 1:
                    
                        # Parse nginx log line
                        data = re.search(lineformat, l)
                        if data:
                            datadict = data.groupdict()
                            ip = datadict["ipaddress"]
                            datetimestring = datadict["dateandtime"]
                            url = datadict["url"]
                            bytessent = datadict["bytessent"]
                            referrer = datadict["refferer"]
                            useragent = datadict["useragent"]
                            status = datadict["statuscode"]
                            method = data.group(6)
                            
                            #get year-month
                            year = datetimestring.split(":")[0].split("/")[-1]
                            month = datetimestring.split(":")[0].split("/")[-2]
                            date = year + "-" + month
                            
                            # add key for year-month
                            if not date in downloads.keys():
                                downloads[date] = {}
                            
                            #if "45.146.166.229" in ip:
                            #    print (useragent)
                            
                            isBot = False
                            #for entry in userAgents:
                            #    if re.search(entry['pattern'], useragent):
                            #        isBot = True
                            if isBot == False:
                            
                            
                                #statuses = increment(status, statuses)
                                #ips = increment(ip, ips)
                                if status == "200" and url.startswith("/downloads/"):
                                    if "file=thumbnail" in url:
                                        pass
                                    else:
                                        rootURL = urlparse(url)[2].strip()
                                        slug = ip + rootURL
                                        #hash = hashlib.md5(slug.encode('utf-8')).hexdigest()
                                        #print (hash)
                                        downloads = increment(slug, rootURL, date, downloads)
                                
                                
                                    """
                                    if url.startswith("/downloads/02871c72m"):
                                        print (ip)
                                        print ("\t" + status)
                                        print ("\t" + useragent)
                                        
                                        for entry in userAgents:
                                            if re.search(entry['pattern'], useragent):
                                                print ("yes!")
                                    """
            # move log to be scheduled for deletion
            shutil.move(logPath, parsed)
 
# write output data
for dateKey in downloads:
    uniquesData = downloads[dateKey]
    uniquesFile = os.path.join(uniques, dateKey) + ".json"
    with open(uniquesFile, 'w') as fp:
        #sorted = {k: v for k, v in sorted(uniquesData.items(), key=lambda item: item[1])}
        #sorted = collections.OrderedDict(sorted(uniquesData.iteritems(), key=lambda (k,v):(v,k), reverse=True))
        #sorted = collections.OrderedDict(sorted(uniquesData.items(), reverse=True))
        #fp.write(json.dumps(sorted, indent=4, sort_keys=False))
        fp.write(json.dumps(uniquesData, sort_keys=False))