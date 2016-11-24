
# coding: utf-8

# ## Set-up: imports, constants, helper functions

# In[1]:

import boto3
import csv
import json
import matplotlib.pyplot as plt
import numpy             as np
import pandas            as pd
import plotly.plotly     as py
import urllib


from collections       import defaultdict
from datetime          import datetime as dt, timedelta

from boto3.s3.transfer import S3Transfer
from moztelemetry      import get_pings, get_pings_properties, get_one_ping_per_client, get_clients_history, get_records


# In[2]:

# Parameters
# TODO Argparse
_LOOKBACK = 0
_START    = None
_END      = None
_FRAC     = 1.0

# Static
_BASE_URL = "http://s3-us-west-2.amazonaws.com"
_BUCKET   = "telemetry-public-analysis-2"
_PATH     = "normandy/data/heartbeat/daily/"
_TRANSFER = None


# In[3]:

# Nested DefaultDict Constructor
def recDD():
    return defaultdict(recDD)

# Copies Dict to Nested DefaultDict
def copyToRecDD(target, source):
    for k,v in source.iteritems():
        if isinstance(v, dict):
            copyToRecDD(target[k], v)
        else:
            target[k] = v

# Safe chained get() function, e.g. foo.get('bar').get('baz') is safe from Nones
def tryGet(data, keys, noneVal = None):
    if not keys:
        return data or noneVal
    elif keys[0] in data.keys():
        return tryGet(data[keys[0]], keys[1:])
    else:
        return noneVal

# Flattens a dict to a 2D list
def dictToList(data):
    if isinstance(data, dict) or isinstance(data, defaultdict):
        output = []
        for k,v in data.iteritems():
            rows = dictToList(v)
            for row in rows:
                row.insert(0,k)
            output.extend(rows)
        return output
    return [[data]]

# Generates all the strings for our S3 file transfers
def generateS3Fileinfo(filename):
    s3key = _PATH + filename
    url = '/'.join([_BASE_URL, _BUCKET, s3key])
    return filename, s3key, url

# Initiates some globals TODO: this is not a great practice, should really use a design pattern to manage the global file transfer
def instantiateFileTransfer():
    global _TRANSFER
    client = boto3.client("s3", "us-west-2")
    _TRANSFER = S3Transfer(client)

# Outputs a file locally then transfers it to S3. Overwrites on collision
def outputFile(filebase, data):
    outputJSON(filebase, data)
    outputCSV(filebase, data)
    
# Outputs a file locally then transfers it to S3. Overwrites on collision
def outputJSON(filebase, data):
    filename,s3key,_ = generateS3Fileinfo(filebase + ".json")
    
    with open(filename, "w") as f:
        f.write(json.dumps(data))
    _TRANSFER.upload_file(filename, _BUCKET, s3key, extra_args={"ContentType":"application/json"})

# Outputs a file locally then transfers it to S3. Overwrites on collision
def outputCSV(filebase, data):
    data = dictToList(data)
    
    filename,s3key,_ = generateS3Fileinfo(filebase + ".csv")
    
    with open(filename, "w") as f:
        wr = csv.writer(f, quoting=csv.QUOTE_ALL)
        for row in data:
            wr.writerow(row)
    _TRANSFER.upload_file(filename, _BUCKET, s3key, extra_args={"ContentType":"application/json"})


# ## Spark: get pings, filter them, and count the permutations

# In[4]:

# Get pings
start_date = _START or dt.strftime(dt.utcnow() - timedelta(1 + _LOOKBACK), "%Y%m%d")
end_date   = _END   or dt.strftime(dt.utcnow() - timedelta(1),             "%Y%m%d")

pings = get_pings(
    sc, 
    doc_type        = "heartbeat", 
    app             = "Firefox", 
    channel         = "release", 
    submission_date = (start_date, end_date), 
    fraction        = _FRAC
)


# In[5]:

# Calculate recent data

# Munges ping into a tuple (surveyId, submissioinDate, heartbeatStatus)
def munge_pings(ping):
    payload = ping.get("payload")
    if not payload:
        status = "unknown"
    elif payload.get("engagedTS"):
        status = "engaged"
    elif payload.get("votedTS"):
        status = "voted%s" % payload.get("score", "?")
    elif payload.get("expiredTS"):
        status = "expired"
    elif payload.get("closedTS"):
        status = "closed"
    elif payload.get("offeredTS"):
        status = "offered"
    else:
        status = "unknown"
    
    # Get ID and remove Telem ID
    surveyId = tryGet(ping,["payload","surveyId"], "unknown")
    if surveyId:
        surveyId = surveyId.split('::', 1)[0]
    
    return (
        surveyId,
        tryGet(ping,["meta","submissionDate"], "unknown"), 
        status
    )


try:
    recent_data = pings.map(lambda p: munge_pings(p)).countByValue()
except Exception as e:
    print(e)
recent_data




# ## Data Munging: merge with existing data, get into dict trees 

# In[6]:

# Get historical data
_,_,all_url = generateS3Fileinfo("all.json") # generate existing data url
all_dict = recDD()


existing_json = json.loads( urllib.urlopen(all_url).read()) # load existing data

copyToRecDD(all_dict, existing_json) # copy into a defaultdict instead of a dict.  This is nested so it needed a copy fn (I may just be ignorant of a more elegant solution)


# In[7]:

# Merge historical with recents, dump json
recent_dict = recDD()
max_date = "00000000"
for key_trie, count in recent_data.iteritems():
    max_date = key_trie[1] if key_trie[1] > max_date else max_date
    recent_dict[key_trie[1]][key_trie[0]][key_trie[2]] = count
    all_dict[key_trie[0]][key_trie[1]][key_trie[2]] = count

recent_dict


# ## Data Export: Initialization, Local Output, AWS S3 Output

# In[8]:

instantiateFileTransfer()


# In[9]:

# Output All
outputFile("all", all_dict)


# In[10]:

# Output Recent
for date, entry in recent_dict.iteritems():
    outputFile(date,entry)

outputFile('latest',recent_dict[max_date])



# In[11]:

"w0000000t"


# In[ ]:



