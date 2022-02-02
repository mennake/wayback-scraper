"""

This is a very basic scraper for tweets archived on Wayback Machine that handles most tweets from
November 2011 onward. It is rather slow and clunky and has plenty of room for improvement.
To download an account's archived tweets:

python3 wayback.py [account]

i.e. to download archived tweets for @DrunkAlexJones:

python3 wayback.py DrunkAlexJones

This will create a DrunkAlexJones_wayback_tweets.csv file containing the archived tweets, as well as 
a DrunkAlexJones_wayback/ directory containing the raw HTML of the archives (which can get large).

Required libraries: bs4, pandas, requests

"""


import bs4
import json
import os
import pandas as pd
import requests
import sys
import time

# utility functions
def snowflake2utc (sf):
    return (sf >> 22) + 1288834974657

def get_retweet (text):
    if text.startswith ("RT @"):
        handle = text.split ()[1][1:]
        if handle.endswith (":"):
            return handle[:-1]
    return ""

# handlers for tweet HTML from a few different eras
def html1 (soup, elements, row):
    emoji = elements[0].find_all ("img", {"class" : "Emoji"})
    for e in emoji:
        e.replaceWith (e["alt"])
    text = elements[0].text.strip ()
    if len (text) == 0:
        return None
    else:
        row["text"] = text
        quote = elements[0].parent.find_next_sibling ("div")
        if quote is not None:
            try:
                qt_handle = quote.find ("span", {"class" : "username"}).text
                qt_text = quote.find ("div", {"class" : "QuoteTweet-text"}).text.strip ()
                row["quotedHandle"] = qt_handle.replace ("@", "")
                row["quotedText"] = qt_text
            except:
                row["quotedHandle"] = ""
                row["quotedText"] = ""
        reply_to = elements[0].parent.find_previous_sibling ("div")
        if reply_to is not None and "Replying to" in reply_to.text:
            try:
                reply_to_handle = reply_to.find ("span", {"class" : "username"}).text
                row["replyToHandle"] = reply_to_handle.replace ("@", "")
            except:
                row["replyToHandle"] = ""
    return row
    
def html2 (soup, elements, row):
    emoji = elements[0].find_all ("img", {"class" : "Emoji"})
    for e in emoji:
        e.replaceWith (e["alt"])
    text = elements[0].text.strip ()
    row["text"] = text
    row["quotedHandle"] = ""
    row["quotedText"] = ""
    if text.startswith ("@"):
        row["replyToHandle"] = text.split ()[0].replace ("@", "")
    return row
    
def html3 (soup, elements, row):
    element = elements[0].find ("p", {"class" : "js-tweet-text"})
    emoji = element.find_all ("img", {"class" : "Emoji"})
    for e in emoji:
        e.replaceWith (e["alt"])
    text = element.text.strip ()
    row["text"] = text
    row["quotedHandle"] = ""
    row["quotedText"] = ""
    if text.startswith ("@"):
        row["replyToHandle"] = text.split ()[0].replace ("@", "")
    return row
    
html_handlers = [["TweetTextSize--jumbo", "p", html1],
                ["TweetTextSize--26px", "p", html2],
                ["opened-tweet", "div", html3],
                ["preexpanded", "div", html3]]

def parse_html (text, row):
    soup = bs4.BeautifulSoup (text)
    for handler in html_handlers:
        elements = soup.find_all (handler[1], {"class" : handler[0]})
        count = len (elements)
        if count == 1:
            try:
                row = handler[2] (soup, elements, row)
                if row is not None:
                    return row
            except:
                pass
    return None

def parse_json (text, row):
    data = json.loads (text)
    row["text"] = data["text"]
    if "in_reply_to_screen_name" in data:
        row["replyToHandle"] = data["in_reply_to_screen_name"]
    if "quoted_status" in data:
        quote = data["quoted_status"]
        row["quotedText"] = quote["text"]
        row["quotedHandle"] = quote["user"]["screen_name"]
    return row

parsers = {"html" : parse_html, "json" : parse_json} 

handle = sys.argv[1]

# read list of captured URLS from from Wayback Machine
url = "https://web.archive.org/web/timemap/json?url=https://twitter.com/" + handle \
        + "&matchType=prefix&collapse=urlkey&output=json&fl=original,mimetype," \
        + "timestamp,endtimestamp,groupcount,uniqcount&filter=!statuscode:[45].." \
        + "&limit=1000000&_=" + str (int (time.time () * 1000))
r = requests.get (url)
arc_file = handle + "_wayback.json"
with open (arc_file, "w") as file:
    file.write (r.text)
    
# create a data frame of captured tweets
json_data = json.loads (r.text)
df = pd.DataFrame ([{"tweetURL"   : j[0],
                     "mime"       : j[1],
                     "t"          : j[2]} for j in json_data])
df = df[df["tweetURL"].str.contains ("status", regex=False)]
df["id"] = df["tweetURL"].apply (lambda u: u.split ("/status/")[-1])
df = df[df["id"].str.isnumeric ()]
df["id"] = df["id"].astype (int)
df = df[df["id"] > 292000000000000] #snowflake IDs only
df["archiveURL"] = df.apply (lambda r: "https://web.archive.org/web/" \
                                    + r["t"] + "/" + r["tweetURL"], axis=1)
print (str (len (df.index)) + " archived tweets to try")
df.to_csv (handle + "_wayback_urls.csv", index=False)

# make directory, loop through, and download captures
path = handle + "_wayback/"
if not os.path.exists (path):
    os.makedirs (path)
count = 0
errors = 0
for ix, row in df.iterrows ():
    retries = 3
    tweet_id = str (row["id"])
    mime = row["mime"]
    ftype = mime[mime.find ("/") + 1:]
    fname = path + tweet_id + "." + ftype
    if not os.path.exists (fname):
        while retries > 0:
            try:
                archive = row["archiveURL"]
                r = requests.get (archive, timeout=15)
                if len (r.text) == 0 or "<p>Job failed</p>" in r.text or \
                         "<p>The Wayback Machine has not archived that URL.</p>" in r.text:
                    print ("redirect or no content available " + tweet_id)
                    errors = errors + 1  
                    retries = 0
                elif "<p>You have already reached the limit of active sessions.</p>" in r.text or \
                        "<h1>504 Gateway Time-out</h1>" in r.text:
                    print ("too many recent requests, sleeping...")
                    time.sleep (15)
                else:
                    with open (fname, "w") as file:
                        file.write (r.text)
                    retries = 0
                    count = count + 1
                    if count % 100 == 0:
                        print (str (count) + " retrieved so far")
            except:
                time.sleep (5)
                retries = retries - 1
                if retries == 0:
                    errors = errors + 1
                    print ("error retrieving " + tweet_id)
    else:
        count = count + 1

# loop through captures and build CSV
count = 0
rows = []
for file in os.listdir (path):
    try:
        mime = file[file.find (".") + 1:]
        if mime not in parsers:
            mime = "html"
        tweet_id = int (file.replace ("." + mime, ""))
        utc = snowflake2utc (tweet_id)
        row = {"id" : tweet_id, "utcTime" : utc, "type" : mime}
        with open (path + file) as fp:    
            try:
                row = parsers[mime] (fp.read (), row)
            except:
                row = None
            if row is not None:
                rows.append (row)
                count = count + 1
                if count % 100 == 0:
                    print (str (count) + " parsed successfully")
            else:
                print ("error: " + file)
                errors = errors + 1
    except:
        print ("error: " + file)
        errors = errors + 1
df = pd.DataFrame (rows)
df["handle"] = handle
df["utcTime"] = pd.to_datetime (df["utcTime"], unit="ms")
print (str (len (df.index)) + " Wayback Machine captures parsed successfully")
print (str (errors) + " errors or missing captures")
columns = ["handle", "tweetID", "utcTime", "archiveURL", "text", "type",
           "quotedHandle", "quotedText", "replyToHandle", "retweetHandle"]
df["id"] = df["id"].astype (int)
df["tweetID"] = df["id"]
df0 = pd.read_csv (handle + "_wayback_urls.csv")
df0["id"] = df0["id"].astype (int)
df = df.merge (df0[["id", "archiveURL"]], on="id")
df["retweetHandle"] = df["text"].apply (get_retweet)
df = df.sort_values ("tweetID")[columns]
df.to_csv (handle + "_wayback_tweets.csv", index=False)
