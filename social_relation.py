import json
import tldextract
import time
import twitter
from twython import Twython, TwythonError
from pymongo import MongoClient
import codecs
import os.path

def getFollowers(screen_name, streams, out_file):
    print 'Getting followers of {}'.format(screen_name)
    f_out = open(out_file, 'w+')
    timers = dict()
    for i in range(0, len(streams)):
        timers[i] = [0, 0]
    total = 0
    next_ = -1
    while next_ :
        # Only retrive 1500 followers
        if total>1500:
            break
        try:
            result = -1
            while result == -1:
                for i in range(0, len(streams)):
                    if timers[i][1] == 15 and timers[i][0] + 900 < time.time():
                        timers[i][0] = 0
                        timers[i][1] = 0
                    if timers[i][0] == 0:
                        result = i
                        break
                    elif timers[i][1] < 15:
                        result = i
                        break
                if result == -1:  # case when all streams are rate limited
                    print 'sleeping for 300 seconds.'
                    time.sleep(300)

            if timers[result][0] == 0:
                timers[result][0] = time.time()
                timers[result][1] = 1
            elif timers[result][1] < 15:
                timers[result][1] += 1
            user_follower = streams[result].get_followers_list(screen_name=screen_name,cursor =next_) ##change to get_friends_list if you want to crawl friends
            user_follower_u = user_follower['users']
            for uf in user_follower_u:
                f_out.write(json.dumps(uf)+'\n')
            next_ = user_follower['next_cursor']
            total+=len(user_follower_u)
            print str(total)

        except TwythonError as e:
            if e.error_code==403:
                print str(screen_name) + "\t" + 'suspend'
                break
            elif e.error_code==401:
                print str(screen_name) + "\t" + 'unauthorized'
                break
            elif e.error_code==404:
                print str(screen_name) + "\t" + 'non-exist'
                break
            elif e.error_code==429:
                timers[result][1] = 15
                timers[result][0] = time.time() - 450
            pass
        except KeyError as k:
            print k
            pass
    f_out.close()
    print '{} done with {} followers'.format(screen_name, total)

if __name__=="__main__":
    streams = []
    with open('./keys_1.txt', 'r') as fKeysIn:
        for line in fKeysIn:
            print line
            line = line.rstrip().split('\t')
            streams.append(Twython(line[0], line[1], line[2], line[3]))
    screen_name = 'bigcrisisdata'
    out_file = 'followes.json'
    getFollowers(screen_name, streams, out_file)