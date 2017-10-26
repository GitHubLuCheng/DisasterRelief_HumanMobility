import json
import time
from twython import Twython, TwythonError
import sparker
def getFollowers(screen_name, streams, fetch_type,f_out):
    # print 'Getting {} of {}'.format(fetch_type,screen_name)
    timers = dict()
    for i in range(0, len(streams)):
        timers[i] = [0, 0]
    total = 0
    next_ = -1
    while next_:
        # Only retrive 1500 followers
        if total > 1500:
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
            if fetch_type == "follower":
                user_follower = streams[result].get_followers_list(screen_name=screen_name,
                                                                   cursor=next_)  ##change to get_friends_list if you want to crawl friends
                print user_follower
                user_follower_u = user_follower['users']
                for uf in user_follower_u:
                    uf["source_screen_name"] = screen_name
                    f_out.write(json.dumps(uf) + '\n')
                next_ = user_follower['next_cursor']
                total += len(user_follower_u)
                print str(total)
            elif fetch_type == "following":
                user_follower = streams[result].get_friends_list(screen_name=screen_name,
                                                                 cursor=next_)  ##change to get_friends_list if you want to crawl friends
                print user_follower
                user_follower_u = user_follower['users']
                for uf in user_follower_u:
                    uf["source_screen_name"] = screen_name
                    f_out.write(json.dumps(uf) + '\n')
                next_ = user_follower['next_cursor']
                total += len(user_follower_u)
                print str(total)
            else:
                print "error"

        except TwythonError as e:
            if e.error_code == 403:
                print str(screen_name) + "\t" + 'suspend'
                break
            elif e.error_code == 401:
                print str(screen_name) + "\t" + 'unauthorized'
                break
            elif e.error_code == 404:
                print str(screen_name) + "\t" + 'non-exist'
                break
            elif e.error_code == 429:
                timers[result][1] = 15
                timers[result][0] = time.time() - 450
            pass
        except KeyError as k:
            print k
            pass
    print 'done with {} {}'.format(total,fetch_type)

# To fetch the name list from the file list
def fetch_source_list(input_root,key):
    return sparker.fetch_name_list(input_root,key)


if __name__ == "__main__":
    streams = []
    with open('keys_1.txt', 'r') as fKeysIn:
        for line in fKeysIn:
            print line
            line = line.rstrip().split(' ')
            streams.append(Twython(line[0], line[1], line[2], line[3]))
    out_file_follower = 'followers.json'
    out_file_following = 'followings.json'
    name_list_path = "spark-warehouse/user_list.json"
    key = "user_name"
    f_out_follower = open(out_file_follower, 'w')
    f_out_following = open(out_file_following,'w')
    user_name_source = fetch_source_list(name_list_path,key)
    for screen_name in user_name_source:
        getFollowers(screen_name["user_name"], streams, "follower", f_out_follower)
        getFollowers(screen_name["user_name"], streams, "following", f_out_following)
    f_out_following.close()
    f_out_follower.close()
