import tweepy
from multiprocessing import Process


def get_apis_from_file(path):
    api_list = []
    with open(path, 'r') as fKeysIn:
        for line in fKeysIn:
            print line
            line = line.rstrip().split(' ')
            auth = tweepy.OAuthHandler(line[0], line[1])
            auth.set_access_token(line[2], line[3])
            api_list.append(tweepy.API(auth))
    return api_list


def get_limit_status(api):
    print api.rate_limit_status()


def crawl_friendships(api, name_list):
    try:
        length = len(name_list)
        for i in range(0,length,1):
            s_s_name = name_list[i]
            for j in range(i+1,length,1):
                t_s_name = name_list[j]
                print "Index  " + str(i)
                print api.show_friendship(source_screen_name=s_s_name, target_screen_name=t_s_name)
    except tweepy.error.RateLimitError:
        print "Time rate limit reach."
        return None


if __name__ == "__main__":
    key_path = 'keys_1.txt'
    source_sreen_name = 'JiayongM'
    target_screen_name = 'ASU'
    apis = get_apis_from_file(key_path)
    name_list = ["JiayongM","ASU"]
    length = len(name_list)
    for api in apis:
        print api.followers("JiayongM")

        # for api in read_keys_from_file(key_path):
        #     print "------------------Change api now------------------------"
        #     for i in range(180):
        #         print "Index  "+str(i)
        #         crawl_friendships(api, source_sreen_name, target_screen_name)
        # try:
        #     for i in range(181):
        #         friendship = api.show_friendship(source_screen_name="JiayongM", target_screen_name="ASU")
        #         print "api 1 "+ str(i)
        #         print friendship
        # except tweepy.error.RateLimitError:
        #     for j in range(181):
        #         friendship2 = api2.show_friendship(source_screen_name="JiayongM", target_screen_name="ASU")
        #         print "api 2 "+ str(j)
        #         print friendship2
        # public_tweets = api.home_timeline()
        # for tweet in public_tweets:
        #     print tweet.user.screen_name
