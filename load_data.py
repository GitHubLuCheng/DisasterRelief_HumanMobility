import json
import tweet


# Get data from the path name and generate data list
def get_data_pool(path):
    tweet_pool = []
    with open(path,'r') as json_data:
        res = json_data.readline()
        # Do loop until reaching the end of json file
        while res != "":
            res_json_object = json.loads(res)
            if res_json_object["coordinates"] is not None:
                tweet_pool.append(
                    tweet.Tweet(
                        res_json_object["id_str"],
                        res_json_object["user"]["id"],
                        res_json_object["user"]["name"],
                        res_json_object["text"],
                        res_json_object["geo"]["coordinates"][0],
                        res_json_object["geo"]["coordinates"][1],
                        res_json_object["timestamp_ ms"]
                    ))
            res = json_data.readline()
    print "Data length of object: "+str(len(tweet_pool))+"!"
    return tweet_pool

