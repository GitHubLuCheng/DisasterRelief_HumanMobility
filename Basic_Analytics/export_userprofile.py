import load_data
import json
# Generate data set of user_data and location
def export_geo_data(input_path, output_path):
    data_pool = load_data.get_data_pool(input_path)
    with open(output_path, 'w') as output_file:
        output_file.write('[')
        list_length = len(data_pool)
        while list_length != 0:
            user_profile = {}
            tweet_item_list = []
            index = 0
            index2 = 0
            is_first_item = True
            tweet = data_pool[index]
            while index2 != list_length:
                tweet2 = data_pool[index2]
                if is_first_item:
                    user_profile["u_id"] = tweet.u_id
                    tweet_item_list.append({"t_id": tweet.t_id, "text": tweet.text,
                    "lat": tweet.lat, "lng": tweet.lng, "t_time": tweet.t_time})
                    user_profile["u_name"] = tweet.u_name
                    user_profile["tweets"]=tweet_item_list
                    data_pool.pop(0)
                    list_length -= 1
                    is_first_item = False
                    continue
                if tweet2.u_id == user_profile["u_id"]:
                    user_profile["tweets"].append({"t_id": tweet2.t_id, "text": tweet2.text,
                    "lat": tweet2.lat, "lng": tweet2.lng, "t_time": tweet2.t_time})
                    data_pool.pop(index2)
                    list_length -= 1
                    continue
                index2 += 1
            if list_length != 0:
                output_file.write(json.dumps(user_profile)+',')
            else:
                output_file.write(json.dumps(user_profile)+']')

    print "Exported user profile is done!"
    output_file.close()
