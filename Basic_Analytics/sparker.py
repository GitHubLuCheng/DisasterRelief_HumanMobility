from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
import os

# print(spark.conf.get('spark.debug.maxToStringFields'))
# df = spark.read.json("/home/garden/Desktop/Data Storm/DataSource/HurricaneHarvy.json")

# df = spark.read.parquet("textAndNames.parquet")
# result = df.select("name", split(df.text, ' ').alias('text_set')).collect()

# Save a file by spark
# df.select("text","user.name").write.save("textAndNames.parquet", format="parquet")
# df.printSchema()
# df.show()
# df.createOrReplaceTempView("tweet")
# tweetTextDF = spark.sql("SELECT text FROM tweet")


# tweetNameDF = spark.sql("SELECT user.name,COUNT(*) AS tweetcount FROM tweet GROUP BY user.name")
# tweetNameDF.write.save("nameAndCount.json",format="json")

# Person = Row('name','age')
# persons =[Person('jack',11),Person('jack',11),Person('Tom',21)]
# df2 = spark.createDataFrame(persons)
# df2.select('name').show()

conf = SparkConf().setAppName("Python Spark SQL").setMaster("local[4]").set('spark.debug.maxToStringFields', 100).set(
    'spark.sql.shuffle.partitions', 1)
spark = SparkSession.builder.appName("Tweet Analytics").config(conf=conf).getOrCreate()


def create_schema(data_path, sql_sentence, save_file):
    df = spark.read.json(data_path)
    df.createOrReplaceTempView("tweetdataframe")
    result = spark.sql(sql_sentence)
    result.write.save(save_file, format="json")
    print "Create Schema " + sql_sentence + " Done!"


def count_word_frequncy(data_path, save_file):
    df = spark.read.json(data_path)
    df.createOrReplaceTempView("uid_tid_word")
    result = spark.sql(
        "SELECT user_id,tweet_id,word,count(*) AS frequency FROM uid_tid_word GROUP BY user_id,tweet_id,word")
    result.write.save(save_file, format="json")
    print ("Count word frequency Done")


def splited_word(data_path, save_file):
    df = spark.read.json(data_path)
    result = df.select("user_id", "tweet_id", split(df.text, ' ').alias('text_set')).collect()
    export_result = []
    USER_TWEET_WORD = Row("user_id", "tweet_id", "word")
    for record in result:
        for word in record.text_set:
            export_result.append(USER_TWEET_WORD(record.user_id, record.tweet_id, word))
    spark.createDataFrame(export_result).write.save(save_file, format="json")
    print("Splited word Done")


def significant_location(input_root, save_file, scale_digit):
    for filename in os.listdir(input_root):
        df = spark.read.json(input_root + filename)
        df.createOrReplaceTempView("geo_view")
        sql = "SELECT geo.coordinates as geo FROM geo_view"
        query_result = spark.sql(sql).collect()
        temp_result = []
        temp_row = Row("latitude", "longtitude")
        for record in query_result:
            temp_result.append(temp_row(record.geo[0], record.geo[1]))
        temp_frame = spark.createDataFrame(temp_result)
        temp_frame.select(round("longtitude", scale_digit).alias('longtitude'),
                          round("latitude", scale_digit).alias('latitude')).createOrReplaceTempView('cluster')
        sql = "SELECT latitude,longtitude,count(*) as amount FROM cluster GROUP BY latitude,longtitude"
        spark.sql(sql).createOrReplaceTempView('location')
        sql = "SELECT * FROM location WHERE amount > "
        spark.sql(sql).write.save("spark-warehouse/" + save_file + "_" + filename, format="json")
    print "Count significant location done!"


def language_distribution(input_root, save_file):
    for filename in os.listdir(input_root):
        df = spark.read.json(input_root + filename)
        df.createOrReplaceTempView("language_view")
        query = "select lang,count(*) as amount from language_view GROUP BY lang"
        spark.sql(query).write.save("spark-warehouse/" + save_file + "_" + filename, format="json")
    print "Language distribution done"


def temperal_inforamtion(input_root, save_file):
    for filename in os.listdir(input_root):
        df = spark.read.json(input_root + filename)
        temp_query = df.select("created_at").collect()
        temp_result = []
        row_schema = Row("date_time")
        for record in temp_query:
            date_time = record["created_at"].split(' ')
            hour = date_time[0] + ' ' + date_time[1] + ' ' + date_time[2] + ' ' + date_time[3].split(':')[0]
            temp_result.append(row_schema(hour))
        temp_frame = spark.createDataFrame(temp_result)
        temp_frame.createOrReplaceTempView("timestamp_view")
        query = "select date_time,count(*) as amount FROM timestamp_view GROUP BY date_time"
        spark.sql(query).write.save("spark-warehouse/" + save_file + "_" + filename, format="json")
    return "Temperal_Information done"


def fetch_name_list(input_path, key):
    df = spark.read.json(input_path)
    return df.select(key).distinct().collect()


def fetch_topic(input_path, save_path):
    df = spark.read.json(input_path)
    df.createOrReplaceTempView('text_view')
    query = 'SELECT id_str,user.screen_name,text, entities.user_mentions,in_reply_to_screen_name AS reply,created_at,timestamp_ms FROM text_view WHERE lang = \'en\''
    result = spark.sql(query).collect()
    # STEP 1
    # TODO remove the stop words
    # TODO remove the emotions
    # TODO remove the terms with two or less characters
    # TODO remove punctuations
    # TODO remove the urls
    # TODO stem the words
    # TODO calculate the similarity between two tweets (TF-IDF)

    # STEP 2
    # TODO construct the tweet to tweet matrix
    # TODO calculate the matrix based on the equation of the paper

    # STEP 3
    # TODO Non-Negative Matrix inter-join Factorization

    # STEP 4
    # TODO pick the top-k possible topic from the maxtrix W(tweet to topic after being clustered)
    # TODO pick the relate word form the matrix V (topic to word)

    spark.sql(query).write.save("spark-warehouse/"+save_path,format="json")
    return "Text query done"

in_data_path = "/home/garden/Desktop/Data Storm/DataSource/HurricaneHarvy.json"
saved_file = "spark-warehouse/uid_tid_text.json"

# The Schema to record the tweet_id post time and tweet_location
# create_schema(in_data_path,"SELECT user.id as user_id,user.name as user_name,id as tweet_id,text FROM tweetdataframe","spark-warehouse/uid_tid_text.json")

# To fetch the user list (twitter user screen name) from the data set
# create_schema(in_data_path,"SELECT user.screen_name as screen_name,user.followers_count as followers_count,user.friends_count as friends_count FROM tweetdataframe","spark-warehouse/user_list.json")

# To split the text sentence into word
# splited_word("spark-warehouse/uid_tid_text.json", "spark-warehouse/splited_uid_tid_word.json")

# To count the word frequencu for the data
# count_word_frequncy("spark-warehouse/splited_uid_tid_word.json","spark-warehouse/uid_tid_word_frequency.json")

# To create the schema for tweet_id, time, geo_location information
# create_schema(in_data_path,
#               "SELECT id as tweet_id,timestamp_ms,geo.coordinates as location FROM tweetdataframe",
#               "spark-warehouse/tweet_basic_info.json")


# To fetch the text from the json data file
fetch_topic(in_data_path,"User_Text.json")

# Count the sinificant location information
GEO_INPUT = "/home/garden/PycharmProjects/DisasterRelief_HumanMobility/Input_Data/geo/"
NO_GEO_INPUT = "/home/garden/PycharmProjects/DisasterRelief_HumanMobility/Input_Data/no_geo/"
# significant_location(GEO_INPUT,"s_l",3)
# language_distribution(GEO_INPUT,"l_d")
# temperal_inforamtion(GEO_INPUT, "t_i")
