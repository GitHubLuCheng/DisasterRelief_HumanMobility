from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row

conf = SparkConf().setAppName("Python Spark SQL").setMaster("local[4]").set('spark.debug.maxToStringFields', 100)
spark = SparkSession.builder.appName("Tweet Analytics").config(conf=conf).getOrCreate()

# print(spark.conf.get('spark.debug.maxToStringFields'))
# df = spark.read.json("/home/garden/Desktop/Data Storm/DataSource/HurricaneHarvy.json")

df = spark.read.parquet("textAndNames.parquet")
result = df.select("name", split(df.text, ' ').alias('text_set')).collect()

# Save a file by spark
# df.select("text","user.name").write.save("textAndNames.parquet", format="parquet")
# df.printSchema()
# df.show()
df.createOrReplaceTempView("tweet")
tweetTextDF = spark.sql("SELECT text FROM tweet")


# tweetNameDF = spark.sql("SELECT user.name,COUNT(*) AS tweetcount FROM tweet GROUP BY user.name")
# tweetNameDF.write.save("nameAndCount.json",format="json")

# Person = Row('name','age')
# persons =[Person('jack',11),Person('jack',11),Person('Tom',21)]
# df2 = spark.createDataFrame(persons)
# df2.select('name').show()

def create_schema(data_path, sql_sentence, save_file):
    df = spark.read.json(data_path)
    df.createOrReplaceTempView("tweetdataframe")
    result = spark.sql(sql_sentence)
    result.write.save(save_file, format="json")
    print "Create Schema " + sql_sentence + " Done!"

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

def count_word_frequncy(data_path, save_file):
    df = spark.read.json(data_path)
    df.createOrReplaceTempView("uid_tid_word")
    result = spark.sql("SELECT user_id,tweet_id,word,count(*) AS frequency FROM uid_tid_word GROUP BY user_id,tweet_id,word")
    result.write.save(save_file,format="json")
    print ("Count word frequency Done")

in_data_path = "/home/garden/Desktop/Data Storm/DataSource/HurricaneHarveyGeoMississippi.json"
saved_file = "spark-warehouse/uid_tid_text.json"

# The Schema to record the tweet_id post time and tweet_location
# create_schema(in_data_path,"SELECT user.id as user_id,id as tweet_id,text FROM tweetdataframe","spark-warehouse/uid_tid_text.json")

# To split the text sentence into word
# splited_word("spark-warehouse/uid_tid_text.json", "spark-warehouse/splited_uid_tid_word.json")

# To count the word frequencu for the data
count_word_frequncy("spark-warehouse/splited_uid_tid_word.json","spark-warehouse/uid_tid_word_frequency.json")

#To create the schema for tweet_id, time, geo_location information
# create_schema(in_data_path,
#               "SELECT id as tweet_id,timestamp_ms,geo.coordinates as location FROM tweetdataframe",
#               "spark-warehouse/tweet_basic_info.json")
