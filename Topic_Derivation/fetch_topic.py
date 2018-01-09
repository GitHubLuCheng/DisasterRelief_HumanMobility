# -*- coding: utf-8 -*-
import math
import numpy as np
import re
from operator import add

from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from pyspark import SparkConf
from pyspark.sql import Row
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("Python Spark SQL").setMaster("local[4]").set('spark.debug.maxToStringFields', 100).set(
    'spark.sql.shuffle.partitions', 20)
spark = SparkSession.builder.appName("Tweet Analytics").config(conf=conf).getOrCreate()


def word_process(text):
    # remove the non ASCII word
    temp1 = ''.join(i if ord(i) < 128 else '' for i in text)
    # remove the urls in the text
    temp2 = re.sub(r"https\S+", "", temp1)
    temp2 = re.sub(r"http\S+", "", temp2)
    # remove the new line syntax in the string
    temp3 = temp2.replace("\n", " ")
    # remove the punctuation from the string
    # remove the stop word by nltk
    # Then stem the words
    stop_words = set(stopwords.words())
    # print stop_words
    filtered_words = []
    ps = PorterStemmer()
    # print word_tokenize(temp4)
    for w in word_tokenize(temp3):
        # remove those words with length less than 2
        if len(w) > 2 and w not in stop_words:
            filtered_words.append(str(ps.stem(w)))
    return filtered_words


def get_all_terms(query_result):
    word_list = []
    for record in query_result:
        for word in record.text:
            word_list.append(word)
    return word_list


def get_term_set(query_result):
    word_set = set(get_all_terms(query_result))
    return word_set


def construct_mapping_matrix(query_result):
    print "constructint the tweet_tweet mapping matrix..."
    indice = 0
    indice_tweet_table = {}
    for record in query_result:
        indice_tweet_table[indice] = record.id_str
        indice += 1
    # print indice_tweet_table
    print "Done!"
    return indice_tweet_table


def construct_mapping_matrix_for_word(word_set):
    print "construct mapping matrix for word with tweet..."
    indice = 0
    indice_word_table = {}
    for word in word_set:
        indice_word_table[indice] = word
        indice += 1
    print "Done!"
    return indice_word_table


def get_sim(text_i, text_j):
    text_sum = text_i + text_j
    sum_size = len(text_sum)
    # print text_sum
    # print set(text_sum)
    if sum_size == 0:
        return 0.0
    else:
        # print len(text_sum)
        # print len(set(text_sum))
        # print float(len(text_sum) - len(set(text_sum))) / float(len(text_sum))
        return 2 * float(len(text_sum) - len(set(text_sum))) / float(len(text_sum))


def get_m(mention_i, mention_j):
    i_list = [record.screen_name for record in mention_i]
    j_list = [record.screen_name for record in mention_j]
    mention_sum = i_list + j_list
    mention_size = len(mention_sum)
    # print mention_sum
    # print set(mention_sum)
    if mention_size == 0:
        return 0.0
    else:
        return 2 * float(len(mention_sum) - len(set(mention_sum))) / float(len(mention_sum))


def get_score(tweet_i, tweet_j):
    if tweet_i.id_str == tweet_j.id_str:
        return 0.0
    if tweet_i.reply and tweet_j.reply:
        if tweet_i.reply == tweet_j.reply:
            return 1.0
        elif tweet_i.screen_name == tweet_j.reply and tweet_i.reply == tweet_j.screen_name:
            return 1.0
    sim = get_sim(tweet_i.text, tweet_j.text)
    m = get_m(tweet_i.user_mentions, tweet_j.user_mentions)
    return (sim + m) - sim * m


def tweet_to_tweet(mapping_matrix, query_result):
    print "Construct tweet to tweet relation matrix...!"
    mm_size = len(mapping_matrix)
    tt_matrix = np.zeros((mm_size, mm_size), dtype=np.float)  # the m*m size matrix for tweet to tweet
    for i in range(mm_size):
        tweet_i = query_result[i]
        for j in range(mm_size):
            #  construct the matrix value
            tt_matrix[i, j] = get_score(tweet_i, query_result[j])
    print "Done!"
    return tt_matrix


def tf(word, text):
    word_count = float(text.count(word))
    text_length = float(1 + len(text))
    return word_count / text_length


def idf(word, result_length, word_dict):
    return float(math.log(result_length / word_dict[word]))


def tf_idf(word, text, result_length, word_dict):
    return tf(word, text) * idf(word, result_length, word_dict)


# TODO
def get_keyval(row):
    words = row.text
    return [[w, 1] for w in set(words)]


def get_word_count(tweet_frame):
    mapped_rdd = tweet_frame.rdd.flatMap(lambda row: get_keyval(row))
    counts_add = mapped_rdd.reduceByKey(add)
    word_count = counts_add.collect()
    # print dict(word_count)
    return dict(word_count)


def tweet_to_term(tweet_matrix, word_matrix, query_result, word_dict):
    print "Constructing tweet_to_term matrix ..."
    t_size = len(tweet_matrix)
    w_size = len(word_matrix)
    result_length = len(query_result)
    tw_matrix = np.zeros((t_size, w_size), dtype=np.float)  # the m*n size matrix for tweet to word
    for i in range(t_size):
        tweet = query_result[i]
        for j in range(w_size):
            word = word_matrix[j]
            if word in tweet.text:
                tw_matrix[i, j] = tf_idf(word, tweet.text, result_length, word_dict)
    print "Done!"
    return tw_matrix  # the word with a larger value indicates it is more important


def object_function(di, dj, alpha):
    return di + alpha * dj


#
def NMijF_jp(tt_matrix, tw_matrix, k_topic, alpha):
    print "Entering NMijF_jp process..."
    tt_shape = tt_matrix.shape
    tw_shape = tw_matrix.shape
    W = np.random.rand(tt_shape[0], k_topic)
    Y = np.random.rand(k_topic, tt_shape[1])
    H = np.random.rand(k_topic, tw_shape[1])
    observation = 0
    for i in range(30):
        print "Fatoring..."+str(float(i)/30.0) + "%"
        W = NMF.update_a(W, Y, tt_matrix)
        Y = NMF.update_x(W, Y, tt_matrix)
        H = NMF.update_x(W, H, tw_matrix)
        di = NMF.divergence_function(tt_matrix, np.dot(W, Y))
        dj = NMF.divergence_function(tw_matrix, np.dot(W, H))
        if (object_function(di, dj, alpha) - observation) < 0.001:
            break
        observation = object_function(di, dj, alpha)
    # print "tweet - topic matrix  \n" + str(W)
    # print "topic - tweet matrix  \n" + str(Y)
    # print "topic - word matrix  \n" + str(H)
    print "Done!"
    return [W, H]


def get_topk_topic_word(topic_word_martix, matrix_for_word, topic, top_k):
    print "Get_topk_topic_word..."
    result = []
    for topic_indice in topic:
        topic_word = []
        for word_indice in np.argsort(topic_word_martix[topic_indice])[-top_k:]:
            topic_word.append(matrix_for_word[word_indice])
        result.append(topic_word)
    print "Done!"
    return result


def get_topk_topic(tweet_topic_matrix, k):
    print "Get_top_k topic..."
    topic_matrix = np.sum(tweet_topic_matrix, axis=0)
    sorted_index = np.argsort(topic_matrix)
    print "Done!"
    return sorted_index[-k:]


def fetch_topic(input_path, save_path, k_topic, top_k, k_word, alpha):
    df = spark.read.json(input_path)
    df.createOrReplaceTempView('text_view')
    # STEP 1
    #  remove the stop words
    #  remove the emotions
    #  remove the terms with two or less characters
    #  remove punctuations
    #  remove the urls
    #  stem the words
    # STEP 2
    #  construct the tweet to tweet matrix
    #  calculate the matrix based on the equation of the paper
    #  calculate the similarity between two tweets (TF-IDF)
    # STEP 3
    #  Non-Negative Matrix inter-join Factorization

    # STEP 4
    # pick the top-k possible topic from the maxtrix W(tweet to topic after being clustered)
    # pick the relate word form the matrix V (topic to word)
    temp_result = []
    query = 'SELECT id_str,user.screen_name,text, entities.user_mentions,entities.hashtags.text as hashtags,in_reply_to_screen_name AS reply,created_at,timestamp_ms FROM text_view WHERE lang = \'en\''
    temp_query = spark.sql(query).dropDuplicates().collect()
    row_schema = Row('id_str', 'screen_name', 'text', 'user_mentions', 'hashtags', 'reply', 'created_at',
                     'timestamp_ms')
    for record in temp_query:
        temp_result.append(
            row_schema(record.id_str, record.screen_name, word_process(record.text), record.user_mentions,
                       record.hashtags, record.reply, record.created_at, record.timestamp_ms))
    temp_frame = spark.createDataFrame(temp_result)

    word_count_mapping = get_word_count(temp_frame)

    ttmm = construct_mapping_matrix(temp_result)

    twmm = construct_mapping_matrix_for_word(get_term_set(temp_result))

    tweet_tweet_matrix = tweet_to_tweet(ttmm, temp_result)

    tweet_term_matrix = tweet_to_term(ttmm, twmm, temp_result, word_count_mapping)
    result_list = NMijF_jp(tweet_tweet_matrix, tweet_term_matrix, k_topic, alpha)
    top_k_topic = get_topk_topic(result_list[0], top_k)
    topic_word = get_topk_topic_word(result_list[1], twmm, top_k_topic, k_word)

    topics = []
    rank = 1
    topic_schema = Row('rank', 'topic_word')
    for record in topic_word:
        topics.append(topic_schema(rank,record))
        rank +=1
    result_frame =  spark.createDataFrame(topics)
    result_frame.write.save(save_path, format='json')

    print " fetch topic from the tweet done"
