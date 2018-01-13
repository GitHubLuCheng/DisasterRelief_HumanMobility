import fetch_topic as ft

# sampe test case 1 with 40 tweets
# sample_input_path = "/home/garden/PycharmProjects/DisasterRelief_HumanMobility/SPARK_Module/test_data/sample.json"
# sample_saved_path = "sample_filtered2.json"
# ft.fetch_topic(sample_input_path, sample_saved_path, 10, 4, 4, 0.1)

# test case 2 10 M
HurricaneHarvy_geo_dup_input = "/home/garden/Desktop/Data Storm/DataSource/dup_filtered (copy)/HurricaneHarvy_geo_dup.json"
HurricaneHarvy_geo_dup_output = "HurricaneHarvy_geo_dup2.json"
ft.fetch_topic(HurricaneHarvy_geo_dup_input, HurricaneHarvy_geo_dup_output, 30, 5, 5, 0.1)

# HurricaneHarveyGeoLouisiana test case
# HurricaneHarveyGeoLouisiana_input = "/home/garden/Desktop/Data Storm/DataSource/dup_filtered/HurricaneHarveyGeoLouisiana_geo_dup.json"
# HurricaneHarveyGeoLouisiana_output = "HurricaneHarveyGeoLouisiana.json"
# k_topic = 30 # The number of topics would we like to dericve from the tweets
# top_k = 5 # Get top k topic.
# top_k_word = 5 # Get the top k word from the topic.
# alpha = 0.1 # the ratio about how important the teet_term matrix is.
# ft.fetch_topic(HurricaneHarveyGeoLouisiana_input, HurricaneHarveyGeoLouisiana_output, k_topic, top_k, top_k_word, alpha)
