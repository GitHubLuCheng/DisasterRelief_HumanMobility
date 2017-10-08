import load_data
import filter_data
import export_userprofile
import os
import json

source_root = '/home/garden/Desktop/Data Storm/DataSource/'
geo_filter_root = '/home/garden/Desktop/Data Storm/DataSource/geo_filtered/'
no_geo_file_root = '/home/garden/Desktop/Data Storm/DataSource/no_geo_data/'
dup_filter_root = '/home/garden/Desktop/Data Storm/DataSource/dup_filtered/'
ep_data_root = '/home/garden/Desktop/Data Storm/DataSource/exported_data/'
DATA_TYPE = '.json'
test_file_list = ['section_test.json', 'section_test_source.json']
original_file_list = [
    'HurricaneHarveyGeoMississippi.json',
    'HurricaneHarvy.json',
    'HurricaneHarveyGeo.json',
    'HurricaneHarveyGeoFlorida.json',
]


# filename_ouput_1 = 'HurricaneHarveyGeoMississippi_geo_filtered.json'
# filename_ouput_2 = 'HurricaneHarveyGeo_geo_filtered.json'
# filename_ouput_3 = 'HurricaneHarveyGeoFlorida_geo_filtered.json'
# filename_ouput_4 = 'section_test_geo_filtered.json'
# filename_ouput_5 = 'section_test_geo_filtered.json'
# filename_ouput_6 = 'section_test_geo_source_filtered.json'
def validate(input_path):
    with open(input_path,'r') as input_file:
        count = 0
        data_str = input_file.readline()
        while data_str != '':
            json.loads(data_str)
            count +=1
            data_str = input_file.readline()
    print "Validating finished with "+ str(count)+" records."
def geo_filt():
    for record in original_file_list:
        input_path = source_root + record
        output_path = geo_filter_root + record.split('.')[0] + '_geo' + DATA_TYPE
        filter_data.geo_filter(input_path, output_path)
        validate(output_path)
def no_geo_filt():
    for record in original_file_list:
        input_path = source_root+record
        output_path = no_geo_file_root + record.split('.')[0] + '_no_geo' + DATA_TYPE
        filter_data.no_geo_filter(input_path,output_path)
        validate(output_path)
def duplicate_filt(input_root,output_root,sufix='_dup'):
    for filename in os.listdir(input_root):
        input_path = input_root + filename
        output_path = output_root + filename.split('.')[0] + sufix + DATA_TYPE
        filter_data.duplicate_filter(input_path, output_path)
        validate(output_path)

def generate_userprofile():
    for filename in os.listdir(dup_filter_root):
        input_path = dup_filter_root+filename
        output_path = ep_data_root+filename.split('.')[0] + '_exp' + DATA_TYPE
        export_userprofile.export_geo_data(input_path,output_path)



# export_userprofile.export_geo_data('/home/garden/Desktop/Data Storm/DataSource/dup_filtered/HurricaneHarvy_geo_dup.json','/home/garden/Desktop/Data Storm/DataSource/test.json')
# generate_userprofile()
# validate('/home/garden/Desktop/Data Storm/DataSource/section_test_source_filtered.json')
# filter_data.geo_filter(source_root+'section_test_source.json',dup_filter_root+'section_test_source.json')
# geo_filt()
# no_geo_filt()
# duplicate_filt()
duplicate_filt(no_geo_file_root,dup_filter_root,'_dup')
# load_data.get_data_pool(geo_filter_root+test_file_list[1])
# load_data.get_data_pool(source_root+filename_ouput_3)
# export_geo_data('/home/garden/Desktop/Data Storm/DataSource/section_test_source.json','/home/garden/Desktop/Data Storm/DataSource/test.json')
# validate('/home/garden/Desktop/Data Storm/DataSource/test.json')


# export_userprofile.export_geo_data(dup_filter_root+'HurricaneHarvy_geo_dup.json','/home/garden/Desktop/Data Storm/DataSource/test.json')
# validate('/home/garden/Desktop/Data Storm/DataSource/test.json')
