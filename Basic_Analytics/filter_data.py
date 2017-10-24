import json

# Filter data with valid geo tag
def geo_filter(input_path, output_path):
    out_file = open(output_path, 'w')
    with open(input_path, 'r') as source_file:
        string_data = source_file.readline()
        while string_data != '':
            json_object = json.loads(string_data)
            if json_object["geo"] is not None:
                out_file.write(string_data)
            string_data = source_file.readline()
    print "Geo Filter Done!"
    source_file.close()
    out_file.close()
#Filter data with invalid geo tag
def no_geo_filter(input_path, output_path):
    out_file = open(output_path, 'w')
    with open(input_path, 'r') as source_file:
        string_data = source_file.readline()
        while string_data != '':
            json_object = json.loads(string_data)
            if json_object["geo"] is None:
                out_file.write(string_data)
            string_data = source_file.readline()
    print "No Geo Filter Done!"
    source_file.close()
    out_file.close()

#Filter those data with duplicate data
def duplicate_filter(input_path, output_path):
    out_file = open(output_path, 'w')
    data_pool = []
    with open(input_path, 'r') as source_file:
        string_data = source_file.readline()
        while string_data != '':
            json_object = json.loads(string_data)
            id_str = json_object["id_str"]
            if id_str not in data_pool:
                data_pool.append(id_str)
                out_file.write(string_data)
            string_data = source_file.readline()
    print "Duplicated Filter Done!"
    source_file.close()
    out_file.close()
