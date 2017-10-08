import json
test_root = '/home/garden/Desktop/Data Storm/DataSource/test.json'
test_list = [{"name":"jane","age":12},{"name":"jack","age":13},{"name":"jack","age":14},{"name":"jack","age":15}]

list_length = len(test_list)
while list_length!=0:
    result = {}
    ages_list = []
    index = 0
    index2 = 0
    is_first_item = True
    record = test_list[index]
    while index2!=list_length:
        record2 = test_list[index2]
        if is_first_item:
            result["name"] = record["name"]
            ages_list.append(record["age"])
            result["ages"] = ages_list
            test_list.pop(0)
            list_length-=1
            is_first_item = False
            continue
        if record2["name"] ==result["name"]:
           result["ages"].append(record2["age"])
           test_list.pop(index2)
           list_length -= 1
           continue
        index2 += 1
    print result
# with open(test_root,'w') as test_file:
#     dict_test = {"1":2,"2":3112441231231231}
#     test_file.write(json.dumps(dict_test))
#     test_file.close()
