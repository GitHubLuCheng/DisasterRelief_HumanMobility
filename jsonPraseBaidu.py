#coding=utf-8

import re
from http import client
import hashlib
from urllib import parse
import random
import json
appKey = "20171020000089693"
secretKey = "QUO8kP4kDnvf8mvkzj81"
httpClient = client.HTTPConnection('api.fanyi.baidu.com')
# 这里的q 就是你要查询的内容
# q = 'That\' my best friends'
# 输入是英文
fromLang = 'auto'
# 翻译到中文
toLang = 'en'
salt = random.randint(1, 65536)

def praseFile(readfile,writefile):
    with open(writefile,'w')as f2:
        with open(readfile,'r') as f:
            for line in f.readlines():
                line = line.strip()
                line = re.sub(r'\'', '\\\"', line)
                if line != "":
                    res = json.loads(line)
                    # print res['user']['lang']
                    if res['user']['lang'] != "en":
                        temp_index = res['text'].find("https:")
                        temp_text = res['text'][0:temp_index]
                        res['text'] = youdaoTranslaor(temp_text)
                    f2.writelines(json.dumps(res))
                    f2.writelines('\n')
            f2.close()

def youdaoTranslaor(text):
    sign = appKey + text + str(salt) + secretKey
    m1 = hashlib.md5()
    m1.update(sign.encode('UTF-8'))
    sign = m1.hexdigest()
    myurl = '/api/trans/vip/translate' + '?appid='+appKey+'&q='+parse.quote(text)+'&from='+fromLang+'&to='+toLang+'&salt='+str(salt)+'&sign='+sign
    try:
        httpClient.request('GET', myurl)
        # response是HTTPResponse对象
        response = httpClient.getresponse()
        res = response.read()
        res = json.loads(res)
        print (res['trans_result'][0]['dst'])
        return res['trans_result'][0]['dst']


    except Exception as e:
        print(e)


if __name__ == "__main__":
    fileName = "lou.json"
    writeName = "louTrans.json"
    praseFile(fileName,writeName)