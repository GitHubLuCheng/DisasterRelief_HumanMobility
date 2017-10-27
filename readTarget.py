#coding=utf-8
import json
import re

#def praseFile(filename):
 #   with open(filename,'r') as f:
  #      for line in f.readlines():
  #          line = line.strip()
            #line = re.sub(r'\'', '\\\"', line)
  #          if line != "":
   #             res = json.loads(line)
   #             print(res['text'])

def praseFile(filename):

        with open(filename,'r') as f:
            for line in f.readlines():
                line = line.strip()
                line = re.sub(r'\'', '\\\"', line)
                if line != "":
                    res = json.loads(line)
                    # print res['user']['lang']
                    if res['user']['lang'] != "en":
                        print(res['text'])


if __name__ == "__main__":
    fileName = "lou.json"
    praseFile(fileName)