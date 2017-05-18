# -*- coding:utf-8 -*-

import sys
import uuid
import urllib,urllib2
import hmac
import base64
import time
import json

from hashlib import sha1
from django.shortcuts import render


accessKeyId = 'testid'
accessKeySecret='testsecret'
server_address = 'http://ecs.aliyuncs.com'

def index(request):
    # 拼接参数
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    params = {
        'Format': 'JSON',
        'Version': '2014-05-26',
        'AccessKeyId': accessKeyId,
        'SignatureVersion': '1.0',
        'SignatureMethod': 'HMAC-SHA1',
        'SignatureNonce': str(uuid.uuid1()),
        'TimeStamp': timestamp,
        'Action': 'DescribeRegions',
    }
    #参数排序
    sortedParams = sorted(params.items(), key=lambda params: params[0])
    #对参数进行编码并用=和&进行连接
    canonicalizedQueryString = ''
    for (k, v) in sortedParams:
        canonicalizedQueryString += '&' + percent_encode(k) + '=' + percent_encode(v)
    #构造待签名的字符串StringToSign
    stringToSign = 'GET&'+ percent_encode('/') + '&' + percent_encode(canonicalizedQueryString[1:])
    #计算哈希值
    h = hmac.new(accessKeySecret + "&", stringToSign, sha1)
    #base64编码
    signature = base64.encodestring(h.digest()).strip()
    #得到签名，封装url
    server_address = 'http://ecs.aliyuncs.com'
    params['Signature'] = signature
    url = server_address + "/?" + urllib.urlencode(params)

    # #根据url获取API信息
    # request = urllib2.Request(url)
    # try:
    #     conn = urllib2.urlopen(request)
    #     response = conn.read()
    # except urllib2.HTTPError, e:
    #     print(e.read().strip())
    #     raise SystemExit(e)
    # try:
    #     obj = json.loads(response)
    # except ValueError, e:
    #     raise SystemExit(e)

    return render(request, 'ecs/index.html',
                  {'url': url, 'signature': signature})


def percent_encode(str):
    res = urllib.quote(str.decode(sys.stdin.encoding).encode('utf8'), '')
    res = res.replace('+', '%20')
    res = res.replace('*', '%2A')
    res = res.replace('%7E', '~')
    return res
