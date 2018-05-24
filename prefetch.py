#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Script to prefetch data to a local directory for testing purposes.
"""
import os
from urllib.request import urlopen
import pandas as pd
import traceback

data_directory = "local"
wfpfood_url    = "http://dataviz.vam.wfp.org/api/getfoodprices?ac="
countries_url  = "http://vam.wfp.org/sites/data/api/adm0code.csv"


def fetch(url,filename):
    with urlopen(url) as f:
        print ("Process %(url)s"%locals())
        content = f.read()
        print ("  Read  %(url)s - OK"%locals())

        path = os.path.join(data_directory,filename)
        print ("  Write %(path)s"%locals())
        with open(path,"wb") as g:
            g.write(content)
            print("  OK")
    return path

if __name__ == "__main__":
    try:
        os.makedirs(data_directory)
    except:
        print("Data directory %(data_directory)s exists" % locals())

    countries = pd.read_csv(fetch(countries_url,"adm0code.csv"))

    for index, row in countries.iterrows():
        country=row.ADM0_NAME
        code=str(row.ADM0_CODE)
        print("Country %(country)-40s  %(code)3s"%locals())
        try:
            fetch(wfpfood_url+code,code)
        except:
            traceback.print_exc()