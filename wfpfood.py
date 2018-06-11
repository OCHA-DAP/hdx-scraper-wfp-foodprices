#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
WFP food prices:
------------

Creates datasets with flattened tables of WFP food prices.

"""

import logging

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from slugify import slugify
import pandas as pd

logger = logging.getLogger(__name__)


def get_countriesdata(countries_url, downloader, country_correspondence):
    """Download a list of countries and provide mapping if necessary.

    A list of dictionaries is returned, each containing the following keys:
    iso3 - ISO 3 country code
    name - country name
    code - WFP country code
    wfp_countries - a list of dictionaries describing WFP countries that are part of the "ISO 3" country.

    Note: The data source (WFP countries) may contain countries that do not have its own ISO 3 code.
    Such WFP countries can be mapped to ISO 3 country using the
    country_correspondence attribute in the project_configuration.yml. All WFP countries mapped to the same ISO 3 country
    will be listed in wfp_countries. Each ISO 3 country will appear at most once in the output.
    """
    countries={}
    unknown=[]


    for row in downloader.get_tabular_rows(countries_url, dict_rows=False, headers=1, format='csv'):
        name = row[0]
        sub_name = name
        code = row[1]
        new_wfp_countries=[dict(name=sub_name,code=code)]
        iso3, fuzzy = Country.get_iso3_country_code_fuzzy(name)
        if iso3 is None:
            name = country_correspondence.get(sub_name)
            if name is None:
                unknown.append(sub_name)
                continue
            else:
                iso3, fuzzy = Country.get_iso3_country_code_fuzzy(name)

        countries[iso3] = countries.get(iso3,dict(name=name,iso3=iso3,wfp_countries=[]))
        countries[iso3]["wfp_countries"] = countries[iso3]["wfp_countries"] + new_wfp_countries
        countries[iso3]["code"] = ([x for x in countries[iso3]["wfp_countries"] if x["name"] == name] + countries[iso3]["wfp_countries"])[0]["code"]

    if len(unknown):
        logger.warning("Some countries were not recognized and are ignored:\n"+",\n".join(unknown))

    return [countries[iso3] for name, iso3 in sorted([(x["name"],x["iso3"]) for x in countries.values()])]

def months_between(fromdate,todate):
    """Returns an iterator of iso-formatted dates between fromdate and todate (inclusive) with the step of 1 month."""
    import datetime
    def to_date(d):
        if isinstance(d, datetime.date):
            return d
        if isinstance(d, str):
            d=datetime.datetime.strptime(d, "%Y/%m/%d")
        if isinstance(d, datetime.datetime):
            return datetime.date(year = d.year, month = d.month, day=d.day)
        logging.error("Unexpected date type: "+repr(d))

    fromdate = to_date(fromdate)
    todate = to_date(todate)
    if fromdate is not None and todate is not None:
        d=fromdate
        while d<=todate:
            yield d.isoformat()
            year = d.year
            month=d.month+1
            if month>12:
                year+=1
                month=1
            d=datetime.date(year=year,month=month,day=d.day)

def read_flattened_data(wfpfood_url, downloader, countrydata):
    """Reads the WFP food prices data from the source and flattens them to a plain table structure.

    WFP data structure contains monthly prices for a continuous time period, which need to be flattened in order
    to fit into a plain table structure. This function creates an iterator which both reads and flattens the data in one go.
    """
    for wfp_countrydata in countrydata["wfp_countries"]:
        logging.debug("Start reading %s data"%countrydata["name"])
        url = wfpfood_url + wfp_countrydata['code']
        for row in downloader.get_tabular_rows(url,file_type='json',dict_rows=True,headers=1):
            dates =list(months_between(row["startdate"],row["enddate"]))
            if len(dates)!=len(row["mp_price"]):
                logging.warning("Number of prices %d does not match with number of expected dates (%d) between %s and %s"%(
                    len(dates),len(row["mp_price"]),row["startdate"],row["enddate"]
                ))
            for date, price in zip(dates,row["mp_price"]):
                if price is not None:
                    yield dict(
                        {key:value for key, value in row.items() if key not in ("startdate","enddate","mp_price")},
                        date = date,
                        price = float(price),
                        country=wfp_countrydata['name']
                    )
        logging.debug("Finished reading %s data"%countrydata["name"])

def flattened_data_to_dataframe(data):
    """Converts data to a Pandas DataFrame format and adds the HXL taggs.
    """
    column_definition="""date #date
  cmname    #item+name
  unit      #item+unit
  category  #item+type
  price     #value
  currency  #currency
  country   #country+name
  admname   #adm1+name
  adm1id    #adm1+code
  mktname   #name+market
  mktid     
  cmid      #item+code
  ptid
  umid
  catid     #item+type+code
  sn        #meta+id
  default   """.split('\n')

    columns = [x.split()[0] for x in column_definition]
    hxl     = {x.split()[0]:" ".join(x.split()[1:]) for x in column_definition}
    return pd.DataFrame(data=[hxl] + list(data),columns=columns)

def generate_dataset_and_showcase(wfpfood_url, downloader, countrydata):
    """Generate datasets and showcases for each country.
    """
    title = '%s - Food Prices' % countrydata['name']
    logger.info('Creating dataset: %s' % title)
    name = 'WFP food prices for %s' % countrydata['name']  #  Example name which should be unique so can include organisation name and country
    slugified_name = slugify(name).lower()

    df = flattened_data_to_dataframe(
        read_flattened_data(wfpfood_url, downloader, countrydata)
    )

    if len(df)<=1:
        logger.warning('Dataset "%s" is empty' % title)
        return None, None

    file_csv = "WFP_food_prices_%s.csv"%countrydata["name"].replace(" ","-")
    df.to_csv(file_csv,index=False)

    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
#    dataset.set_maintainer("9957c0e9-cd38-40f1-900b-22c91276154b") # Orest Dubay
    dataset.set_maintainer("154de241-38d6-47d3-a77f-0a9848a61df3")
    dataset.set_organization("3ecac442-7fed-448d-8f78-b385ef6f84e7")

    dataset.set_dataset_date(df.ix[1:].date.min(),df.ix[1:].date.max(),"%Y-%m-%d")
    dataset.set_expected_update_frequency("weekly")
    dataset.add_country_location(countrydata["name"])
    dataset.add_tags(["food","food consumption","food and nutrition","food crisis","health","monitoring","nutrition","wages"])

    resource = Resource({
        'name': title,
        'description': "Food prices data with HXL tags"
    })
    resource.set_file_type('csv')  # set the file type to eg. csv
    resource.set_file_to_upload(file_csv)
    dataset.add_update_resource(resource)

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': title+" showcase",
        'notes': countrydata["name"] + " food prices data from World Food Programme displayed through VAM Economic Explorer",
        'url': "http://dataviz.vam.wfp.org/economic_explorer/prices?adm0="+countrydata["code"],
        'image_url': "http://dataviz.vam.wfp.org/_images/home/economic_2-4.jpg"
    })
    showcase.add_tags(["food","food and nutrition","monitoring","nutrition","wages"])
    return dataset, showcase
