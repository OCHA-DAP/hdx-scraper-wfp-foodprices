#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
WFP food prices:
------------

Creates datasets with flattened tables of WFP food prices.

"""

import logging
from math import sin

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource, ResourceView
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from slugify import slugify
import pandas as pd
import numpy as np
import math
import datetime
import time

logger = logging.getLogger(__name__)
tags = ["food","health","monitoring","nutrition","wages"]


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
    df = pd.DataFrame(data=[hxl] + list(data),columns=columns)
    return df

_cache=None


def read_dataframe(wfpfood_url, downloader, countrydata):
    global _cache

    if _cache is not None:
        if countrydata["name"] in _cache:
            df = _cache[countrydata["name"]]
        else:
            df = flattened_data_to_dataframe(
                read_flattened_data(wfpfood_url, downloader, countrydata)
            )
            _cache[countrydata["name"]] = df
        return df.copy()


    return flattened_data_to_dataframe(
      read_flattened_data(wfpfood_url, downloader, countrydata)
    )


def year_from_date(d):
    try:
        return datetime.datetime.strptime(d, "%Y-%m-%d").year
    except:
        return 0


def month_from_date(d):
    try:
        return datetime.datetime.strptime(d, "%Y-%m-%d").month
    except:
        return 0


def quickchart_dataframe(df, shortcuts, keep_last_years = 5, remove_nonfood=True):
    """This function creates filtered dataframe with scaled median prices and short names suitable for quickchart.
    """
    def sinceEpoch(d):
        try:
            return time.mktime(datetime.datetime.strptime(d, "%Y-%m-%d").timetuple())
        except:
            return 0
    df=df.assign(year = df.date.apply(year_from_date))
    hxl = df.loc[:0]
    df=df.loc[1:]
    df1=hxl.copy()
    df1=df1.assign(label="#item+label")
    df1=df1.assign(cmnameshort = "#item+name+short")

    df.loc[:,"price"] = pd.to_numeric(df.price, errors='coerce')
    df.loc[:,"cmname"] = df.cmname.apply(str)
    df.loc[:,"unit"] = df.unit.apply(str)
    from_year = df["year"].max() - keep_last_years
    df=df.loc[df["year"] >= from_year]  # keep only last keep_last_years years
    df=df.assign(x = df.date.apply(sinceEpoch))

    dates = sorted(df.date.unique())
    x = np.array([sinceEpoch(d) for d in dates])

    if remove_nonfood:
        df.loc[:, "catid"] = pd.to_numeric(df.catid, errors='coerce')
        df=df.loc[df.catid != 8]

    processed_data=[]
    for key, index in sorted(df.groupby(["cmname","unit","category","cmid","catid"]).groups.items()):
        commodity, unit, category, cmid, catid = key
        g=df.loc[index]
        gd = g.groupby(["date"])

        invmean = 100.0 / g.price.mean()
        quantity = math.pow(10, math.trunc(math.log10(invmean)))
        qunit = "%d %s"%(quantity,unit)
        if quantity < 1:
            qunit = "1/%d %s"%(int(1/quantity),unit)
        if quantity == 1:
            qunit = unit

        label="%(commodity)s (%(qunit)s)"%locals()
        short_commodity = shortcuts.get(commodity,commodity)
        series = {}
        for date,median in gd.price.median().items():
            gg = g.loc[g.date==date]
            median = gg.loc[gg.price<=median].price.max()
            if median > 0:
                row = dict(gg.loc[gg.price==median].iloc[0])
                row["price"]*=quantity
                row["unit"]=qunit
                row["label"]=label
                row["cmnameshort"] = short_commodity
                row["scaling"]=quantity
                row["interpolated"]=0
                series[date]=row
        source_dates = sorted(series.keys())
        xp = np.array([series[d]["x"] for d in source_dates])
        yp = np.array([series[d]["price"] for d in source_dates])
        y = np.interp(x,xp,yp)
        for date,price in zip(dates,y):
            if date in series:
                processed_data.append(series[date])
            else:
                processed_data.append(dict(
                    date         = date,
                    price        = price,
                    unit         = qunit,
                    label        = label,
                    cmname       = commodity,
                    cmnameshort  = short_commodity,
                    scaling      = quantity,
                    category     = category,
                    interpolated = 1,
                    cmid         = cmid,
                    catid        = catid
                ))
    df1=df1.append(pd.DataFrame(processed_data), ignore_index=True)
    return df1


def generate_dataset_and_showcase(wfpfood_url, downloader, countrydata, shortcuts):
    """Generate datasets and showcases for each country.
    """
    title = '%s - Food Prices' % countrydata['name']
    logger.info('Creating dataset: %s' % title)
    name = 'WFP food prices for %s' % countrydata['name']  #  Example name which should be unique so can include organisation name and country
    slugified_name = slugify(name).lower()

    df = read_dataframe(wfpfood_url, downloader, countrydata)

    if len(df)<=1:
        logger.warning('Dataset "%s" is empty' % title)
        return None, None


    dataset = Dataset({
        'name': slugified_name,
        'title': title,
        "dataset_preview": "resource_id"
    })
    dataset.set_maintainer("9957c0e9-cd38-40f1-900b-22c91276154b") # Orest Dubay
#    dataset.set_maintainer("154de241-38d6-47d3-a77f-0a9848a61df3")
    dataset.set_organization("3ecac442-7fed-448d-8f78-b385ef6f84e7")

    dataset.set_dataset_date(df.loc[1:].date.min(),df.loc[1:].date.max(),"%Y-%m-%d")
    dataset.set_expected_update_frequency("weekly")
    dataset.add_country_location(countrydata["name"])
    dataset.add_tags(tags)


    file_csv = "WFP_food_prices_%s.csv"%countrydata["name"].replace(" ","-")
    df.to_csv(file_csv,index=False)
    resource = Resource({
        'name': title,
        "dataset_preview_enabled": "False",
        'description': "Food prices data with HXL tags"
    })
    resource.set_file_type('csv')  # set the file type to eg. csv
    resource.set_file_to_upload(file_csv)
    dataset.add_update_resource(resource)

    df1 = quickchart_dataframe(df, shortcuts)
    file_csv = "WFP_food_median_prices_%s.csv"%countrydata["name"].replace(" ","-")
    df1.to_csv(file_csv,index=False)
    resource = Resource({
        'name': '%s - Food Median Prices' % countrydata['name'],
        "dataset_preview_enabled": "True",
        'description':
"""Food median prices data with HXL tags.
Median of all prices for a given commodity observed on different markets is shown, together with the market where
it was observed. Data are shortened in multiple ways:

- Rather that prices on all markets, only median price across all markets is shown, together with the market
  where it has been observed.
- Only food commodities are displayed (non-food commodities like fuel and wages are not shown).
- Only data after %s are shown. Missing data are interpolated.
- Column with shorter commodity names "cmnshort" are available to be used as chart labels.
- Units are adapted and prices are rescaled in order to yield comparable values (so that they
  can be displayed and compared in a single chart). Scaling factor is present in scaling column.
  Label with full commodity name and a unit (with scale if applicable) is in column "label".  

This reduces the amount of data and allows to make cleaner charts.
"""%(df1.loc[1:].date.min())
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
    showcase.add_tags(tags)
    return dataset, showcase


def generate_resource_view(dataset):
    resource_view = ResourceView({'resource_id': dataset.get_resource(1)['id']})
    resource_view.update_from_yaml()
    return resource_view


def joint_dataframe(wfpfood_url, downloader, countriesdata):
    def ptid_to_ptname(ptid):
        return {15:"Retail", 14:"Wholesale", 17:"Producer", 18:"Farm Gate"}.get(ptid,"")

    df = None
    for countrydata in countriesdata:
        logging.info("Loading %s into a joint dataset"%(countrydata["name"]))
        df_country = read_dataframe(wfpfood_url, downloader, countrydata)

        df_country = df_country.loc[1:]

        dff = pd.DataFrame(dict(
            adm0_id   = [int(countrydata["code"])]*len(df_country),
            adm0_name = [str(countrydata["name"])]*len(df_country),
            adm1_id   = df_country.adm1id,
            adm1_name = df_country.admname,
            mkt_id    = df_country.mktid,
            mkt_name  = df_country.mktname,
            cm_id     = df_country.cmid,
            cm_name   = df_country.cmname,
            cur_id    = [0]*len(df_country),
            cur_name  = df_country.currency,
            pt_id     = df_country.ptid,
            pt_name   = df_country.ptid.apply(ptid_to_ptname),
            um_id     = df_country.umid,
            um_name   = df_country.unit,
            mp_month  = df_country.date.apply(month_from_date),
            mp_year   = df_country.date.apply(year_from_date),
            mp_price  = df_country.price,
            mp_commoditysource = [""]*len(df_country),
        ), columns ="""adm0_id
            adm0_name
            adm1_id
            adm1_name
            mkt_id
            mkt_name
            cm_id
            cm_name
            cur_id
            cur_name
            pt_id
            pt_name
            um_id
            um_name
            mp_month
            mp_year
            mp_price
            mp_commoditysource""".split()
        )
        df = dff if df is None else df.append(dff,ignore_index=True)
    return df


def generate_joint_dataset_and_showcase(wfpfood_url, downloader, countriesdata):
    """Generate single joint datasets and showcases containing data for all countries.
    """
    title = 'Global Food Prices Database (WFP)'
    logger.info('Creating joint dataset: %s' % title)
    name = title
    slugified_name = slugify(name).lower()

    df = joint_dataframe(wfpfood_url, downloader, countriesdata)

    if len(df)<=1:
        logger.warning('Dataset "%s" is empty' % title)
        return None, None

    dataset = Dataset({
        'name': slugified_name,
        'title': title
    })
    dataset.set_maintainer("9957c0e9-cd38-40f1-900b-22c91276154b") # Orest Dubay
#    dataset.set_maintainer("154de241-38d6-47d3-a77f-0a9848a61df3")
    dataset.set_organization("3ecac442-7fed-448d-8f78-b385ef6f84e7")

    maxmonth = (100*df.mp_year+df.mp_month).max()%100
    dataset.set_dataset_date("%04d-01-01"%df.mp_year.min(),"%04d-%02d-15"%(df.mp_year.max(),maxmonth),"%Y-%m-%d")
    dataset.set_expected_update_frequency("weekly")
    dataset.add_country_locations(sorted(df.adm0_name.unique()))
    dataset.add_tags(tags)

    file_csv = "WFPVAM_FoodPrices.csv"
    df.to_csv(file_csv,index=False)
    resource = Resource({
        'name': title,
        'description': "Word Food Programme – Food Prices  Data Source: WFP Vulnerability Analysis and Mapping (VAM)."
    })
    resource.set_file_type('csv')  # set the file type to eg. csv
    resource.set_file_to_upload(file_csv)
    dataset.add_update_resource(resource)

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': title+" showcase",
        'notes': "Interactive data visualisation of WFP's Food Market Prices dataset",
        'url': "https://data.humdata.org/organization/wfp#interactive-data",
        'image_url': "https://docs.humdata.org/wp-content/uploads/wfp_food_prices_data_viz.gif"
    })
    showcase.add_tags(tags)

    dataset.update_from_yaml()
    dataset['notes'] = dataset['notes'] % 'Global Food Prices data'
    dataset.create_in_hdx()
    showcase.create_in_hdx()
    showcase.add_dataset(dataset)
    dataset.get_resource().create_datastore_from_yaml_schema(yaml_path="wfp_food_prices.yml", path=file_csv)
    logger.info('Finished joint dataset')

    return dataset, showcase
