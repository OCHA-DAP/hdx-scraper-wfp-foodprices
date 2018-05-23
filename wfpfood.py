#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
WFP food prices:
------------

Creates a proxy to the WFP food prices dataset. 

"""

import logging
from urllib.parse import quote_plus

from hdx.data.dataset import Dataset
from hdx.data.resource_view import ResourceView
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
#from hdx.utilities.location import Location
from slugify import slugify

logger = logging.getLogger(__name__)

hxlate = '&name=ACLEDHXL&tagger-match-all=on&tagger-02-header=iso&tagger-02-tag=%23country%2Bcode&tagger-03-header=event_id_cnty&tagger-03-tag=%23event%2Bcode&tagger-05-header=event_date&tagger-05-tag=%23date%2Boccurred+&tagger-08-header=event_type&tagger-08-tag=%23event%2Btype&tagger-09-header=actor1&tagger-09-tag=%23group%2Bname%2Bfirst&tagger-10-header=assoc_actor_1&tagger-10-tag=%23group%2Bname%2Bfirst%2Bassoc&tagger-12-header=actor2&tagger-12-tag=%23group%2Bname%2Bsecond&tagger-13-header=assoc_actor_2&tagger-13-tag=%23group%2Bname%2Bsecond%2Bassoc&tagger-16-header=region&tagger-16-tag=%23region%2Bname&tagger-17-header=country&tagger-17-tag=%23country%2Bname&tagger-18-header=admin1&tagger-18-tag=%23adm1%2Bname&tagger-19-header=admin2&tagger-19-tag=%23adm2%2Bname&tagger-20-header=admin3&tagger-20-tag=%23adm3%2Bname&tagger-21-header=location&tagger-21-tag=%23loc%2Bname&tagger-22-header=latitude&tagger-22-tag=%23geo%2Blat&tagger-23-header=longitude&tagger-23-tag=%23geo%2Blon&tagger-25-header=source&tagger-25-tag=%23meta%2Bsource&tagger-27-header=notes&tagger-27-tag=%23description&tagger-28-header=fatalities&tagger-28-tag=%23affected%2Bkilled&header-row=1'
urlexample = "https://proxy.hxlstandard.org/data.csv?tagger-match-all=on&tagger-01-header=currency&tagger-01-tag=%23x_currency&tagger-02-header=startdate&tagger-02-tag=%23date+%2Bstart&tagger-03-header=enddate&tagger-03-tag=%23date+%2Bend&url=http%3A%2F%2Fdataviz.vam.wfp.org%2Fapi%2Fgetfoodprices%3Fac%3D1&header-row=1"
hxlate = "&tagger-match-all=on&tagger-01-header=currency&tagger-01-tag=%23x_currency&tagger-02-header=startdate&tagger-02-tag=%23date+%2Bstart&tagger-03-header=enddate&tagger-03-tag=%23date+%2Bend&header-row=1"

def get_countriesdata(countries_url, downloader):
    countries=[]
    unknown=[]
    for row in downloader.get_tabular_rows(countries_url, dict_rows=False, headers=1, format='csv'):
        name = row[0]
        code = row[1]
        iso3, fuzzy = Country.get_iso3_country_code_fuzzy(name)
        if iso3 is None:
            unknown.append(name)
        else:
            countries.append(dict(name=name, code=code, iso3=iso3))
    if len(unknown):
        logger.warning("Some countries were not recognized and are ignored:\n"+",\n".join(unknown))


    return countries

def generate_dataset_and_showcase(wfpfood_url, hxlproxy_url, downloader, countrydata):
    """Parse json of the form:
    {
    },
    """
    title = '%s - Food Prices' % countrydata['name']
    logger.info('Creating dataset: %s' % title)
    name = 'WFP food prices for %s' % countrydata['name']  #  Example name which should be unique so can include organisation name and country
    slugified_name = slugify(name).lower()

    wfpfood_country_url = wfpfood_url + countrydata['code']
    url = '%surl=%s%s' % (hxlproxy_url, quote_plus(wfpfood_country_url), hxlate)
    print (url)
    exit(0)

    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer()
    dataset.set_organization()
    dataset.set_dataset_date()
    dataset.set_expected_update_frequency()
    dataset.add_country_location()
    dataset.add_tags([])

    resource = {
        'name': title,
        'url': None,
        'description': None
    }
    resource.set_file_type('csv')  # set the file type to eg. csv
    dataset.add_update_resource(resource)

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': None,
        'notes': None,
        'url': None,
        'image_url': None
    })
    showcase.add_tags([])
    return dataset, showcase
