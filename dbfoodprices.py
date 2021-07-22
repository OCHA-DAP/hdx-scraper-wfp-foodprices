# -*- coding: utf-8 -*-
from hdx.database import Base
from sqlalchemy import Column, DateTime, Boolean, String
from sqlalchemy import Integer

hxltags = {'date': '#date', 'adm1name': '#adm1+name', 'adm2name': '#adm2+name', 'latitude': '#geo+lat',
           'longitude': '#geo+lon', 'market': '#name+market', 'category': '#item+type', 'commodity': '#item+name',
           'unit': '#item+unit', 'currency': '#currency', 'pricetype': '#item+price+type', 'price': '#value'}


class DBDataset(Base):
    date = Column(DateTime, primary_key=True)
    adm1name = Column(String, primary_key=True)
    dataset_date = Column(String)
    update_frequency = Column(Integer)
    review_date = Column(DateTime)
    last_modified = Column(DateTime, nullable=False)
    updated_by_script = Column(DateTime)
    metadata_modified = Column(DateTime, nullable=False)  # this field and above are CKAN fields
    latest_of_modifieds = Column(DateTime, nullable=False)
    what_updated = Column(String, nullable=False)
    last_resource_updated = Column(String, nullable=False)  # an id
    last_resource_modified = Column(DateTime, nullable=False)  # a date
    fresh = Column(Integer)
    error = Column(Boolean, nullable=False)

    def __repr__(self):
        output = '<Dataset(run number=%d, id=%s, ' % (self.run_number, self.id)
        output += 'dataset date=%s, update frequency=%s,\n' % (str(self.dataset_date), self.update_frequency)
        output += 'review date=%s, last modified=%s, ' % (str(self.review_date), str(self.last_modified))
        output += 'metadata modified=%s, updated by script=%s,\n' % (
        str(self.metadata_modified), str(self.updated_by_script))
        output += 'latest of modifieds=%s, what updated=%s,\n' % (str(self.latest_of_modifieds), str(self.what_updated))
        output += 'Resource %s: last modified=%s,\n' % (str(self.last_resource_updated),
                                                        str(self.last_resource_modified))
        output += 'Dataset fresh=%s, error=%s)>' % (str(self.fresh), str(self.error))
        return output
