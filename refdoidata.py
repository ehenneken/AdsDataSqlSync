from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.schema import CreateSchema, DropSchema

from sqlalchemy import *
from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import sessionmaker
from collections import defaultdict
from datetime import datetime
import time
import sys
import json
import argparse
import logging

import row_view
import utils


#from settings import(ROW_VIEW_DATABASE, METRICS_DATABASE, METRICS_DATABASE2)

Base = declarative_base()
meta = MetaData()

# part of the Metrics system

class RefDoiData():

    def __init__(self, refdoidata_schema='refdoidata', passed_config=None):
        self.config = {}
        self.config.update(utils.load_config())
        if passed_config:
            self.config.update(passed_config)

        self.meta = MetaData()
        self.schema = refdoidata_schema
        self.table = self.get_refdoidata_table()
        self.database = self.config.get('INGEST_DATABASE', 'postgresql://postgres@localhost:5432/postgres')
        self.engine = create_engine(self.database)
        self.connection = self.engine.connect()
        # if true, don't bother checking if metrics database has bibcode, assume db insert
        self.from_scratch = self.config.get('FROM_SCRATCH', True)
        # if true, send data to stdout
        self.copy_from_program = self.config.get('COPY_FROM_PROGRAM', True)
        self.upserts = []

        # sql command to update a row in the refdoidata table
        self.u = self.table.update().where(self.table.c.doi == bindparam('tmp_doi')). \
            values ({'bibcode': bindparam('bibcode'),
                     'citing_bibcodes': bindparam('citing_bibcodes'),
                     'data_sources': bindparam('data_sources'),
                     'resolver_scores': bindparam('resolver_scores')})

        self.tmp_count = 0
        self.tmp_update_buffer = []
        self.logger = logging.getLogger('AdsDataSqlSync')

    def get_refdoidata_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('refdoidata', meta,
                     Column('id', Integer, primary_key=True),
                     Column('doi', String, nullable=False, index=True, unique=True),
                     Column('bibcode', String),
                     Column('citing_bibcodes', postgresql.ARRAY(String)),
                     Column('data_sources', postgresql.ARRAY(String)),
                     Column('resolver_scores', postgresql.ARRAY(String)),
                     Column('modtime',DateTime),
                     schema=self.schema)

    def create_refdoidata_table(self):
        self.engine.execute(CreateSchema(self.schema))
        temp_meta = MetaData()
        table = self.get_refdoidata_table(temp_meta)
        temp_meta.create_all(self.engine)
        self.logger.info('refdoidata.py, refdoidata table created')

    def rename_schema(self, new_name):
        self.sql_sync_engine.execute("alter schema {} rename to {}".format(self.schema_name, new_name))
        self.logger.info('refdoidata, renamed schema {} to {} '.format(self.schema_name, new_name))


    def drop_refdoidata_table(self):
        self.engine.execute("drop schema if exists {} cascade".format(self.schema))
        self.logger.info('refdoidata.py, refdoidata table dropped')

    def save(self, values_dict):
        """buffered save does actual save every 100 records, call flush at end of processing"""

        doi = values_dict['doi']
        if doi is None:
            print 'error: cannont save refdoidata data that does not have a doi'
            return

        if self.copy_from_program:
            print self.to_sql(values_dict)
            return

        self.upserts.append(values_dict)

        self.tmp_count += 1
        if (self.tmp_count % 100) != 0:
            self.flush()
            self.tmp_count = 0

    def flush(self):
        """bulk write records to sql database"""
        if self.copy_from_program:
            return

        updates = []
        inserts = []
        if self.from_scratch:
            # here if we just assume these records are not in database and insert
            # it may have just been created so we just insert, not update
            inserts = self.upserts
        else:
            # here if we have to check each record to see if we update or insert
            for d in self.upserts:
                current_doi = d['doi']
                s = select([self.table]).where(self.table.c.doi== current_doi)
                r = self.connection.execute(s)
                first = r.first()
                if first:
                    d['tmp_doi'] = d['doi']
                    d.pop('id', None)
                    updates.append(d)
                else:
                    d.pop('id', None)
                    inserts.append(d)

        if len(updates):
            result = self.connection.execute(self.u, updates)
        if len(inserts):
            result = self.connection.execute(self.table.insert(), inserts)

        self.upserts = []

    def read(self, doi):
        """read the passed doi from the postgres database"""
        s = select([self.table]).where(self.table.c.doi == doi)
        r = self.connection.execute(s)
        first = r.first()
        return first

    def update_refdoidata_changed(self, row_view_schema='ingest'):  #, delta_schema='delta'):
        """changed dois are in sql table, for each we update refdoidata record"""
        self.copy_from_program = False  # maybe hack
        delta_sync = row_view.SqlSync(row_view_schema)
        delta_table = delta_sync.get_delta_table()
        sql_sync = row_view.SqlSync(row_view_schema)
        row_view_table = sql_sync.get_row_view_table()
        connection = sql_sync.sql_sync_engine.connect()
        s = select([delta_table])
        results = connection.execute(s)
        count = 0
        for delta_row in results:
            row = sql_sync.get_row_view(delta_row['doi'])
            metrics_dict = self.row_view_to_refdoidata(row, sql_sync)
            self.save(refdoidata_dict)
            if (count % 10000) == 0:
                self.logger.debug('delta count = {}, doi = {}'.format(count, delta_row['doi']))
            count += 1
        self.flush()
        # need to close?

    def update_refdoidata_test(self, doi, row_view_schema='ingest'):
        sql_sync = row_view.SqlSync(row_view_schema)
        row_view_doi = sql_sync.get_row_view(doi)
        refdoidata_dict = self.row_view_to_refdoidata(row_view_doi, sql_sync)
        self.save(refdoidata_dict)
        self.flush()

    def to_sql(self, refdoidata_dict):
        """return string representation of refdoidata data suitable for postgres copy from program"""
        return_str = str(refdoidata_dict['id']) + '\t' + refdoidata_dict['doi']
        return_str += '\t' + str(refdoidata_dict['bibcode'])
        return_str += '\t' + '{' + json.dumps(refdoidata_dict['citing_bibcodes']).strip('[]') + '}'
        return_str += '\t' + '{' + json.dumps(refdoidata_dict['data_sources']).strip('[]') + '}'
        return_str += '\t' + '{' + json.dumps(refdoidata_dict['resolver_scores']).strip('[]') + '}'
        return_str += '\t' + str(datetime.now())
        return return_str

    def update_refdoidata_all(self, row_view_schema='ingest', start_offset=1, end_offset=-1):
        """update all elements in the refdoidata database between the passed id offsets"""
        # we request one block of rows from the database at a time
        start_time = time.time()
        step_size = 1000
        count = 0
        offset = start_offset
        max_rows = self.config['MAX_ROWS']
        sql_sync = row_view.SqlSync(row_view_schema)
        table = sql_sync.get_row_view_table()
        while True:
            s = select([table])
            s = s.where(sql_sync.row_view_table.c.id >= offset).where(sql_sync.row_view_table.c.id < offset + step_size)
            connection = sql_sync.sql_sync_connection;
            results = connection.execute(s)
            for row_view_current in results:
                refdoidata_dict = self.row_view_to_refdoidata(row_view_current, sql_sync)
                self.save(refdoidata_dict)
                count += 1
                if max_rows > 0 and count > max_rows:
                    break
                if count % 100000 == 0:
                    self.logger.debug('refdoidata.py, refdoidata count = {}'.format(count))
                if end_offset > 0 and end_offset <= (count + start_offset):
                    # here if we processed the last requested row
                    self.flush()
                    end_time = time.time()
                    return

            if results.rowcount < step_size:
                # here if last read got the last block of data and we're done processing it
                self.flush()
                end_time = time.time()
                return;

            offset += step_size
