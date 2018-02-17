# -*- coding: utf-8 -*-

import os
import configparser
import logging 
import logging.config
import sys
import requests
import backoff

# 3rd party providers
import quandl

# local dependencies
import ingestionDao
import ingestor
import processor
import stagingDao
import exporter
import orchestrator


if __name__ == '__main__':
  # load app configuration
  config = configparser.ConfigParser()
  config.read(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config.ini')))
  env = os.environ.get('ENV', 'DEFAULT')

  # load logger configuration
  logging.config.fileConfig(os.path.abspath(os.path.join(os.path.dirname(__file__), 'logging_config.ini')))
  logger = logging.getLogger(__name__)

  logger.info ("init ingestor...")

  cassandra_contact_points=config[env]['CASSANDRA_CONTACT_POINTS'] 
  cassandra_username=config[env]['CASSANDRA_USER'] 
  cassandra_password=config[env]['CASSANDRA_PASSWORD']
  quandl_key = config[env]['QUANDL_KEY']
  ingestionDao = ingestionDao.IngestionDaoMockCassandra(cassandra_contact_points, cassandra_username, cassandra_password)
  ingestor = ingestor.IngestorQuandl(quandl_key, ingestionDao);

  logger.info ("init processor...")
  processor = processor.ProcessorBw()

  logger.info ("init exporter...")
  mongo_host=config[env]['MONGO_HOST']
  mongo_db=config[env]['MONGO_DB']
  print ('mongo_host %s',mongo_host)
  stagingDao = stagingDao.StagingDaoMockMongo(mongo_host, mongo_db)
  exporter = exporter.ExporterJson(stagingDao)

  logger.info ("init orchestrator...")
  orchestrator = orchestrator.OrchestratorIpe(ingestor,processor,exporter)
 
  logger.info ("orchestrator.doIpe...")
  # use a scheduler to run periodically?
  data = orchestrator.doIpe(config[env]['TS_NAME'])

  # print (data)
  logger.info ("done...")


