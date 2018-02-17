# -*- coding: utf-8 -*-

import logging 

from pymongo import MongoClient

class StagingDaoItf( object ):
    """Abstract base class for staging DAO,
    the class provides a prototypical persist dataframe row by row"""
    def persist_json_list( self, name, dataJson ):
        raise NotImplementedError("Class %s doesn't implement aMethod()" % (self.__class__.__name__)) 

class StagingDaoMockMongo( StagingDaoItf ):
  """Implementation of a mock staging DAO for a list of json, simulating a connection to Mongo ,
  the class provides a prototypical persist dataframe.""" 

  def __init__(self, mongo_host, mongo_db):
    self.logger = logging.getLogger(__name__)
    self.client = MongoClient(mongo_host)
    self.db = self.client[mongo_db]

  def __del__(self):
    # we neeed to override destructor to close connection clean and tidy to cassandra 
    #self.cluster.shutdown()
    pass

  def persist_json_list( self, name, dataJson ):
    self.logger.info("persisting data to mongo")
    # the metod persists the list of json to mongo
    # potentially it could throw some exception 
    # e.g. if connectivity or authentication fail
    # the caller should deal with those errors in 
    # a similar way as using the quandl ingestor
    # potentially caller can decide to keep processing 
    # even if data cannot be persisted at this stage

    ## example below not tested as I did not have instance of mongo at hand
    # collection = db[self.__get_collection_by_name(name)]
    # result = collection.insert_many(dataJson)
    pass

  def __get_collection_by_name(name):
    # good enough for a mock, in reality one can use a smarter lookup policy :D
    return name


