# -*- coding: utf-8 -*-

import logging 
import sys
import requests
import backoff

import quandl

from ingestionDao import IngestionDaoItf 

class IngestorItf( object ):
    """Abstract base class for ingestors,
    the class provides a prototypical get data, and no fringes."""
    def get_data( self ):
        raise NotImplementedError("Class %s doesn't implement aMethod()" % (self.__class__.__name__)) 


class IngestorQuandl( IngestorItf ):
  """Implementation of an ingestor for quandl timeseries ,
  the class provides a prototypical get data, and no fringes."""
  
  def __init__( self, quandl_key, ingestionDao ):
    self.logger = logging.getLogger(__name__)
    quandl.ApiConfig.api_key = quandl_key
    if not isinstance(ingestionDao, IngestionDaoItf):
      raise ValueError('argument must be a IngestionDao')
    self.ingestionDao = ingestionDao


  @backoff.on_exception(backoff.expo,
                      (requests.exceptions.Timeout,
                       requests.exceptions.ConnectionError),
                      max_tries=3)
  def get_data( self, name ):
    self.logger.info("get_data(%s)", name)
    data = quandl.get(name)
    data = self.__validate(data)
    self.ingestionDao.persist_dataframe( name, data )
    return data

  def __validate( self, data ):
    # here we should validate data according with policies,
    # fix where it is possible to fix data,
    # or raise an exception if the data cannot be recovered.
    # The policies require to observe a few cases to figure out 
    # what we need to check for and how to fix/discard data
    # Optionally we can have the validator injected as we inject
    # the DAO
    return data


