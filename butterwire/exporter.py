# -*- coding: utf-8 -*-

import logging 
import json

from stagingDao import StagingDaoItf 

class ExporterItf( object ):
    """Abstract base class for ingestors,
    the class provides a prototypical get data, and no fringes."""
    def export_data( self, data ):
        raise NotImplementedError("Class %s doesn't implement aMethod()" % (self.__class__.__name__)) 


class ExporterJson( ExporterItf ):
  """Implementation of an ingestor for quandl timeseries ,
  the class provides a prototypical get data, and no fringes."""
  
  def __init__(self, stagingDao):
    self.logger = logging.getLogger(__name__)
    if not isinstance(stagingDao, StagingDaoItf):
      raise ValueError('argument must be a StagingDaoItf')
    self.stagingDao = stagingDao

  def export_data( self, name, data ):
    self.logger.info("export_data...")
    dataJson = []
    d = data.to_dict(orient='index')
    for k, v in d.items():
      j = json.dumps({k.isoformat():{x:y for x,y in v.items()}})
      dataJson.append(j)
    self.stagingDao.persist_json_list(name, dataJson)
    return dataJson
