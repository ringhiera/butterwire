# -*- coding: utf-8 -*-

import logging 
import numpy as np

class ProcessorItf( object ):
    """Abstract base class for processor,
    the class provides a prototypical process, and no fringes."""
    def process( self ):
        raise NotImplementedError("Class %s doesn't implement aMethod()" % (self.__class__.__name__)) 


class ProcessorBw( ProcessorItf ):
  """Implementation of a processor for quandl timeseries ,
  the class provides a prototypical process computing demean and logReturn and appending them to the dataframe."""
  
  def __init__(self):
    self.logger = logging.getLogger(__name__)

  def process( self, data, defaultValue ):
    self.logger.info("processing data")
    data = self.__deltaCashBuyer(data)
    data = self.__logReturn(data, defaultValue)
    return data

  ## Note: those methods are better defined as handlers and injected.
  # in e.g. a container of functions as it will allow for more general processing
  #
  def __deltaCashBuyer(self, data):
    mean = data["Cash Buyer"].mean()
    demean = data["Cash Buyer"] - mean
    data['CashBuyerDemean'] = demean
    return data

  def __logReturn(self, data, defaultValue):
    # log return = log[v_(i+1) / v_(i)] = log(v_(i+1))-log(v_(i))
    logRet = np.log(data["Cash Buyer"]).diff().fillna(defaultValue)	
    data['CashBuyerLogReturn'] = logRet
    return data

