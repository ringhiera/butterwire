# -*- coding: utf-8 -*-

import logging 

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd


class IngestionDaoItf( object ):
    """Abstract base class for ingestion DAO,
    the class provides a prototypical persist dataframe, and no fringes."""
    def persist_dataframe( self, name, data ):
        raise NotImplementedError("Class %s doesn't implement aMethod()" % (self.__class__.__name__)) 


class IngestionDaoMockCassandra( IngestionDaoItf ):
  """Implementation of a mock ingestion DAO for dataframes, simulating a connection to Cassandra ,
  the class provides a prototypical persist dataframe, and no fringes."""

  # It might be interesting to try out caspanda, it looks interesting from an abstraction perspective
  # https://github.com/aaronbenz/caspanda 

  def __init__(self, contact_points, username, password, fetch_size = 10000000):
    self.logger = logging.getLogger(__name__)
    #self.cluster = Cluster(
    #  contact_points=contact_points, 
    #  auth_provider = PlainTextAuthProvider(username=username, password=password)
    #)
    #self.fetch_size = fetch_size

  def __del__(self):
    # we neeed to override destructor to close connection clean and tidy to cassandra 
    #self.cluster.shutdown()
    pass

  def persist_dataframe( self, name, data ):
    self.logger.info("persisting data to cassandra")
    # the metod persists the dataframe to cassandra
    # potentially it could throw some exception 
    # e.g. if connectivity or authentication fail
    # the caller should deal with those errors in 
    # a similar way as using the quandl ingestor
    # potentially caller can decide to keep processing 
    # even if data cannot be persisted at this stage

    ## example below not tested as I did not have instance of cassandra at hand
    # column_names = list(data.columns.values)
    # cql_query = """
    #   INSERT INTO {table_name} ({col_names})
    #   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    #   """.format(table_name=self.__get_table_by_name(name), col_names=','.join(map(str, column_names)))

    # session = self.cluster.connect()
    # session.set_keyspace(self.__get_keyspace_by_name(name))
    # session.row_factory = pandas_factory
    # session.default_fetch_size = fetch_size
    # session.execute(cql_query, data)
    pass

  def __get_table_by_name(name):
    # good enough for a mock, in reality one can use a smarter lookup policy :D
    return name

  def __get_keyspace_by_name(name):
    # good enough for a mock, in reality one can use a smarter lookup policy :D
    return name

