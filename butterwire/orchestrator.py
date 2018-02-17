# -*- coding: utf-8 -*-

import ingestor
import logging
import requests
import quandl

from ingestor  import IngestorItf 
from processor import ProcessorItf 
from exporter  import ExporterItf 



class OrchestratorIpe( object ):
  """Implementation of an orchestrator for a standardized Ingest Procell Export process,
    The class is a demo with bare minimum fringes."""

  def __init__(self, ingestor, processor, exporter):
    self.logger = logging.getLogger(__name__)
    if not isinstance(ingestor, IngestorItf):
      raise ValueError('first argument must be a IngestorItf')
    self.ingestor = ingestor
    if not isinstance(processor, ProcessorItf):
      raise ValueError('second argument must be a ProcessorItf')
    self.processor = processor
    if not isinstance(exporter, ExporterItf):
      raise ValueError('third argument must be a ExporterItf')
    self.exporter = exporter


  def doIpe(self, ts_name):
    # Ingest data
    try: # recoverable failures are handled by the connector using exponential backoff
      data = self.ingestor.get_data(ts_name);
    except (requests.exceptions.Timeout,
      requests.exceptions.ConnectionError) as e:
      self.logger.info('Exceded number of attempts on recoverable failure', exc_info=True)
      raise
    except (quandl.errors.quandl_error.AuthenticationError,
      requests.exceptions.TooManyRedirects) as e:
      self.logger.info('Unrecoverable failure', exc_info=True)
      raise

    # process data
    data = self.processor.process(data,0)

    # Export data
    jsonData = self.exporter.export_data(ts_name, data)

    return jsonData;

