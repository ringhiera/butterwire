# -*- coding: utf-8 -*-

from context import butterwire
import os
import unittest
import mock
import pandas

import ingestor
import processor
import exporter
import orchestrator

class OrchestratorTestSuite(unittest.TestCase):
  """Basic test cases."""
  # verify the orchestrator calls each of the 3 handlers
  def test_doIpe(self):
    mock_ingestor = mock.MagicMock(spec=ingestor.IngestorItf) 
    mock_processor = mock.MagicMock(spec=processor.ProcessorItf)
    mock_exporter = mock.MagicMock(spec=exporter.ExporterItf)
    o = orchestrator.OrchestratorIpe(mock_ingestor, mock_processor, mock_exporter)
    o.doIpe('test_data')

    assert mock_ingestor.get_data.called
    assert mock_ingestor.get_data.call_count == 1
    assert mock_processor.process.called
    assert mock_processor.process.call_count == 1
    assert mock_exporter.export_data.called
    assert mock_exporter.export_data.call_count == 1

# test the orchestrator handles a failure on the ingestor - omitted

if __name__ == '__main__':
  unittest.main()
