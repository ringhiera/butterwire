# -*- coding: utf-8 -*-

from context import butterwire
import os
import unittest
import mock
import pandas
import json

import stagingDao
import exporter


def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError as e:
    return False
  return True

# 
class ExporterTestSuite(unittest.TestCase):
  """Basic test cases."""
  def test_export_data(self):
    resources_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'resources'))
    testdata = pandas.read_csv(os.path.join(resources_path,'exporter_testdata.csv'), parse_dates=True, index_col=0)
    mock_dao = mock.MagicMock(spec=stagingDao.StagingDaoMockMongo)
    e = exporter.ExporterJson(mock_dao)
    actual = e.export_data('test_data', testdata)
    print (actual)
    assert 3 == len(actual)
    for row in actual:
      assert is_json(row)
      # one could further improve the power of the test verifying record content.
    assert mock_dao.persist_json_list.called 
    assert mock_dao.persist_json_list.call_count == 1


if __name__ == '__main__':
  unittest.main()
