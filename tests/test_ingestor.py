# -*- coding: utf-8 -*-

from context import butterwire
import os
import unittest
import pandas
import mock
import requests

import ingestor
import ingestionDao

# Tests are an example of mocking a connector provided by 3rd party (i.e. we can assume they tested it)
# to insulate from external dependencies, which are scope of system integration test, not unit test.
# In theory, we should also add test for requests.exceptions.ConnectionError and non recoverable exceptions.
# Those tests are omitted for brevity and because we can reasonably trust the backoff module works fine and is not changing
# as requirement is frozen to a particular version.
# In case we rely on our own implementations, it is better to have more tests to catch for changes in either module.

# mocks quandl.get under optimal case
def mock_quandl_get_bau(name):
  resources_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'resources'))
  testdata = pandas.read_csv(os.path.join(resources_path,'testdata.csv'))
  return testdata 

# mocks quandl.get to timeout once
to1_count = 0
def mock_quandl_get_to1(name):
  global to1_count
  if to1_count <1:
    to1_count+=1
    raise requests.exceptions.Timeout
  resources_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'resources'))
  testdata = pandas.read_csv(os.path.join(resources_path,'testdata.csv'))
  return testdata 

# mocks quandl.get to timeout always
def mock_quandl_get_to_always(name):
  raise requests.exceptions.Timeout

class IngestorTestSuite(unittest.TestCase):
  """Basic test cases."""

  @mock.patch('quandl.get', side_effect=mock_quandl_get_bau)
  def test_bau(self, quandl_get_function):
    resources_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'resources'))
    expected = pandas.read_csv(os.path.join(resources_path,'testdata.csv'))
    mock_dao = mock.MagicMock(spec=ingestionDao.IngestionDaoItf)
    i = ingestor.IngestorQuandl('mock_key', mock_dao)
    actual = i.get_data('test_data')
    pandas.testing.assert_frame_equal(actual, expected)
    assert quandl_get_function.called
    assert quandl_get_function.call_count == 1
    args, kwargs = quandl_get_function.call_args
    assert args == ('test_data',)
    assert kwargs == {}
    assert mock_dao.persist_dataframe.called 
    assert mock_dao.persist_dataframe.call_count == 1

  # Test that the ingestor works fine even if the quandl connector raises timeout exception 1<3 times
  @mock.patch('quandl.get', side_effect=mock_quandl_get_to1)
  def test_to1(self, quandl_get_function):
    resources_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'resources'))
    expected = pandas.read_csv(os.path.join(resources_path,'testdata.csv'))
    mock_dao = mock.MagicMock(spec=ingestionDao.IngestionDaoItf)
    i = ingestor.IngestorQuandl('mock_key', mock_dao)
    actual = i.get_data('test_data')
    pandas.testing.assert_frame_equal(actual, expected)
    assert quandl_get_function.called
    assert quandl_get_function.call_count == 2
    args, kwargs = quandl_get_function.call_args
    assert args == ('test_data',)
    assert kwargs == {}
    assert mock_dao.persist_dataframe.called 
    assert mock_dao.persist_dataframe.call_count == 1
    
  # Test that the ingestor fails if the quandl connector raises timeout exception >3 times
  @mock.patch('quandl.get', side_effect=mock_quandl_get_to_always)
  def test_to4(self, quandl_get_function):
    resources_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'resources'))
    expected = pandas.read_csv(os.path.join(resources_path,'testdata.csv'))
    mock_dao = mock.MagicMock(spec=ingestionDao.IngestionDaoItf)
    i = ingestor.IngestorQuandl('mock_key', mock_dao)
    with self.assertRaises(Exception) as context:
        actual = i.get_data('test_data')
    assert quandl_get_function.called
    assert quandl_get_function.call_count == 3
    args, kwargs = quandl_get_function.call_args
    assert args == ('test_data',)
    assert kwargs == {}
    assert not mock_dao.persist_dataframe.called 
    

if __name__ == '__main__':
  unittest.main()




