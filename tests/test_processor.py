# -*- coding: utf-8 -*-

from context import butterwire
import os
import unittest
import pandas
import processor

class ProcessorTestSuite(unittest.TestCase):
  """Basic test cases."""
  def test_process(self):
    resources_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'resources'))
    testdata = pandas.read_csv(os.path.join(resources_path,'testdata.csv'))
    expected = pandas.read_csv(os.path.join(resources_path,'processor_process_expected.csv'))
    p = processor.ProcessorBw()
    actual = p.process(testdata, 0)
    pandas.testing.assert_frame_equal(actual, expected)


if __name__ == '__main__':
  unittest.main()
