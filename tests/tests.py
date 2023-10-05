import unittest, sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pypelines

from . instances import pipeline_test_instance

class TestVersions(unittest.TestCase):

    def setUp(self):
        self.version_handler = pypelines.HashVersionHandler('version_example.py')
        self.pipeline = pipeline_test_instance

    def test_function_hash(self):
        self.assertEqual(self.version_handler.get_function_hash(), "ad2d1f4")

if __name__ == '__main__':
    unittest.main()


