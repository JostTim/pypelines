import unittest, sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pypelines

from pypelines import examples

class TestVersions(unittest.TestCase):

    def setUp(self):
        self.pipeline = examples.example_pipeline

    def test_pipeline_generate(self):
        self.assertEqual(self.pipeline.ExamplePipe.example_step1.generate("bonjour"), {"argument1" : "bonjour", "optionnal_argument2" : 23})

if __name__ == '__main__':
    unittest.main()


