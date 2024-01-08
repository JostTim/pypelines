import unittest

# for testing on local version, instead of installed version,
# this may not be desired as testing uninstalled may not catch issues that occur after installation is performed
# comment the next three lines to test installed version
# import sys
# from pathlib import Path
# sys.path.append(str(Path(__file__).resolve().parent / "src"))

from pypelines import examples
from pypelines.sessions import Session


class TestVersions(unittest.TestCase):
    def setUp(self):
        self.pipeline = examples.example_pipeline
        self.session = Session(subject="test_subject", date="2023-10-10", number=0, path="C:/test", auto_path=True)
        print(self.session.alias)

    def test_pipeline_generate(self):
        self.assertEqual(
            self.pipeline.ExamplePipe.example_step1.generate(self.session, "bonjour"),
            {"argument1": "bonjour", "optionnal_argument2": 23},
        )


if __name__ == "__main__":
    unittest.main()
