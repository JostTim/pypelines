import pytest

# for testing on local version, instead of installed version,
# this may not be desired as testing uninstalled may not catch issues that occur after installation is performed
# comment the next three lines to test installed version
# import sys
# from pathlib import Path
# sys.path.append(str(Path(__file__).resolve().parent / "src"))

from pypelines import examples
from pypelines.sessions import Session

from pypelines import Pipeline, stepmethod, BaseStep
from pypelines.pickle_backend import PicklePipe


@pytest.fixture
def test_class_based_pypeline():

    pipeline = Pipeline("test_class_based")

    @pipeline.register_pipe
    class MyPipe(PicklePipe):
        class Steps:
            def my_step(self, session, extra=""):
                return 1

            my_step.requires = []

    return pipeline


@pytest.fixture
def test_method_based_pypeline():

    pipeline = Pipeline("test_method_based")

    @pipeline.register_pipe
    class MyPipe(PicklePipe):

        @stepmethod(requires=[])
        def my_step(self, session, extra=""):
            return 1


def test_pypeline_creation(test_class_based_pypeline):
    assert isinstance(test_class_based_pypeline.MyPipe.my_step, BaseStep)
    assert hasattr(test_class_based_pypeline.MyPipe.my_step, "generate")
    assert hasattr(test_class_based_pypeline.MyPipe.my_step, "load")
    assert hasattr(test_class_based_pypeline.MyPipe.my_step, "save")
