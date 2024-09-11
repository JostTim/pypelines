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

from pathlib import Path


@pytest.fixture
def pipeline_steps_group_class_based():

    test_pipeline = Pipeline("test_class_based")

    @test_pipeline.register_pipe
    class MyPipe(PicklePipe):
        class Steps:
            def my_step(self, session, extra=""):
                return "a_good_result"

            my_step.requires = []

    return test_pipeline


@pytest.fixture
def pipeline_method_based():

    test_pipeline = Pipeline("test_method_based")

    @test_pipeline.register_pipe
    class MyPipe(PicklePipe):

        @stepmethod(requires=[])
        def my_step(self, session, extra=""):
            return "a_good_result"

    return test_pipeline


def get_pipelines_fixtures():
    return ["pipeline_method_based", "pipeline_steps_group_class_based"]


@pytest.fixture
def session_root_path():
    directory = Path("./tests/temp_sessions_directory")
    directory.mkdir(parents=True, exist_ok=True)
    yield directory

    if directory.exists():

        def remove_directory(path: Path):
            print("removing :", path)
            for child in path.iterdir():
                if child.is_file():
                    child.unlink()
                else:
                    remove_directory(child)
            path.rmdir()

        remove_directory(directory)


@pytest.fixture
def session(session_root_path):
    test_session = Session(subject="test_subject", date="2024-10-05", number=1, auto_path=True, path=session_root_path)
    return test_session


@pytest.mark.parametrize("pipeline_fixture_name", get_pipelines_fixtures())
def test_pypeline_creation(request, pipeline_fixture_name):
    pipeline = request.getfixturevalue(pipeline_fixture_name)

    assert isinstance(pipeline.my_pipe.my_step, BaseStep)
    assert hasattr(pipeline.my_pipe.my_step, "generate")
    assert hasattr(pipeline.my_pipe.my_step, "load")
    assert hasattr(pipeline.my_pipe.my_step, "save")


@pytest.mark.parametrize("pipeline_fixture_name", get_pipelines_fixtures())
def test_pypeline_call(request, pipeline_fixture_name: str, session):
    pipeline = request.getfixturevalue(pipeline_fixture_name)

    # expecting the output to not be present if the pipeline step was not generated first
    with pytest.raises(ValueError):
        assert pipeline.my_pipe.my_step.load(session) == "a_good_result"

    # this only calculates and returns the pipeline step output, and do not saves it
    assert pipeline.my_pipe.my_step(session) == "a_good_result"

    # expecting the output to not be present if the pipeline step was not generated first
    with pytest.raises(ValueError):
        assert pipeline.my_pipe.my_step.load(session) == "a_good_result"

    # generate the pipeline step output to file (saves it with generation mechanism)
    assert pipeline.my_pipe.my_step.generate(session) == "a_good_result"

    # expecting the output to be present now
    assert pipeline.my_pipe.my_step.load(session) == "a_good_result"
