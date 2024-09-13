import pytest

# for testing on local version, instead of installed version,
# this may not be desired as testing uninstalled may not catch issues that occur after installation is performed
# comment the next three lines to test installed version
# import sys
# from pathlib import Path
# sys.path.append(str(Path(__file__).resolve().parent / "src"))

from pypelines import examples
from pypelines.sessions import Session

from pypelines import Pipeline, stepmethod, BaseStep, BasePipe
from pypelines.pickle_backend import PicklePipe, PickleDiskObject

from pathlib import Path


@pytest.fixture
def pipeline_method_based():

    test_pipeline = Pipeline("test_method_based")

    @test_pipeline.register_pipe
    class MyPipe(PicklePipe):

        @stepmethod(requires=[])
        def my_step(self, session, extra=""):
            return "a_good_result"

    @test_pipeline.register_pipe
    class complex_pipe(BasePipe):

        @stepmethod(disk_class=PickleDiskObject)
        def my_step_name(self, session, extra=""):
            return 154

        @stepmethod(requires="complex_pipe.my_step_name", disk_class=PickleDiskObject)
        def another_name(self, session, extra=""):
            data = self.load_requirement("complex_pipe", session, extra=extra)
            return data - 100

    return test_pipeline


@pytest.fixture
def pipeline_steps_group_class_based():

    test_pipeline = Pipeline("test_group_based")

    @test_pipeline.register_pipe
    class MyPipe(PicklePipe):
        class Steps:
            def my_step(self, session, extra=""):
                return "a_good_result"

            my_step.requires = []

    @test_pipeline.register_pipe
    class complex_pipe(BasePipe):

        class Steps:
            @stepmethod(disk_class=PickleDiskObject)
            def my_step_name(self, session, extra=""):
                return 154

            def another_name(self, session, extra=""):
                data = self.load_requirement("complex_pipe", session, extra=extra)
                return data - 100

            another_name.requires = "complex_pipe.my_step_name"
            another_name.disk_class = PickleDiskObject

    return test_pipeline


@pytest.fixture
def pipeline_class_based():

    test_pipeline = Pipeline("test_class_based")

    @test_pipeline.register_pipe
    class MyPipe(PicklePipe):

        class MyStep(BaseStep):
            def worker(self, session, extra=""):
                return "a_good_result"

    @test_pipeline.register_pipe
    class MyComplexPipe(BasePipe):

        pipe_name = "complex_pipe"

        class MyStep(BaseStep):
            def worker(self, session, extra=""):
                return 154

            disk_class = PickleDiskObject
            step_name = "my_step_name"

        class MyStep2(BaseStep):
            def worker(self, session, extra=""):
                data = self.load_requirement("complex_pipe", session, extra=extra)
                return data - 100

            step_name = "another_name"
            requires = "complex_pipe.my_step_name"
            disk_class = PickleDiskObject

    return test_pipeline


def get_pipelines_fixtures():
    return ["pipeline_method_based", "pipeline_steps_group_class_based", "pipeline_class_based"]


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
    assert len(pipeline.my_pipe.steps) == 1


@pytest.mark.parametrize("pipeline_fixture_name", get_pipelines_fixtures())
def test_pypeline_call(request, pipeline_fixture_name: str, session):
    pipeline: Pipeline = request.getfixturevalue(pipeline_fixture_name)

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


@pytest.mark.parametrize("pipeline_fixture_name", get_pipelines_fixtures())
def test_pypeline_requirement_stack(request, pipeline_fixture_name: str, session):
    pipeline: Pipeline = request.getfixturevalue(pipeline_fixture_name)

    # before being resolved (called), requires is a list of string
    assert pipeline.complex_pipe.another_name.requires == ["complex_pipe.my_step_name"]

    # expect no result is present on disk because we didn't check_requirements
    with pytest.raises(ValueError):
        pipeline.complex_pipe.another_name.generate(session)

    assert pipeline.complex_pipe.another_name.generate(session, check_requirements=True) == 54

    # now, requires has been resolved and contains instanciated step objects
    assert pipeline.complex_pipe.another_name.requires == [pipeline.complex_pipe.my_step_name]
