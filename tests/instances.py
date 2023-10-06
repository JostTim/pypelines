
import pypelines

pipeline_test_instance = pypelines.BasePipeline()


@pipeline_test_instance.register_pipe
class TestPipe(pypelines.BasePipe):
    