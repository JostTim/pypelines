## Pypelines

### Core Components
1. **Step**: Represents a processing stage that can take 0 to multiple inputs and produce an output. It uses a `DiskObject` to manage the output's existence and storage.
2. **Pipe**: A collection of steps. It helps in organizing steps that process the same data structure, preventing data duplication by adding new information to the existing data.
3. **Pipeline**: A collection of pipes and steps that handle dependencies and execution flow.

### Key Features
- **Dependency Management**: Automatically resolves dependencies based on the declared input requirements of each step.
- **Automatic Execution**: Runs necessary upstream steps when retrieving a specific step in the pipeline.
- **Graph Representation**: Visualizes the pipeline steps and their dependencies.

### Installation
You can install the package using `pip` or `pdm`:
```bash
pip install processing-pypelines
# or with pdm
pdm add processing-pypelines
```

### Basic Usage Example
The example provided demonstrates how to create a pipeline with multiple pipes and steps, handling data from CSV files:
1. Define the pipeline.
2. Register pipes and their corresponding steps.
3. Implement the worker methods for each step.
4. Visualize, generate, and process the pipeline steps.

```python
from pypelines import Pipeline, BasePipe, BaseStep, Session, pickle_backend
from pathlib import Path
import pandas, numpy, json

ROIS_URL = "https://raw.githubusercontent.com/JostTim/pypelines/refs/heads/main/tests/data/rois_df.csv"
TRIALS_URL = "https://raw.githubusercontent.com/JostTim/pypelines/refs/heads/main/tests/data/trials_df.csv"

# Initialize the pipeline
pipeline = Pipeline("my_neurophy_pipeline")

# Define pipes and steps
@pipeline.register_pipe
class ROIsTablePipe(BasePipe):
    pipe_name = "rois_df"
    disk_class = pickle_backend.PickleDiskObject

    class InitialCalculation(BaseStep):
        step_name = "read"

        def worker(self, session, extra=""):
            rois_data = pandas.read_csv(ROIS_URL).set_index("roi#")
            rois_data["F_norm"] = rois_data["F_norm"].apply(json.loads)
            return rois_data

@pipeline.register_pipe
class TrialsTablePipe(BasePipe):
    pipe_name = "trials_df"
    disk_class = pickle_backend.PickleDiskObject

    class InitialRead(BaseStep):
        step_name = "read"

        def worker(self, session, extra=""):
            trials_data = pandas.read_csv(TRIALS_URL).set_index("trial#")
            return trials_data

    class AddFrameTimes(BaseStep):
        step_name = "frame_times"
        requires = "trials_df.read"

        def worker(self, session, extra="", sample_frequency_ms=1000/30):
            def get_frame(time_ms):
                return int(numpy.round(time_ms / sample_frequency_ms))
            trials_data = self.load_requirement("trials_df", session)
            trials_data["trial_start_frame"] = trials_data["trial_start_global_ms"].apply(get_frame)
            trials_data["stimulus_start_frame"] = trials_data["stimulus_start_ms"].apply(get_frame)
            trials_data["stimulus_change_frame"] = trials_data["stimulus_change_ms"].apply(get_frame)
            return trials_data

@pipeline.register_pipe
class TrialsCrossRoisTablePipe(BasePipe):
    pipe_name = "trials_rois_df"
    disk_class = pickle_backend.PickleDiskObject

    class InitialMerge(BaseStep):
        step_name = "merge"
        requires = ["rois_df.read", "trials_df.frame_times"]

        def worker(self, session, extra=""):
            trials_data = self.load_requirement("trials_df", session)
            rois_data = self.load_requirement("rois_df", session)

            trials_starts = trials_data["trial_start_frame"].to_list() + [len(rois_data["F_norm"].iloc[0])]

            trials_rois_data = []
            for roi_id, roi_details in rois_data.iterrows():
                roi_details = roi_details.to_dict()
                roi_fluorescence = roi_details.pop("F_norm")
                for trial_nb, (trial_id, trial_details) in enumerate(trials_data.iterrows()):
                    new_row = {"roi#": roi_id, "trial#": trial_id}
                    new_row["F_norm"] = roi_fluorescence[trials_starts[trial_nb]:trials_starts[trial_nb+1]]
                    new_row.update(trial_details.to_dict())
                    new_row.update(roi_details)
                    trials_rois_data.append(new_row)

            return pandas.DataFrame(trials_rois_data).set_index(["roi#", "trial#"])

# Draw the pipeline graph
pipeline.graph.draw()

# Create a session and generate the data
session = Session(subject="test", date="2025-05-15", number=1, path=".", auto_path=True)
trials_roi_df = pipeline.trials_rois_df.merge.generate(session=session, check_requirements=True)
```

### Additional Information
- **Visualization**: You can visualize the pipeline graph using `pipeline.graph.draw()`.
- **Session Management**: The `Session` class manages session-specific data. It must implement a ``.subject``, ``.date`` and ``.number`` method to identify the unique session. It must also identify a ``.path`` method that points out a string to the location of this session's root path.


### Generation Mechanism

![generation_flowchart](\.assets\PypelinesGenerate.svg)

1. Inputs to Generate:

    - ``session``:  
    The current session object.
    - ``extra`` :  
    Additional information for the step in case it can save several different outputs per session.
    - ``*args`` and ``**kwargs`` :  
    other positionnal or kewyord arguments you defined in the worker method associated to your step.
    - generation added arguments :
        - ``skip`` (bool): Skips the step if the output is already available on disk.
        - ``refresh`` (bool): Forces the regeneration of the step even if output is already available.
        - ``refresh_requirements`` (bool/str/list): Refreshes specific upstream steps.
        - ``check_requirements`` (bool): Verifies and generates the necessary outputs of upstream steps.
        - ``save_o_utput`` (bool): Whether to save the output of the step to disk.

2. Steps Involved in the Generate Mechanism:

    - **Initialize Logger**:  
        Sets up a logger for the step.
    - **Resolve Pipeline**:  
        Ensures the pipeline structure is fully resolved.
    - **Check Skip/Refresh Flags**:  
        If Refresh = True: Ignore existing output and regenerate.  
        If Skip = True: Check if output exists.  
        If yes, skip loading the output (to save some time if at the middle of a processing chain).
    - **Check Dependencies (Requirements)**:  
        Determine if any dependencies need to be regenerated due to changes or requirements.  
        If Yes: Trigger regeneration of dependencies with specific generate method calls.
    - **Disk Object Operations**:  
        Create a disk object for this step using the session and extra parameters.  
        If Output exists and not refreshed: Try to load the existing output from disk.  
        If Output does not exist or needs refresh: Proceed to execute the worker method.
    - **Execute Worker Method**:  
        Call the worker method with the provided inputs. If the worker requires a , it's it responsibility to load it. The helping method for that is ``self.load_requirement(session, required_pipe_name)``. It uses the infos supplied by the ``requires`` list defined in the ``Step``, to identify which level (Step) of the required ``Pipe`` to load. Therefore, you don't need to supply a specific ``pipe_name.step_name`` to ``load_requirement`` but just ``pipe_name``.
    - **Post Processing**:  
        If ``save_output`` is ``True``: Save the output to disk using the disk_class's save method.
        Trigger any save callbacks for additional processing.
    - **Return Output**:  
        Return the output generated by the worker method.
