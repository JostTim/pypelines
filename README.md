# Pypelines

![Python Version from PEP 621 TOML](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2FJostTim%2Fpypelines%2Frefs%2Fheads%2Fmain%2Fpyproject.toml)
[![PyPI - Version](https://img.shields.io/pypi/v/processing_pypelines)](https://pypi.org/project/processing-pypelines/)
![coverage](https://gitlab.example.com/<namespace>/<project>/badges/<branch>/coverage.svg?job=coverage)
[![pdm-managed](https://img.shields.io/endpoint?url=https%3A%2F%2Fcdn.jsdelivr.net%2Fgh%2Fpdm-project%2F.github%2Fbadge.json)](https://pdm-project.org)

## Installation

```bash
pip install processing-pypelines
```
or with [pdm](https://pdm-project.org)
```bash
pdm add processing-pypelines
```

## Intro

This package aims at providing a lightweight yet powerfull framework for chaining processing tasks, on experimental sessions. 
It does so in a "data agnostic" way, which means it is interfaceable with already existing packages, that themselves provide some sort of pipelining (such as suite2p for example).

Out of the box, it most easily handles processing steps that takes diven set of parameters and/or files as input, and outputs built-in python dictionnaries, or pandas DataFrames.
To interface if with more complex or intricated input/output data structures, you need to implement custom versions of ``DiskObject``, wich are the readers/writers that define how to check the 
existence of a ``Step`` output (to know when to skip it if the output exists) how to save such output, and how to load it in case a ``Step`` further in the ``Pipeline`` wants to access this output for further processing stages.

## Concepts

As so, I identified that chopping tasks needed two key concepts that may facilitate the life of the developpers : ``Pipe`` and ``Step``

- ``Step``
A step is a processing stage. It takes an input 0 to virtually unlimited inputs and performs python execution to generate an output. The existance of it's output can be verified with the atached ``DiskObject``.

- ``Pipe``
A pipe is a collection of Steps. In real life situations, it is very often that the same data structure can be having several Steps that add some data to it. For example, imaging data can be obtained, and segmented into tables of neurons fluorescence over time. As the execution goes further in the processing pipeline, some of the processing steps may calculate the responsivness of the neurons to one or another type of stimulation, obtained in another step. It then seems logical to only add this responsiveness information to the tables data, to not overcrowd the disk with duplicated stages with increasingly detailed/further processed data. The pipe serves this purpose. When a given ``Step`` requires the output of another ``Step``, it looks up wich ``Pipe`` (e.g. general data structure) it is attached to, and loads the most advanced one available in that ``Pipe``. As such, it means a ``Step`` output at level 6 of a ``Pipe``, has to also be valid for the data that it holds, to a ``Step`` that requires the level 1 of this ``Pipe``. (meaning : as a rule of thumb, don't delete data from a ``Pipe`` output with increasing steps, but only add new fields/columns to it.)

Below, is the example of a full ``Pipeline`` graph :
![Pipeline Graph](./docs/documentation/.assets/pipeline_example.png)

On the same column, the dots represents the different Steps of a ``Pipe``. A ``Pipe`` is then a single column on this graph. Links between steps represent the dependancies between them.

The most usefull parts that this package allows you do to is :
- to auto-resolve this graph simply based on what you declared that a ``Step`` needs as input requirement (it will raise Exceptions to you about errors with cyclic dependancies you might have made, when creating the Pipeline at runtime, to help sort them out)
- to automatically run the required upstream Steps, whenever you decide to get a specific ``Step`` arbitrarily positionned in the tree.

## Examples : 

To implement a Pipeline, you have to define at least a Step, attached inside a Pipe.
Here is a simple example.
In only 70 lines of code, it allows to perform the automatic chaining for 4 Steps, loading and writing of intermediate processing outputs, without the developper requiring to take care for any of it.
You can test this example, and see the result on your own computer. (Two csv files located in the ``test/data`` folder of this repository will be used for demonstration purposes.)

```python
from pypelines import Pipeline, BasePipe, BaseStep, Session, pickle_backend
from pathlib import Path
import pandas, numpy, json

ROIS_URL = "https://raw.githubusercontent.com/JostTim/pypelines/refs/heads/main/tests/data/rois_df.csv"
TRIALS_URL = "https://raw.githubusercontent.com/JostTim/pypelines/refs/heads/main/tests/data/trials_df.csv"

pipeline = Pipeline("my_neurophy_pipeline")

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

        def worker(self, session, extra = ""):
            trials_data = pandas.read_csv(TRIALS_URL).set_index("trial#")            
            return trials_data
        
    class AddFrameTimes(BaseStep):
        step_name = "frame_times"
        requires = "trials_df.read"

        def worker(self, session, extra = "", sample_frequency_ms = 1000/30):
            def get_frame(time_ms):
                return int(numpy.round(time_ms / sample_frequency_ms))
            trials_data = self.load_requirement("trials_df",session)     
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

        def worker(self, session, extra = ""):
            trials_data = self.load_requirement("trials_df",session)  
            rois_data = self.load_requirement("rois_df",session)

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
```

```python
pipeline.graph.draw()
```

```python
session = Session(subject="test",date="2025-05-15",number=1,path=".",auto_path=True)

trials_roi_df = pipeline.trials_rois_df.merge.generate(session = session, check_requirements=True)
```