from .pickle_backend import PicklePipe
from .pipelines import Pipeline
from .steps import stepmethod

example_pipeline = Pipeline("example")


@example_pipeline.register_pipe
class treated_videos(PicklePipe):
    # single_step = True

    @stepmethod()
    def compress(self, session, video_codec="ffmpeg", extra="", compression_rate=0.5):
        return {
            "pixels": [1, 2, 5, 7, 18, 8, 9, 8, 21],
            "video_codec": video_codec,
            "compression_rate": compression_rate,
        }


@example_pipeline.register_pipe
class modified_videos(PicklePipe):
    @stepmethod(requires="local_features.templates_new_locations", version="1")
    def draw_templates(self, session, extra=""):
        video = self.pipeline.treated_videos.compress.load(session, extra)["pixels"]
        templates = self.pipeline.local_features.templates_new_locations.load(session, extra)
        video = video + templates["processed_data"]
        return {"video": video}

    @stepmethod(requires=["modified_videos.draw_templates", "background_features.detect_buildings"], version="1")
    def draw_godzilla(self, session, roar="grrrr", extra=""):
        obj = self.object()
        obj["caption"] = roar
        return obj


@example_pipeline.register_pipe
class background_features(PicklePipe):
    @stepmethod(version="1", requires="background_features.enhanced_background")
    def blobs(self, session, argument1, extra="", optionnal_argument2="5"):
        obj = self.object()
        obj["optionnal_argument2"] = optionnal_argument2
        return obj

    @stepmethod(requires="treated_videos.compress", version="2")
    def enhanced_background(self, session, extra="", clahe_object=None):
        return {"clahe_object": clahe_object}

    @stepmethod(requires="treated_videos.compress", version="3")
    def scale_spaces(self, session, scales="0", extra=""):
        # obj = self.object()
        # obj.update({"scales" : scales, "argument2" : "i"})
        return "testouillet"  # obj

    @stepmethod(requires="treated_videos.compress", version="3")
    def detect_buildings(self, session, scales, extra=""):
        obj = self.object()
        return {"scales": scales, "argument2": "i"}


@example_pipeline.register_pipe
class local_features(PicklePipe):
    @stepmethod(version="1", requires="background_features.scale_spaces")
    def template_matches(self, session, argument1=1, extra="", optionnal_argument2="1"):
        return {"argument1": argument1, "optionnal_argument2": optionnal_argument2}

    @stepmethod(requires=["local_features.template_matches", "background_features.blobs"], version="2")
    def templates_new_locations(self, session, new_locations, extra=""):
        obj = self.object()  # get previous object version from disk
        obj.update({"new_locations": new_locations, "processed_data": [int(loc) * int(loc) for loc in new_locations]})
        return obj
