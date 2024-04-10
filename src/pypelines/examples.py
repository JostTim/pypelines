from .pickle_backend import PicklePipe
from .pipelines import Pipeline
from .steps import stepmethod

example_pipeline = Pipeline("example")


@example_pipeline.register_pipe
class treated_videos(PicklePipe):
    # single_step = True

    @stepmethod()
    def compress(self, session, video_codec="ffmpeg", extra="", compression_rate=0.5):
        """Compresses a video using the specified video codec and compression rate.

        Args:
            session: The session to compress the video.
            video_codec (str): The video codec to use for compression. Default is "ffmpeg".
            extra (str): Any extra information for compression. Default is an empty string.
            compression_rate (float): The compression rate to apply to the video. Default is 0.5.

        Returns:
            dict: A dictionary containing the compressed video information including pixels, video_codec,
                and compression_rate.
        """
        return {
            "pixels": [1, 2, 5, 7, 18, 8, 9, 8, 21],
            "video_codec": video_codec,
            "compression_rate": compression_rate,
        }


@example_pipeline.register_pipe
class modified_videos(PicklePipe):
    @stepmethod(requires="local_features.templates_new_locations", version="1")
    def draw_templates(self, session, extra=""):
        """Draws templates on the video.

        Args:
            session: The session to load the data from.
            extra: Additional information to specify the data to load (default is "").

        Returns:
            A dictionary containing the processed video with templates drawn on it.
        """
        video = self.pipeline.treated_videos.compress.load(session, extra)["pixels"]
        templates = self.pipeline.local_features.templates_new_locations.load(session, extra)
        video = video + templates["processed_data"]
        return {"video": video}

    @stepmethod(requires=["modified_videos.draw_templates", "background_features.detect_buildings"], version="1")
    def draw_godzilla(self, session, roar="grrrr", extra=""):
        """Draws a Godzilla with a caption and optional extra information.

        Args:
            session: The session to use for drawing.
            roar (str): The sound Godzilla makes (default is "grrrr").
            extra (str): Any extra information to include in the drawing (default is "").

        Returns:
            dict: A dictionary representing the drawn Godzilla with the caption and extra information.
        """
        obj = self.object()
        obj["caption"] = roar
        return obj


@example_pipeline.register_pipe
class background_features(PicklePipe):
    @stepmethod(version="1", requires="background_features.enhanced_background")
    def blobs(self, session, argument1, extra="", optionnal_argument2="5"):
        """Return a blob object with the specified arguments.

        Args:
            session: The session object.
            argument1: The first argument.
            extra (str, optional): Extra information. Defaults to "".
            optionnal_argument2 (str, optional): Another optional argument. Defaults to "5".

        Returns:
            dict: A blob object with the specified arguments.
        """
        obj = self.object()
        obj["optionnal_argument2"] = optionnal_argument2
        return obj

    @stepmethod(requires="treated_videos.compress", version="2")
    def enhanced_background(self, session, extra="", clahe_object=None):
        """Return a dictionary containing the 'clahe_object' parameter.

        Args:
            session: The session object.
            extra (str): An optional extra parameter (default is "").
            clahe_object: An optional CLAHE object (default is None).

        Returns:
            dict: A dictionary containing the 'clahe_object' parameter.
        """
        return {"clahe_object": clahe_object}

    @stepmethod(requires="treated_videos.compress", version="3")
    def scale_spaces(self, session, scales="0", extra=""):
        """Scale the spaces based on the provided scales and extra parameters.

        Args:
            session: The session object.
            scales (str): The scales to be applied (default is "0").
            extra (str): Any extra parameters to be considered (default is "").

        Returns:
            str: The result of scaling the spaces.
        """
        # obj = self.object()
        # obj.update({"scales" : scales, "argument2" : "i"})
        return "testouillet"  # obj

    @stepmethod(requires="treated_videos.compress", version="3")
    def detect_buildings(self, session, scales, extra=""):
        """Detect buildings using the specified session and scales.

        Args:
            session: The session to use for detecting buildings.
            scales: The scales to be used for detection.
            extra: Additional information (default is an empty string).

        Returns:
            A dictionary containing the scales and an example argument.


        """
        obj = self.object()
        return {"scales": scales, "argument2": "i"}


@example_pipeline.register_pipe
class local_features(PicklePipe):
    @stepmethod(version="1", requires="background_features.scale_spaces")
    def template_matches(self, session, argument1=1, extra="", optionnal_argument2="1"):
        """Return a dictionary with the values of argument1 and optionnal_argument2.

        Args:
            session: The session object.
            argument1: An integer representing the first argument (default is 1).
            extra: An optional string argument (default is an empty string).
            optionnal_argument2: An optional string argument (default is "1").

        Returns:
            A dictionary with the values of argument1 and optionnal_argument2.
        """
        return {"argument1": argument1, "optionnal_argument2": optionnal_argument2}

    @stepmethod(requires=["local_features.template_matches", "background_features.blobs"], version="2")
    def templates_new_locations(self, session, new_locations, extra=""):
        """Update the object with new locations and processed data.

        Args:
            session: The session object.
            new_locations: A list of new locations to be added.
            extra: An optional extra parameter (default is an empty string).

        Returns:
            The updated object with new locations and processed data.
        """
        obj = self.object()  # get previous object version from disk
        obj.update({"new_locations": new_locations, "processed_data": [int(loc) * int(loc) for loc in new_locations]})
        return obj
