class BaseMultisessionAccessor:
    def __init__(self, parent):
        self.step = parent
        self._packer = self.step.pipe.disk_class.multisession_packer
        self._unpacker = self.step.pipe.disk_class.multisession_unpacker

    def load(self, sessions, extras=None):
        session_result_dict = {}

        if not isinstance(extras, (list, tuple)):
            extras = [extras] * len(sessions)

        if len(extras) != len(sessions):
            raise ValueError(
                "The number of extra values supplied is different than the number of sessions. Cannot map them."
            )

        for (index, session), extra in zip(sessions.iterrows(), extras):
            session_result_dict[index] = self.step.load(session, extra=extra)

        return self._packer(sessions, session_result_dict)

    def save(self, sessions, datas, extras=None):
        if not isinstance(extras, (list, tuple)):
            extras = [extras] * len(sessions)

        if len(extras) != len(sessions):
            raise ValueError(
                "The number of extra values supplied is different than the number of sessions. Cannot map them."
            )

        for (session, data), extra in zip(self._unpacker(sessions, datas), extras):
            self.step.save(session, data, extra=extra)

        return None

    def generage(self, sessions, *args, extras=None, **kwargs):
        session_result_dict = {}

        if not isinstance(extras, (list, tuple)):
            extras = [extras] * len(sessions)

        if len(extras) != len(sessions):
            raise ValueError(
                "The number of extra values supplied is different than the number of sessions. Cannot map them."
            )

        for (index, session), extra in zip(sessions.iterrows(), extras):
            session_result_dict[index] = self.step.save(
                session, *args, extra=extra, **kwargs
            )

        return self._packer(sessions, session_result_dict)
