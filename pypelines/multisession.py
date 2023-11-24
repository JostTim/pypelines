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

    def generate(self, sessions, *args, extras=None, extra=None, **kwargs):
        session_result_dict = {}

        if extra is not None:
            if extras is not None:
                raise ValueError(
                    "Ambiguous arguments extra and extras. "
                    "They cannot be used at the same time.\n"
                    "extra sets the same extra value for all session, "
                    "while extras excepts a list like and "
                    "allows to set one value per session (order based)"
                )
            extras = [extra] * len(sessions)
        else:
            if extras is None:
                extras = [extras] * len(sessions)
        if not isinstance(extras, (list, tuple)):
            raise ValueError(
                "if extras is used, it must be a list of the same size of the sessions number supplied. "
                "If you want to supply a single value for all, use extra instead of extras"
            )

        if len(extras) != len(sessions):
            raise ValueError(
                "The number of extra values supplied is different than the number of sessions. Cannot map them."
            )

        for (index, session), extra in zip(sessions.iterrows(), extras):
            session_result_dict[index] = self.step.generate(
                session, *args, extra=extra, **kwargs
            )

        return self._packer(sessions, session_result_dict)
