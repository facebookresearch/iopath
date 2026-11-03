# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.


import threading
from collections.abc import Mapping
from typing import Any, TypeVar


try:
    from tmetry.simpleevent import SimpleEventRecord
    from tmetry.writer import TmetryWriter

    b_tmetry_available = True
except ImportError:
    b_tmetry_available = False

# pyrefly: ignore [invalid-type-var]
VTYPE = TypeVar("T", str, int, bool, float)


class _EventState(threading.local):
    """The event currently being assembled by one caller thread."""

    def __init__(self) -> None:
        self.event = SimpleEventRecord()

    def __reduce__(self) -> tuple[type["_EventState"], tuple[()]]:
        return (_EventState, ())


class EventLogger:
    """
    Base class for providing event logging in a path handler
    It implements event logging by wrapping the tmetry interface.
    If tmetry packages is not available, it is a no-op.
    """

    DEFAULT_TOPIC = "iopath_tmetry"
    OP_KEY = "op"

    # Interval after which event is logged to backend.
    SAMPLING_PERIOD = 10

    # Map to keep track of sample count per operation.
    sample_counts: dict[str, int] = {}
    _sample_counts_lock = threading.Lock()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if b_tmetry_available:
            self._writers = []
            self._event_state = _EventState()
            self._enabled = True

    def add_writer(self, writer: Any) -> None:
        if b_tmetry_available:
            if isinstance(writer, TmetryWriter):
                self._writers.append(writer)

    def add_key(self, key: str, val: VTYPE) -> None:
        if b_tmetry_available:
            self._event_state.event.set(key, val)

    def add_keys(self, kvs: Mapping[str, str | int | bool | float]) -> None:
        if b_tmetry_available:
            self._event_state.event.set_keys(dict(kvs))

    def _sample_record(self) -> bool:
        """
        Samples the current event and logs only when the count
        reaches logging interval.

        Returns:
            True: if this sample should be logged.
            False: otherwise.
        """
        evt_op = self._event_state.event.get(self.OP_KEY)
        if evt_op is None:
            # No op is set. Let's log it.
            return True

        with self._sample_counts_lock:
            if evt_op not in self.sample_counts:
                self.sample_counts[evt_op] = 1
                return True

            self.sample_counts[evt_op] += 1
            if self.sample_counts[evt_op] > self.SAMPLING_PERIOD:
                # Let's log this and reset sampling counter.
                self.sample_counts[evt_op] = 1
                return True

            # Skip this sample.
            return False

    def set_logging(self, enable: bool) -> None:
        self._enabled = enable

    def is_logging_enabled(self) -> bool:
        return self._enabled

    def log_event(self, topic: str | None = None) -> None:
        if b_tmetry_available and self._enabled:
            event = self._event_state.event

            # Sample the current event.
            if not self._sample_record():
                return

            if topic is None:
                topic = self.DEFAULT_TOPIC

            for writer in self._writers:
                writer.writeRecord(topic, event)
            self._event_state.event = SimpleEventRecord()
