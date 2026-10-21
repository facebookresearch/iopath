# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import copy
import pickle
import threading
import unittest
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Protocol
from unittest.mock import patch

from iopath.common import event_logger as event_logger_module
from iopath.common.event_logger import EventLogger


class _Event(Protocol):
    def get_keys(self) -> dict[str, str | int | bool | float]: ...


class _IteratingWriter:
    def __init__(self) -> None:
        self.iteration_started = threading.Event()
        self.mutation_complete = threading.Event()
        self.record: dict[str, str | int | bool | float] | None = None

    def writeRecord(self, topic: str, event: _Event) -> None:
        items = iter(event.get_keys().items())
        first_item = next(items)
        self.iteration_started.set()
        if not self.mutation_complete.wait(timeout=1.0):
            raise TimeoutError("Concurrent mutation did not complete")
        self.record = dict([first_item, *items])


class _RecordingWriter:
    def __init__(self) -> None:
        self.records: list[dict[str, str | int | bool | float]] = []

    def writeRecord(self, topic: str, event: _Event) -> None:
        self.records.append(dict(event.get_keys()))


def _pickle_round_trip(logger: EventLogger) -> EventLogger:
    return pickle.loads(pickle.dumps(logger))


def _records_after_copy(
    copy_logger: Callable[[EventLogger], EventLogger],
) -> list[dict[str, str | int | bool | float]]:
    logger = EventLogger()
    logger.add_key("before_copy", "stale")
    copied_logger = copy_logger(logger)
    writer = _RecordingWriter()

    with patch.object(event_logger_module, "TmetryWriter", _RecordingWriter):
        copied_logger.add_writer(writer)

    copied_logger.add_key("after_copy", "fresh")
    copied_logger.log_event()
    return writer.records


class EventLoggerTest(unittest.TestCase):
    def test_concurrent_events_do_not_share_mutable_records(self) -> None:
        logger = EventLogger()
        writer = _IteratingWriter()

        with patch.object(event_logger_module, "TmetryWriter", _IteratingWriter):
            logger.add_writer(writer)

        def write_event() -> None:
            logger.add_keys({"first": "1", "second": "2"})
            logger.log_event()

        def mutate_event() -> None:
            if not writer.iteration_started.wait(timeout=1.0):
                raise TimeoutError("Event iteration did not start")
            logger.add_key("third", "3")
            writer.mutation_complete.set()

        with ThreadPoolExecutor(max_workers=2) as pool:
            write_future = pool.submit(write_event)
            mutation_future = pool.submit(mutate_event)
            mutation_future.result()
            write_future.result()

        self.assertEqual(writer.record, {"first": "1", "second": "2"})

    def test_deepcopy_recreates_empty_event_state(self) -> None:
        self.assertEqual(
            _records_after_copy(copy.deepcopy),
            [{"after_copy": "fresh"}],
        )

    def test_pickle_recreates_empty_event_state(self) -> None:
        self.assertEqual(
            _records_after_copy(_pickle_round_trip),
            [{"after_copy": "fresh"}],
        )

    def test_sampling_behavior_is_preserved(self) -> None:
        logger = EventLogger()
        logger.sample_counts = {}
        logger.SAMPLING_PERIOD = 2
        writer = _RecordingWriter()

        with patch.object(event_logger_module, "TmetryWriter", _RecordingWriter):
            logger.add_writer(writer)

        for sequence in range(3):
            logger.add_keys({"op": "sampling_test", "sequence": sequence})
            logger.log_event()

        self.assertEqual(
            writer.records,
            [
                {"op": "sampling_test", "sequence": 0},
                {"op": "sampling_test", "sequence": 2},
            ],
        )
