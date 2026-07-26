"""Tests for the consumer's pure logic: offset tracking and dead-lettering.

The batch/flush path needs a live Kafka and Postgres and is exercised by the
end-to-end run instead. What is unit-tested here is the part that is easy to
get subtly wrong and expensive to notice: where offsets resume from, and
whether a malformed message can be stored at all.
"""

from __future__ import annotations

from clickstream.consumer import _Batch, _for_storage


class TestForStorage:
    def test_passes_through_plain_text(self):
        assert _for_storage(b'{"a": 1}') == '{"a": 1}'

    def test_none_stays_none(self):
        assert _for_storage(None) is None

    def test_replaces_invalid_utf8(self):
        # Must not raise: the message is a dead letter precisely because its
        # bytes are not what we expected.
        assert _for_storage(b"\xff\xfe") is not None

    def test_strips_nul_bytes(self):
        """Postgres text columns reject NUL, and 'replace' does not remove it.

        Regression test: a message of invalid UTF-8 containing NUL made the
        dead-letter INSERT itself fail, turning one bad message into a poison
        pill that stalled the partition it was on.
        """
        out = _for_storage(b"\xff\xfe\x00\x01")
        assert out is not None
        assert "\x00" not in out

    def test_nul_is_kept_visible_as_an_escape(self):
        assert _for_storage(b"a\x00b") == "a\\x00b"

    def test_truncates_to_the_limit(self):
        assert len(_for_storage(b"x" * 20_000)) == 8000

    def test_respects_a_custom_limit(self):
        assert len(_for_storage(b"x" * 999, limit=10)) == 10


class TestBatchOffsets:
    def test_tracks_the_highest_offset_per_partition(self):
        batch = _Batch(topic="t")
        batch.note_offset(0, 5)
        batch.note_offset(0, 9)
        batch.note_offset(1, 2)
        assert batch.max_offsets == {0: 9, 1: 2}

    def test_out_of_order_offsets_do_not_lower_the_mark(self):
        # Committing a lower offset would replay messages already written.
        batch = _Batch(topic="t")
        batch.note_offset(0, 9)
        batch.note_offset(0, 5)
        assert batch.max_offsets == {0: 9}

    def test_commits_the_next_offset_to_read(self):
        """Kafka commits the offset to resume FROM, not the last one read.

        Committing the last-read offset instead would redeliver one message
        per partition on every restart.
        """
        batch = _Batch(topic="events")
        batch.note_offset(3, 41)
        committed = batch.commit_offsets()
        assert len(committed) == 1
        assert committed[0].topic == "events"
        assert committed[0].partition == 3
        assert committed[0].offset == 42

    def test_offsets_cover_every_partition_touched(self):
        batch = _Batch(topic="events")
        for partition in range(4):
            batch.note_offset(partition, partition * 10)
        assert {tp.partition: tp.offset for tp in batch.commit_offsets()} == {
            0: 1,
            1: 11,
            2: 21,
            3: 31,
        }

    def test_length_counts_accepted_and_rejected(self):
        # The flush trigger is total messages handled, not just the good ones,
        # so a burst of malformed messages still flushes on schedule.
        batch = _Batch(topic="t")
        batch.events.append((object(), 0, 1))
        batch.rejected.append(object())
        assert len(batch) == 2

    def test_a_new_batch_is_empty(self):
        assert len(_Batch(topic="t")) == 0
        assert _Batch(topic="t").commit_offsets() == []
