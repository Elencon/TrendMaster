r"""
C:\Economy\Invest\TrendMaster\tests\database\batch_operations\test_batch_processors.py
Tests for batch_operations package.
Path: tests/database/batch_operations/test_batch_processors.py
pytest tests/database/batch_operations/test_batch_processors.py
"""

import pytest
from unittest.mock import MagicMock, patch

from src.database.batch_operations import (
    BatchProcessor,
    InsertProcessor,
    UpdateProcessor,
    UpsertProcessor,
    DeleteProcessor,
    BaseBatchProcessor,
)


# ---------------------------------------------------------------------------
# Shared fixtures and helpers
# ---------------------------------------------------------------------------

BASE_PATH = "src.database.batch_operations"

def make_cursor(rowcount: int = 5) -> MagicMock:
    cursor = MagicMock()
    cursor.rowcount = rowcount
    return cursor


def make_conn(cursor: MagicMock, autocommit: bool = True) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.autocommit = autocommit
    return conn


def make_connection_manager(conn: MagicMock) -> MagicMock:
    cm = MagicMock()
    cm.get_connection.return_value.__enter__ = lambda s: conn
    cm.get_connection.return_value.__exit__ = MagicMock(return_value=False)
    return cm


def make_processor(cls, batch_size: int = 1000, data_validator=None):
    cursor = make_cursor()
    conn = make_conn(cursor)
    cm = make_connection_manager(conn)
    processor = cls(connection_manager=cm, data_validator=data_validator, batch_size=batch_size)
    return processor, conn, cursor


SAMPLE_RECORDS = [
    {"id": 1, "name": "Alice", "value": 10},
    {"id": 2, "name": "Bob",   "value": 20},
    {"id": 3, "name": "Carol", "value": 30},
]

KEY_COLUMNS = ["id"]


# ---------------------------------------------------------------------------
# BaseBatchProcessor
# ---------------------------------------------------------------------------

class TestBaseBatchProcessor:

    def test_init_defaults(self):
        cm = MagicMock()
        processor = BaseBatchProcessor(connection_manager=cm)
        assert processor.batch_size == 1000
        assert processor.data_validator is None
        assert processor.stats is not None

    def test_validate_records_no_validator(self):
        cm = MagicMock()
        processor = BaseBatchProcessor(connection_manager=cm)
        records = [{"id": 1}]
        validated, errors = processor.validate_records(records)
        assert validated == records
        assert errors == []

    def test_validate_records_with_validator(self):
        cm = MagicMock()
        validator = MagicMock()
        processor = BaseBatchProcessor(connection_manager=cm, data_validator=validator)
        records = [{"id": 1}]
        expected = ([{"id": 1}], ["some error"])

        with patch(f"{BASE_PATH}.base_processor.DataUtils.validate_records", return_value=expected):
            result = processor.validate_records(records)

        assert result == expected

    def test_update_progress_calls_callback(self):
        cm = MagicMock()
        processor = BaseBatchProcessor(connection_manager=cm)
        callback = MagicMock()
        processor.update_progress(50, 100, "my_table", callback)
        callback.assert_called_once_with(50, 100, "my_table")

    def test_update_progress_no_callback(self):
        cm = MagicMock()
        processor = BaseBatchProcessor(connection_manager=cm)
        processor.update_progress(50, 100, "my_table", None)

    def test_update_progress_callback_exception_does_not_propagate(self):
        cm = MagicMock()
        processor = BaseBatchProcessor(connection_manager=cm)
        callback = MagicMock(side_effect=RuntimeError("callback blew up"))
        processor.update_progress(1, 10, "t", callback)

    def test_handle_batch_error_returns_batch_size(self):
        cm = MagicMock()
        processor = BaseBatchProcessor(connection_manager=cm)
        result = processor.handle_batch_error(ValueError("oops"), 50, "insert")
        assert result == 50

    def test_handle_batch_error_updates_stats(self):
        cm = MagicMock()
        processor = BaseBatchProcessor(connection_manager=cm)
        processor.handle_batch_error(ValueError("oops"), 10, "insert")
        stats = processor.get_stats()
        assert stats["total_records_failed"] == 10

    def test_reset_stats(self):
        cm = MagicMock()
        processor = BaseBatchProcessor(connection_manager=cm)
        processor.handle_batch_error(ValueError("e"), 10, "insert")
        processor.reset_stats()
        stats = processor.get_stats()
        assert stats["total_records_failed"] == 0

    def test_get_stats_summary_returns_string(self):
        cm = MagicMock()
        processor = BaseBatchProcessor(connection_manager=cm)
        assert isinstance(processor.get_stats_summary(), str)

    def test_autocommit_restored_after_operation(self):
        """autocommit must be restored even if an inner exception occurs."""
        cursor = make_cursor()
        conn = make_conn(cursor)
        conn.autocommit = True
        cm = make_connection_manager(conn)
        processor = InsertProcessor(connection_manager=cm)

        with patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.generate_insert_sql",
                   return_value="INSERT ..."), \
             patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.records_to_tuples",
                   side_effect=RuntimeError("db error")):
            processor.insert_batch("t", SAMPLE_RECORDS)

        assert conn.autocommit is True


# ---------------------------------------------------------------------------
# InsertProcessor
# ---------------------------------------------------------------------------

class TestInsertProcessor:

    def _make(self, batch_size=1000, rowcount=3):
        cursor = make_cursor(rowcount)
        conn = make_conn(cursor)
        cm = make_connection_manager(conn)
        processor = InsertProcessor(connection_manager=cm, batch_size=batch_size)
        return processor, conn, cursor

    def test_empty_records_returns_zero(self):
        processor, _, _ = self._make()
        inserted, failed = processor.insert_batch("t", [])
        assert inserted == 0 and failed == 0

    def test_successful_insert(self):
        processor, conn, cursor = self._make(rowcount=3)
        with patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.generate_insert_sql",
                   return_value="INSERT ..."), \
             patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(1,), (2,), (3,)]):
            inserted, failed = processor.insert_batch("t", SAMPLE_RECORDS)

        assert inserted == 3
        assert failed == 0
        conn.commit.assert_called_once()

    def test_failed_batch_rolls_back(self):
        processor, conn, cursor = self._make()
        cursor.executemany.side_effect = Exception("db error")

        with patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.generate_insert_sql",
                   return_value="INSERT ..."), \
             patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(1,), (2,), (3,)]):
            inserted, failed = processor.insert_batch("t", SAMPLE_RECORDS)

        conn.rollback.assert_called()
        assert failed == len(SAMPLE_RECORDS)
        assert inserted == 0

    def test_no_commit_when_batch_fails(self):
        processor, conn, cursor = self._make()
        cursor.executemany.side_effect = Exception("db error")

        with patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.generate_insert_sql",
                   return_value="INSERT ..."), \
             patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(1,)]):
            processor.insert_batch("t", SAMPLE_RECORDS)

        conn.commit.assert_not_called()

    def test_batching_splits_records(self):
        processor, conn, cursor = self._make(batch_size=2, rowcount=2)
        with patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.generate_insert_sql",
                   return_value="INSERT ..."), \
             patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(1,), (2,)]):
            processor.insert_batch("t", SAMPLE_RECORDS)

        # 3 records with batch_size=2 → 2 executemany calls
        assert cursor.executemany.call_count == 2

    def test_progress_callback_called(self):
        processor, _, _ = self._make(rowcount=3)
        callback = MagicMock()

        with patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.generate_insert_sql",
                   return_value="INSERT ..."), \
             patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(1,), (2,), (3,)]):
            processor.insert_batch("t", SAMPLE_RECORDS, progress_callback=callback)

        callback.assert_called()

    def test_stats_updated_after_insert(self):
        processor, _, _ = self._make(rowcount=3)
        with patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.generate_insert_sql",
                   return_value="INSERT ..."), \
             patch(f"{BASE_PATH}.insert_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(1,), (2,), (3,)]):
            processor.insert_batch("t", SAMPLE_RECORDS)

        stats = processor.get_stats()
        assert stats["total_records_processed"] == 3
        assert stats["total_records_inserted"] == 3

    def test_no_connection_returns_failed(self):
        cm = MagicMock()
        cm.get_connection.return_value.__enter__ = lambda s: None
        cm.get_connection.return_value.__exit__ = MagicMock(return_value=False)
        processor = InsertProcessor(connection_manager=cm)

        inserted, failed = processor.insert_batch("t", SAMPLE_RECORDS)
        assert inserted == 0
        assert failed == len(SAMPLE_RECORDS)


# ---------------------------------------------------------------------------
# UpdateProcessor
# ---------------------------------------------------------------------------

class TestUpdateProcessor:

    def _make(self, batch_size=1000, rowcount=3):
        cursor = make_cursor(rowcount)
        conn = make_conn(cursor)
        cm = make_connection_manager(conn)
        processor = UpdateProcessor(connection_manager=cm, batch_size=batch_size)
        return processor, conn, cursor

    def test_empty_records_returns_zero(self):
        processor, _, _ = self._make()
        updated, failed = processor.update_batch("t", [], key_columns=["id"])
        assert updated == 0 and failed == 0

    def test_raises_without_key_columns(self):
        processor, _, _ = self._make()
        with pytest.raises(ValueError, match="key_columns must be specified"):
            processor.update_batch("t", SAMPLE_RECORDS, key_columns=[])

    def test_raises_when_key_column_missing_in_records(self):
        processor, _, _ = self._make()
        records = [{"name": "Alice"}]
        with pytest.raises(ValueError, match="Key columns missing"):
            processor.update_batch("t", records, key_columns=["id"])

    def test_raises_when_all_columns_are_keys(self):
        processor, _, _ = self._make()
        records = [{"id": 1}]
        with pytest.raises(ValueError, match="No columns available for update"):
            processor.update_batch("t", records, key_columns=["id"])

    def test_successful_update(self):
        processor, conn, cursor = self._make(rowcount=3)
        with patch(f"{BASE_PATH}.update_processor.DatabaseUtils.generate_update_sql",
                   return_value="UPDATE ..."), \
             patch(f"{BASE_PATH}.update_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(10, 1), (20, 2), (30, 3)]):
            updated, failed = processor.update_batch("t", SAMPLE_RECORDS, KEY_COLUMNS)

        assert updated == 3
        assert failed == 0
        conn.commit.assert_called_once()

    def test_failed_batch_rolls_back(self):
        processor, conn, cursor = self._make()
        cursor.executemany.side_effect = Exception("db error")

        with patch(f"{BASE_PATH}.update_processor.DatabaseUtils.generate_update_sql",
                   return_value="UPDATE ..."), \
             patch(f"{BASE_PATH}.update_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(10, 1)]):
            updated, failed = processor.update_batch("t", SAMPLE_RECORDS, KEY_COLUMNS)

        conn.rollback.assert_called()
        assert failed == len(SAMPLE_RECORDS)

    def test_autocommit_restored(self):
        processor, conn, cursor = self._make()
        conn.autocommit = True
        cursor.executemany.side_effect = Exception("err")

        with patch(f"{BASE_PATH}.update_processor.DatabaseUtils.generate_update_sql",
                   return_value="UPDATE ..."), \
             patch(f"{BASE_PATH}.update_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(1,)]):
            processor.update_batch("t", SAMPLE_RECORDS, KEY_COLUMNS)

        assert conn.autocommit is True

    def test_stats_updated_after_update(self):
        processor, _, _ = self._make(rowcount=3)
        with patch(f"{BASE_PATH}.update_processor.DatabaseUtils.generate_update_sql",
                   return_value="UPDATE ..."), \
             patch(f"{BASE_PATH}.update_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(10, 1), (20, 2), (30, 3)]):
            processor.update_batch("t", SAMPLE_RECORDS, KEY_COLUMNS)

        stats = processor.get_stats()
        assert stats["total_records_processed"] == 3
        assert stats["total_records_updated"] == 3


# ---------------------------------------------------------------------------
# UpsertProcessor
# ---------------------------------------------------------------------------

class TestUpsertProcessor:

    def _make(self, batch_size=1000, rowcount=3):
        cursor = make_cursor(rowcount)
        conn = make_conn(cursor)
        cm = make_connection_manager(conn)
        processor = UpsertProcessor(connection_manager=cm, batch_size=batch_size)
        return processor, conn, cursor

    def test_empty_records_returns_zero(self):
        processor, _, _ = self._make()
        result = processor.upsert_batch("t", [], key_columns=["id"])
        assert result == (0, 0, 0)

    def test_raises_without_key_columns(self):
        processor, _, _ = self._make()
        with pytest.raises(ValueError, match="key_columns required"):
            processor.upsert_batch("t", SAMPLE_RECORDS, key_columns=[])

    def test_raises_when_key_column_missing(self):
        processor, _, _ = self._make()
        records = [{"name": "Alice"}]
        with pytest.raises(ValueError, match="Key columns missing"):
            processor.upsert_batch("t", records, key_columns=["id"])

    def test_successful_upsert(self):
        processor, conn, cursor = self._make(rowcount=4)
        with patch(f"{BASE_PATH}.upsert_processor.DatabaseUtils.generate_upsert_sql",
                   return_value="INSERT ... ON DUPLICATE KEY UPDATE ..."), \
             patch(f"{BASE_PATH}.upsert_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(1,), (2,), (3,)]):
            ins, upd, failed = processor.upsert_batch("t", SAMPLE_RECORDS, KEY_COLUMNS)

        assert failed == 0
        assert ins + upd > 0
        conn.commit.assert_called_once()

    def test_failed_batch_rolls_back(self):
        processor, conn, cursor = self._make()
        cursor.executemany.side_effect = Exception("db error")

        with patch(f"{BASE_PATH}.upsert_processor.DatabaseUtils.generate_upsert_sql",
                   return_value="INSERT ..."), \
             patch(f"{BASE_PATH}.upsert_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(1,)]):
            ins, upd, failed = processor.upsert_batch("t", SAMPLE_RECORDS, KEY_COLUMNS)

        conn.rollback.assert_called()
        assert failed == len(SAMPLE_RECORDS)

    def test_autocommit_restored(self):
        processor, conn, cursor = self._make()
        conn.autocommit = True
        cursor.executemany.side_effect = Exception("err")

        with patch(f"{BASE_PATH}.upsert_processor.DatabaseUtils.generate_upsert_sql",
                   return_value="INSERT ..."), \
             patch(f"{BASE_PATH}.upsert_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(1,)]):
            try:
                processor.upsert_batch("t", SAMPLE_RECORDS, KEY_COLUMNS)
            except Exception:
                pass

        assert conn.autocommit is True

    def test_stats_updated_after_upsert(self):
        processor, _, _ = self._make(rowcount=3)
        with patch(f"{BASE_PATH}.upsert_processor.DatabaseUtils.generate_upsert_sql",
                   return_value="INSERT ..."), \
             patch(f"{BASE_PATH}.upsert_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(1,), (2,), (3,)]):
            processor.upsert_batch("t", SAMPLE_RECORDS, KEY_COLUMNS)

        stats = processor.get_stats()
        assert stats["total_records_processed"] == 3


# ---------------------------------------------------------------------------
# DeleteProcessor
# ---------------------------------------------------------------------------

class TestDeleteProcessor:

    def _make(self, batch_size=1000, rowcount=3):
        cursor = make_cursor(rowcount)
        conn = make_conn(cursor)
        cm = make_connection_manager(conn)
        processor = DeleteProcessor(connection_manager=cm, batch_size=batch_size)
        return processor, conn, cursor

    def test_empty_conditions_returns_zero(self):
        processor, _, _ = self._make()
        deleted, failed = processor.delete_batch("t", [])
        assert deleted == 0 and failed == 0

    def test_single_column_delete_uses_in_clause(self):
        processor, conn, cursor = self._make(rowcount=3)
        conditions = [{"id": 1}, {"id": 2}, {"id": 3}]
        processor.delete_batch("t", conditions)
        cursor.execute.assert_called()

    def test_single_column_deduplicates_values(self):
        processor, conn, cursor = self._make(rowcount=2)
        conditions = [{"id": 1}, {"id": 1}, {"id": 2}]
        processor.delete_batch("t", conditions)
        call_args = cursor.execute.call_args
        values_passed = call_args[0][1]
        assert len(values_passed) == 2

    def test_single_column_skips_none_values(self):
        processor, conn, cursor = self._make(rowcount=1)
        conditions = [{"id": None}, {"id": 1}]
        processor.delete_batch("t", conditions)
        call_args = cursor.execute.call_args
        values_passed = call_args[0][1]
        assert None not in values_passed

    def test_composite_key_uses_executemany(self):
        processor, conn, cursor = self._make(rowcount=3)
        conditions = [
            {"id": 1, "tenant": "a"},
            {"id": 2, "tenant": "b"},
        ]
        with patch(f"{BASE_PATH}.delete_processor.DatabaseUtils.generate_delete_sql",
                   return_value="DELETE FROM t WHERE ..."), \
             patch(f"{BASE_PATH}.delete_processor.DatabaseUtils.records_to_tuples",
                   return_value=[(1, "a"), (2, "b")]):
            processor.delete_batch("t", conditions)

        cursor.executemany.assert_called()

    def test_failed_chunk_rolls_back(self):
        processor, conn, cursor = self._make()
        cursor.execute.side_effect = Exception("db error")
        conditions = [{"id": 1}, {"id": 2}]
        deleted, failed = processor.delete_batch("t", conditions)
        conn.rollback.assert_called()
        assert failed > 0

    def test_commit_called_on_success(self):
        processor, conn, cursor = self._make(rowcount=2)
        conditions = [{"id": 1}, {"id": 2}]
        processor.delete_batch("t", conditions)
        conn.commit.assert_called_once()

    def test_autocommit_restored(self):
        processor, conn, cursor = self._make()
        conn.autocommit = True
        cursor.execute.side_effect = Exception("err")
        conditions = [{"id": 1}]
        processor.delete_batch("t", conditions)
        assert conn.autocommit is True

    def test_stats_updated_after_delete(self):
        processor, _, _ = self._make(rowcount=2)
        conditions = [{"id": 1}, {"id": 2}]
        processor.delete_batch("t", conditions)
        stats = processor.get_stats()
        assert stats["total_records_processed"] == 2
        assert stats["total_records_deleted"] == 2


# ---------------------------------------------------------------------------
# BatchProcessor (coordinator)
# ---------------------------------------------------------------------------

class TestBatchProcessor:

    def _make(self, batch_size=1000):
        cm = MagicMock()
        processor = BatchProcessor(connection_manager=cm, batch_size=batch_size)
        return processor, cm

    def test_delegates_insert_to_insert_processor(self):
        processor, _ = self._make()
        processor.insert_processor.insert_batch = MagicMock(return_value=(5, 0))
        processor.infer_schema = MagicMock(return_value=[])

        result = processor.insert_batch("t", SAMPLE_RECORDS)

        processor.insert_processor.insert_batch.assert_called_once()
        assert result == (5, 0)

    def test_delegates_update_to_update_processor(self):
        processor, _ = self._make()
        processor.update_processor.update_batch = MagicMock(return_value=(3, 0))
        processor.infer_schema = MagicMock(return_value=[])

        result = processor.update_batch("t", SAMPLE_RECORDS, KEY_COLUMNS)

        processor.update_processor.update_batch.assert_called_once()
        assert result == (3, 0)

    def test_delegates_upsert_to_upsert_processor(self):
        processor, _ = self._make()
        processor.upsert_processor.upsert_batch = MagicMock(return_value=(2, 1, 0))
        processor.infer_schema = MagicMock(return_value=[])

        result = processor.upsert_batch("t", SAMPLE_RECORDS, KEY_COLUMNS)

        processor.upsert_processor.upsert_batch.assert_called_once()
        assert result == (2, 1, 0)

    def test_delegates_delete_to_delete_processor(self):
        processor, _ = self._make()
        processor.delete_processor.delete_batch = MagicMock(return_value=(3, 0))
        processor.infer_schema = MagicMock(return_value=[])

        result = processor.delete_batch("t", SAMPLE_RECORDS)

        processor.delete_processor.delete_batch.assert_called_once()
        assert result == (3, 0)

    def test_get_processor_returns_correct_processor(self):
        processor, _ = self._make()
        assert processor.get_processor("insert") is processor.insert_processor
        assert processor.get_processor("update") is processor.update_processor
        assert processor.get_processor("upsert") is processor.upsert_processor
        assert processor.get_processor("delete") is processor.delete_processor

    def test_get_processor_raises_on_unknown_type(self):
        processor, _ = self._make()
        with pytest.raises(ValueError, match="Unknown operation type"):
            processor.get_processor("merge")

    def test_set_batch_size_updates_all_processors(self):
        processor, _ = self._make(batch_size=1000)
        processor.set_batch_size(500)
        assert processor.batch_size == 500
        assert processor.insert_processor.batch_size == 500
        assert processor.update_processor.batch_size == 500
        assert processor.upsert_processor.batch_size == 500
        assert processor.delete_processor.batch_size == 500

    def test_get_stats_aggregates_all_processors(self):
        processor, _ = self._make()
        for p in processor._processors:
            p.get_stats = MagicMock(return_value={
                "total_operations": 1,
                "records_processed": 10,
                "records_inserted": 5,
                "records_updated": 3,
                "records_deleted": 2,
                "records_failed": 0,
            })

        stats = processor.get_stats()
        assert stats["total_operations"] == 4
        assert stats["total_records_processed"] == 40
        assert stats["total_records_inserted"] == 20
        assert stats["total_records_updated"] == 12
        assert stats["total_records_deleted"] == 8
        assert stats["total_records_failed"] == 0

    def test_reset_stats_resets_all_processors(self):
        processor, _ = self._make()
        for p in processor._processors:
            p.reset_stats = MagicMock()

        processor.reset_stats()

        for p in processor._processors:
            p.reset_stats.assert_called_once()

    def test_get_stats_summary_contains_headers(self):
        processor, _ = self._make()
        summary = processor.get_stats_summary()
        assert "Batch Processor Statistics" in summary
        assert "Per-Processor Breakdown" in summary

    def test_get_operation_summary(self):
        processor, _ = self._make()
        for p in processor._processors:
            p.get_stats = MagicMock(return_value={"total_operations": 2})

        summary = processor.get_operation_summary()
        assert summary["insert_operations"] == 2
        assert summary["update_operations"] == 2
        assert summary["upsert_operations"] == 2
        assert summary["delete_operations"] == 2

    def test_normalize_records_passthrough_list(self):
        processor, _ = self._make()
        records = [{"id": 1}]
        assert processor._normalize_records(records) is records

    def test_normalize_records_generator(self):
        processor, _ = self._make()
        records = ({"id": i} for i in range(3))
        result = processor._normalize_records(records)
        assert isinstance(result, list)
        assert len(result) == 3

    def test_normalize_records_unsupported_type_raises(self):
        processor, _ = self._make()
        with pytest.raises(TypeError, match="Unsupported record type"):
            processor._normalize_records(42)
