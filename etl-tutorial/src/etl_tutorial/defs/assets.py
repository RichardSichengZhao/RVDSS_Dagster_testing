import dagster as dg
import duckdb
import filelock


def serialize_duckdb_query(duckdb_path: str, sql: str):
    """Execute SQL statement with file lock to guarantee cross-process concurrency."""
    lock_path = f"{duckdb_path}.lock"
    with filelock.FileLock(lock_path):
        conn = duckdb.connect(duckdb_path)
        try:
            return conn.execute(sql)
        finally:
            conn.close()


@dg.asset
def assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
