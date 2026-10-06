# SQL Server raw-load throughput spike (2026-10-05)

Goal: choose how Task 8 bulk-loads raw NDJSON lines into a `varchar(max)` UTF-8 column
(`Latin1_General_100_CI_AS_SC_UTF8`, `DATA_COMPRESSION = PAGE`) on SQL Server.

## Environment
- Host: Windows Server 2019 (the same box running the spike)
- Server: `cssnpi`, SQL Server 2019 CU32 Developer; scratch database `npd_test`
- Driver: ODBC Driver 18 for SQL Server (DataEngine's choice); Python 3.12, Python-DataEngine 2.4.0, pyodbc
- Data: 200,000 synthetic rows (the `ORG1` fixture, about 1.3 KB of JSON each, unique id per row); no real .ndjson used
- Batches of 5,000 rows, one commit per batch; script was not committed (throwaway)

## Measurements (200,000 rows)
| Variant | Time | Rows/s |
|---|---|---|
| `fast_executemany` | 16.7 s | 11,954 |
| `fast_executemany` + `setinputsizes` (SQL_WVARCHAR, 0, 0 on the resource column) | 20.1 s | 9,963 |
| `bcp` (tab-separated UTF-8 file, `-C 65001 -b 5000`; file write excluded) | 5.5 s | 36,197 |

No variant raised an error; the bcp invocation ran as written (no adjustments needed).
All `spike_*` tables were dropped by the script; none remain in `npd_test`.

## Rule applied
Use the fastest `fast_executemany` variant if it reaches at least 5,000 rows/s (the Postgres full run
averaged about 2,300 raw rows/s); otherwise use `bcp`. The fastest `fast_executemany` variant
(plain, 11,954 rows/s) clears the threshold, so bcp is not needed even though it was about 3x faster.

Decision: fast_executemany
