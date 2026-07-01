"""
Engine runners: each wraps the external command needed to execute a
query with a given engine, and returns (wall_seconds, stdout).

Timing is wall-clock from process invocation to exit - this deliberately
includes JVM/cluster startup overhead, since that overhead is real cost
a user pays every time they run a job, not an artifact to hide.
"""

import subprocess
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _run(cmd, cwd=None):
    start = time.perf_counter()
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    elapsed = time.perf_counter() - start
    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr[-4000:]}"
        )
    return elapsed, proc.stdout


def run_pandas(query: str, local_input: str, local_zones: str = None) -> float:
    script = PROJECT_ROOT / "queries" / "baseline_pandas" / f"{query}.py"
    cmd = ["python", str(script), "--input", local_input]
    if local_zones:
        cmd += ["--zones", local_zones]
    elapsed, _ = _run(cmd)
    return elapsed


def run_spark(query: str, hdfs_input: str, hdfs_zones: str = None,
              driver_memory="512m", executor_memory="512m") -> float:
    script_in_container = f"/workspace/queries/spark/{query}.py"
    cmd = [
        "docker", "exec", "bench-spark-master",
        "/opt/bitnami/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--driver-memory", driver_memory,
        "--executor-memory", executor_memory,
        script_in_container, "--input", hdfs_input,
    ]
    if hdfs_zones:
        cmd += ["--zones", hdfs_zones]
    elapsed, _ = _run(cmd)
    return elapsed


def run_hadoop_mapreduce(query: str, hdfs_input: str, hdfs_output: str,
                          local_zones: str = None) -> float:
    mapper = f"/workspace/queries/hadoop_mapreduce/{query}/mapper.py"
    reducer = f"/workspace/queries/hadoop_mapreduce/{query}/reducer.py"
    streaming_jar = "/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"

    _run(["docker", "exec", "bench-resourcemanager", "hdfs", "dfs", "-rm", "-r", "-f", hdfs_output])

    files_arg = f"{mapper},{reducer}"
    if local_zones:
        files_arg += f",{local_zones}#zones.csv"

    cmd = [
        "docker", "exec", "bench-resourcemanager",
        "hadoop", "jar", streaming_jar,
        "-files", files_arg,
        "-mapper", "python3 mapper.py",
        "-reducer", "python3 reducer.py",
        "-input", hdfs_input,
        "-output", hdfs_output,
    ]
    elapsed, _ = _run(cmd)
    return elapsed
