"""Exercise the live runtime client and its optional outer archive with an echo worker."""
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SCHEMA = "simulator-joint-recording-selection-v1"


def echo():
    for line in sys.stdin:
        request = json.loads(line)
        command = request.pop("command")
        if "--slow-activation" in sys.argv:
            time.sleep(0.4)
        request["body"] = {"echo": command}
        print(json.dumps(request), flush=True)
        if command == "close":
            break


def check(client, root, name, marker, sidecar, expected_logging, *, ipc_limit=4, budget_policy=None, engine="in-process", scenario="normal"):
    case = root / name
    case.mkdir()
    output = case / "output"
    config = dict(version="joint-runtime-v1", run_id="test", session_id="test",
                  worker_argv=[sys.executable, str(Path(__file__).resolve())], output=str(output),
                  limits=dict(frame_bytes="4096", host_timeout_ms="5000", ipc_calls=str(ipc_limit),
                              transcript_bytes="4096" if expected_logging else "1"))
    if budget_policy is not None:
        config["host_budget_policy"] = budget_policy
        config["memory_engine"] = engine
    if marker:
        config["recording"] = {"policy_schema": SCHEMA}
    if sidecar is not None:
        (case / "runtime-recording.json").write_text(sidecar)
    path = case / "runtime-config.json"
    path.write_text(json.dumps(config))
    result = subprocess.run([client, str(path), scenario], cwd=case, capture_output=True, timeout=15)
    if expected_logging is None:
        assert result.returncode and not output.exists(), (name, result.stderr)
        return
    assert result.returncode == 0, (name, result.stderr)
    if scenario == "legacy-ipc-limit":
        return
    log = output / "runtime.jsonl"
    assert log.exists() == expected_logging, name
    if expected_logging:
        rows = [json.loads(line) for line in log.read_text().splitlines()]
        assert [(r["command"]["command"], r["reply"]["body"]["echo"]) for r in rows] == [
            ("open", "open"), ("close", "close")]


def check_activation_time(client, root, *, supervised, remaining_ms, expect_ready):
    case = root / f"activation-{supervised}-{remaining_ms}"
    case.mkdir()
    config = dict(version="joint-runtime-v1", run_id="r", session_id="s",
                  output=str(case / "output"), activation={},
                  worker_argv=[sys.executable, str(Path(__file__).resolve()), "--slow-activation"],
                  limits=dict(frame_bytes="4096", host_timeout_ms="150", ipc_calls="5"))
    if supervised:
        config.update(host_budget_policy="cgroup-v2-whole-job-v1", memory_engine="in-process")
    path = case / "config.json"
    path.write_text(json.dumps(config))
    env = dict(os.environ, CX_EXPERIMENT_DEADLINE_NS=str(time.monotonic_ns() + remaining_ms * 1_000_000))
    # The client's cancellation FD must stay connected to a live host during the test.
    with subprocess.Popen([client, str(path), "supervised-activation" if expect_ready else "limited-activation"],
                          cwd=case, env=env, stdin=subprocess.PIPE) as process:
        assert process.wait(timeout=8) == 0


def run(client):
    def policy(positions):
        return json.dumps({"schema": SCHEMA, "positions": positions})
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        check_activation_time(client, root, supervised=False, remaining_ms=3000, expect_ready=False)
        if os.getenv("CX_EXPERIMENT_MEMORY_SCOPE"):
            check_activation_time(client, root, supervised=True, remaining_ms=3000, expect_ready=True)
            check_activation_time(client, root, supervised=True, remaining_ms=100, expect_ready=False)
        check(client, root, "legacy-ipc", False, None, True, ipc_limit=1, scenario="legacy-ipc-limit")
        check(client, root, "unknown-budget", False, None, None, budget_policy="other")
        check(client, root, "split-budget", False, None, None,
              budget_policy="cgroup-v2-whole-job-v1", engine="split")
        check(client, root, "whole-job-ipc", False, None,
              True if os.getenv("CX_EXPERIMENT_MEMORY_SCOPE") else None,
              ipc_limit=1, budget_policy="cgroup-v2-whole-job-v1")
        check(client, root, "legacy", False, "invalid and deliberately ignored", True)
        check(client, root, "on", True, policy(["outer", "akita"]), True)
        check(client, root, "off", True, policy([]), False)
        check(client, root, "inner", True, policy(["akita", "order"]), False)
        for i, invalid in enumerate((None, "not json", policy(["unknown"]), policy(["cx", "outer"]),
                                     policy(["outer", "outer"]), policy("off"), " " * 4097)):
            check(client, root, f"invalid-{i}", True, invalid, None)


if __name__ == "__main__":
    if "--config" in sys.argv[1:]:
        echo()
    else:
        run(sys.argv[1])
