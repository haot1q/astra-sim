"""Exercise the live runtime client and its optional outer archive with an echo worker."""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

SCHEMA = "simulator-joint-recording-selection-v1"


def echo():
    for line in sys.stdin:
        request = json.loads(line)
        command = request.pop("command")
        request["body"] = {"echo": command}
        print(json.dumps(request), flush=True)
        if command == "close":
            break


def check(client, root, name, marker, sidecar, expected_logging):
    case = root / name
    case.mkdir()
    output = case / "output"
    config = dict(version="joint-runtime-v1", run_id="test", session_id="test",
                  worker_argv=[sys.executable, str(Path(__file__).resolve())], output=str(output),
                  limits=dict(frame_bytes="4096", host_timeout_ms="5000", ipc_calls="4",
                              transcript_bytes="4096" if expected_logging else "1"))
    if marker:
        config["recording"] = {"policy_schema": SCHEMA}
    if sidecar is not None:
        (case / "runtime-recording.json").write_text(sidecar)
    path = case / "runtime-config.json"
    path.write_text(json.dumps(config))
    result = subprocess.run([client, str(path), "normal"], cwd=case, capture_output=True, timeout=15)
    if expected_logging is None:
        assert result.returncode and not output.exists(), (name, result.stderr)
        return
    assert result.returncode == 0, (name, result.stderr)
    log = output / "runtime.jsonl"
    assert log.exists() == expected_logging, name
    if expected_logging:
        rows = [json.loads(line) for line in log.read_text().splitlines()]
        assert [(r["command"]["command"], r["reply"]["body"]["echo"]) for r in rows] == [
            ("open", "open"), ("close", "close")]


def run(client):
    def policy(positions):
        return json.dumps({"schema": SCHEMA, "positions": positions})
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        check(client, root, "legacy", False, "invalid and deliberately ignored", True)
        check(client, root, "on", True, policy(["outer", "akita"]), True)
        check(client, root, "off", True, policy([]), False)
        check(client, root, "inner", True, policy(["akita", "order"]), False)
        for i, invalid in enumerate((None, "not json", policy(["unknown"]), policy(["cx", "outer"]),
                                     policy(["outer", "outer"]), policy("off"), " " * 4097)):
            check(client, root, f"invalid-{i}", True, invalid, None)


if __name__ == "__main__":
    if sys.argv[1] == "--config":
        echo()
    else:
        run(sys.argv[1])
