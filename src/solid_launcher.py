import os
import subprocess
import signal
import sys
import socket
from pathlib import Path
import time

# =========================
# Configuration
# =========================
PROJECT_ROOT = Path(__file__).resolve().parent.parent

GENERATED_ROOT = PROJECT_ROOT / "data" / "experiment_data"
BASE_PORT = 3001
BASE_URL_HOST = "http://localhost"
SOLID_VERSION = "@solid/community-server@6"
SOLID_CONFIG = "solid-config.json"
LOG_DIR = PROJECT_ROOT / "solid-logs"

# =========================
# Helpers
# =========================
def discover_servers(root):
    def server_number(name):
        return int(name.replace("server", ""))
    
    return sorted(
        (d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d)) and d.startswith("server")),
        key=server_number
    )


def build_command(port, root_path):
    base_url = f"{BASE_URL_HOST}:{port}"
    cmd = [
        "npx", "--yes", SOLID_VERSION,
        "--baseUrl", base_url,
        "--port", str(port),
        "--rootFilePath", root_path,
        "-c", SOLID_CONFIG
    ]
    return cmd, base_url

def is_port_free(port):
    """Check if a TCP port is available on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("localhost", port))
            return True
        except OSError:
            return False

def kill_process_on_port(port):
    """Kill any process using the given port (Linux/macOS)."""
    try:
        # Use lsof to find PID using the port
        result = subprocess.run(
            ["lsof", "-ti", f"tcp:{port}"], capture_output=True, text=True
        )
        pids = result.stdout.strip().splitlines()
        for pid in pids:
            if pid:
                print(f"Killing process {pid} using port {port}")
                os.kill(int(pid), signal.SIGTERM)
        if not pids:
            print(f"No process found on port {port}")
    except FileNotFoundError:
        # lsof not available (Windows)
        print(f"Warning: Cannot check port {port} automatically on this OS. Ensure it is free.")

# =========================
# Main Controller
# =========================
def main():
    LOG_DIR.mkdir(exist_ok=True)

    servers = discover_servers(GENERATED_ROOT)
    if not servers:
        print("No generated servers found.")
        sys.exit(1)

    total_servers = len(servers)
    print(f"Preparing to launch {total_servers} Solid servers...")

    # =========================
    # Kill any existing Solid servers on the ports we need
    # =========================
    print("Checking for running servers on required ports...")
    for idx in range(total_servers):
        port = BASE_PORT + idx
        if not is_port_free(port):
            print(f"Port {port} is in use. Attempting to free it...")
            kill_process_on_port(port)
            # Give time for OS to release port
            time.sleep(0.5)
            if not is_port_free(port):
                print(f"Error: Could not free port {port}. Exiting.")
                sys.exit(1)

    print("All required ports are free.\n")

    processes = []

    # =========================
    # Launch servers
    # =========================
    for idx, server_name in enumerate(servers):
        port = BASE_PORT + idx
        root_path = str(Path(GENERATED_ROOT) / server_name)

        cmd, base_url = build_command(port, root_path)

        log_path = LOG_DIR / f"{server_name}.log"
        log_file = open(log_path, "w")

        print(f"[+] {server_name}")
        print(f"    Port:     {port}")
        print(f"    Base URL: {base_url}")
        print(f"    Root:     {root_path}")
        print(f"    Log:      {log_path}\n")

        proc = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT
        )

        processes.append((server_name, proc, log_file))

    # =========================
    # Graceful shutdown
    # =========================
    def shutdown(signum, frame):
        print("\nShutting down Solid servers...")
        for srv, proc, log in processes:
            print(f"Stopping {srv}")
            proc.terminate()
            log.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("All Solid servers launched. Press Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown(None, None)

if __name__ == "__main__":
    main()
