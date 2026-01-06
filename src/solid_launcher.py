import os
import subprocess
import signal
import sys
from pathlib import Path

# =========================
# Configuration
# =========================
GENERATED_ROOT = "../data/experiment_data"
BASE_PORT = 3001
BASE_URL_HOST = "http://localhost"
SOLID_VERSION = "@solid/community-server@6"
SOLID_CONFIG = "solid-config.json"
LOG_DIR = "./solid-logs"

# =========================
# Helpers
# =========================
def discover_servers(root):
    return sorted(
        d for d in os.listdir(root)
        if os.path.isdir(os.path.join(root, d)) and d.startswith("server")
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

# =========================
# Main Controller
# =========================
def main():
    os.makedirs(LOG_DIR, exist_ok=True)

    servers = discover_servers(GENERATED_ROOT)
    if not servers:
        print("No generated servers found.")
        sys.exit(1)

    processes = []

    print(f"Launching {len(servers)} Solid servers...\n")

    for idx, server_name in enumerate(servers):
        port = BASE_PORT + idx
        root_path = str(Path(GENERATED_ROOT) / server_name)

        cmd, base_url = build_command(port, root_path)

        log_path = f"{LOG_DIR}/{server_name}.log"
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

    signal.pause()

if __name__ == "__main__":
    main()
