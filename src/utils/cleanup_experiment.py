import shutil
from pathlib import Path
import json

def cleanup_experiment():
    """Clean up the experiment directory"""
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    base_dir = Path(config["base_directory"])
    
    if base_dir.exists():
        shutil.rmtree(base_dir)
        print(f"Removed experiment directory: {base_dir}")
    else:
        print(f"Experiment directory does not exist: {base_dir}")

if __name__ == "__main__":
    cleanup_experiment()