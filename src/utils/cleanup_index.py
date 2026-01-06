import shutil
from pathlib import Path
import json

def cleanup_index():
    """Clean up the experiment index directory"""
    # Load configuration
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # Get the index directory path from the config
    index_dir = Path(config.get("index_directory"))
    
    # Remove the directory if it exists
    if index_dir.exists() and index_dir.is_dir():
        shutil.rmtree(index_dir)
        print(f"Removed index directory: {index_dir}")
    else:
        print(f"Index directory does not exist: {index_dir}")

if __name__ == "__main__":
    cleanup_index()
