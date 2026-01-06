import json
import time
import os
from pathlib import Path
from generate_structure import distribute_files
# from indexer import index_all_pods  # Commented out indexing

def get_config_path() -> Path:
    """Return the absolute path to config.json located in src/config/."""
    config_path = Path(__file__).parent / "config" / "config.json"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")
    
    return config_path.resolve()

def run_experiment():
    """Run the complete scalability experiment"""
    config_path = get_config_path()
    print(f"Looking for config file at: {config_path}")
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Error: Config file not found at {config_path}")
        print("Current working directory:", os.getcwd())
        print("Please make sure config.json exists in the correct location.")
        return
    
    print("Starting scalability experiment...")
    print(f"Configuration: {config}")
    
    source_dir = Path(config["source_directory"])
    if not source_dir.exists():
        config_dir = config_path.parent
        source_dir = config_dir / config["source_directory"]
        if not source_dir.exists():
            raise ValueError(f"Source directory does not exist: {source_dir}")
    
    source_files = list(source_dir.glob("*.txt"))
    if not source_files:
        raise ValueError(f"No .txt files found in source directory: {source_dir}")
    
    # Step 1: Generate the file structure by distributing source files
    pod_strategy = config.get("pod_distribution_strategy", "uniform")
    file_strategy = config.get("file_distribution_strategy", "uniform")
    
    print(f"\n1. Distributing files from source directory using {pod_strategy} pod distribution and {file_strategy} file distribution...")
    start_time = time.time()
    
    try:
        access_control, webid_structure = distribute_files(config)
    except ValueError as e:
        if "Not enough source files" in str(e):
            print(f"Error: {e}")
            print("Please either:")
            print("  1. Add more files to the source directory")
            print("  2. Reduce the number of servers, pods, or files per pod")
            print("  3. Use a less skewed distribution strategy")
            return
        else:
            raise e
    
    structure_time = time.time() - start_time
    print(f"File distribution completed in {structure_time:.2f} seconds")
    
    base_dir = Path(config["base_directory"])
    distribution_info_path = base_dir / "distribution_info.json"
    if distribution_info_path.exists():
        with open(distribution_info_path, 'r') as f:
            distribution_info = json.load(f)
        print(f"Distribution stats: {distribution_info['stats']}")
    
    # Step 2: Index all pods (commented out)
    print("\n2. Indexing all pods... (skipped)")
    indexing_time = 0
    success_count = 0
    total_count = 0
    # start_time = time.time()
    # success_count, total_count = index_all_pods(config, access_control)
    # indexing_time = time.time() - start_time
    
    # Report results
    print("\n3. Experiment Results:")
    print(f"   - File distribution time: {structure_time:.2f} seconds")
    print(f"   - Indexing time: {indexing_time:.2f} seconds (skipped)")
    print(f"   - Total time: {structure_time + indexing_time:.2f} seconds")
    print(f"   - Successful pods: {success_count}/{total_count}")
    print(f"   - Success rate: 0.00%")
    
    results = {
        "config": config,
        "file_distribution_time": structure_time,
        "indexing_time": indexing_time,
        "total_time": structure_time + indexing_time,
        "successful_pods": success_count,
        "total_pods": total_count,
        "success_rate": 0
    }
    
    if distribution_info_path.exists():
        with open(distribution_info_path, 'r') as f:
            distribution_info = json.load(f)
        results["distribution_stats"] = distribution_info["stats"]
        results["pod_distribution_strategy"] = distribution_info["pod_strategy"]
        results["file_distribution_strategy"] = distribution_info["file_strategy"]
    
    results_path = base_dir / "experiment_results.json"
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to {results_path}")
    print(f"WebID structure saved to {base_dir / 'webid_structure.json'}")

if __name__ == "__main__":
    run_experiment()
