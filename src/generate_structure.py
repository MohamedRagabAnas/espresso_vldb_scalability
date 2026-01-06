import os
import random
import json
import shutil
import math
import csv
from pathlib import Path
from webid_generator import WebIDGenerator
from distributions import DistributionStrategies
# from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_config():
    with open(PROJECT_ROOT / "src" / "config" / "config.json", "r") as f:
        return json.load(f)

def imagineaclspecial(files, social_agents, file_label='medical'):
    access_control = {}
    
    print(f"Creating special ACL for {len(files)} {file_label} files with {len(social_agents)} special agents")
    
    # Initialize access control with empty lists for all destination files
    for source_path, dest_path in files:
        access_control[str(dest_path)] = []
    
    # Process each social agent based on their power level
    for agent in social_agents:
        try:
            power = int(agent['power'])
        except (ValueError, TypeError):
            power = 0  
        
        webid = agent['webid']
        
        # Calculate how many files this agent can access based on power percentage
        num_files_to_access = math.floor(len(files) * (power / 100))
        
        print(f"  Social agent {webid} (power={power}%): can access {num_files_to_access} files")
        
        if num_files_to_access > 0:
            # Select random files for this agent
            selected_files = random.sample(files, num_files_to_access)
            
            # Grant access to selected files
            for source_path, dest_path in selected_files:
                access_control[str(dest_path)].append(webid)
    
    return access_control

def create_webid_structure(access_control):
    """
    Create the WebID-based dictionary structure
    
    Args:
        access_control: Dictionary of {file_path: [webids]}
        
    Returns:
        Dictionary in format: {"WebID": {"Server": {"Pod": ["file1", "file2", ...]}}}
    """
    webid_structure = {}
    
    for file_path, webids in access_control.items():
        # Parse the file path to extract server, pod, and filename
        path_parts = Path(file_path).parts
        if len(path_parts) >= 3:
            server_name = path_parts[-3]  # e.g., "server1"
            pod_name = path_parts[-2]     # e.g., "pod1"
            filename = path_parts[-1]     # e.g., "original-filename.txt"
            
            for webid in webids:
                if webid not in webid_structure:
                    webid_structure[webid] = {}
                
                if server_name not in webid_structure[webid]:
                    webid_structure[webid][server_name] = {}
                
                if pod_name not in webid_structure[webid][server_name]:
                    webid_structure[webid][server_name][pod_name] = []
                
                webid_structure[webid][server_name][pod_name].append(filename)
    
    return webid_structure

# def create_server_metaindex(base_dir, server_id, pod_count):
#     """
#     Create a metaindex.csv file for a server listing all pod index directories
#     placed within an ESPRESSO directory
    
#     Args:
#         base_dir: Base directory path
#         server_id: Server ID number
#         pod_count: Number of pods in this server
#     """
#     server_dir = base_dir / f"server{server_id}"
    
#     # Create ESPRESSO directory
#     espresso_dir = server_dir / "ESPRESSO"
#     espresso_dir.mkdir(exist_ok=True)
    
#     metaindex_path = espresso_dir / "metaindex.csv"
    
#     with open(metaindex_path, 'w', newline='') as csvfile:
#         writer = csv.writer(csvfile)
#         # writer.writerow(['pod_id', 'index_directory_path'])
        
#         for pod_id in range(1, pod_count + 1):
#             pod_index_dir = server_dir / f"pod{pod_id}" / "espressoindex"
#             writer.writerow([str(pod_index_dir.resolve())])
    
#     return metaindex_path


def create_server_metaindex(base_dir, server_id, pod_count,config):
    """
    Create a metaindex.csv file for a server listing all pod espressoindex URLs
    exposed by that server.

    Args:
        base_dir: Base directory path
        server_id: Server ID number (1-based: server1, server2, ...)
        pod_count: Number of pods in this server
    """
    server_dir = base_dir / f"server{server_id}"

    # Create ESPRESSO directory
    espresso_dir = server_dir / "ESPRESSO"
    espresso_dir.mkdir(exist_ok=True)

    metaindex_path = espresso_dir / "metaindex.csv"

    # server1 -> 3001, server2 -> 3002, ...
    port = 3000 + server_id
    # base_url = f"http://localhost:{port}"
    base_url = f"http://{config['server_host']}:{port}"

    
    with open(metaindex_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)

        for pod_id in range(1, pod_count + 1):
            pod_url = f"{base_url}/pod{pod_id}/espressoindex/"
            writer.writerow([pod_url])

    return metaindex_path

def create_ltoverlay_servers_csv(base_dir, num_servers, starting_port=3001):
    """
    Create a CSV file 'LTOVERLAYSERVERS.csv' mapping server names to localhost ports.

    Args:
        base_dir: Base directory to save the CSV.
        num_servers: Number of servers to include.
        starting_port: Port number of server1 (default 3001). Subsequent servers increment by 1.
    """
    csv_path = base_dir / "LTOVERLAYSERVERS.csv"
    
    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for server_id in range(1, num_servers + 1):
            server_name = f"server{server_id}"
            server_url = f"localhost:{starting_port + server_id - 1}"
            writer.writerow([server_name, server_url])
    
    print(f"Created LTOVERLAYSERVERS.csv with {num_servers} servers at {csv_path}")
    return csv_path


def distribute_files(config):
    """Distribute files from source directory to pods based on configuration"""
    # base_dir = Path(config["base_directory"])
    base_dir = PROJECT_ROOT / config["base_directory"]
    

    # source_dir = Path(config["source_directory"])
    source_dir = PROJECT_ROOT / config["source_directory"]
    

    
    # # If source_dir is relative, make it relative to the current working directory
    # if not source_dir.is_absolute():
    #     source_dir = Path.cwd() / source_dir
    
    base_dir.mkdir(exist_ok=True, parents=True)
    
    # Get all source files
    all_source_files = list(source_dir.glob("*.txt"))
    if not all_source_files:
        raise ValueError(f"No .txt files found in source directory: {source_dir}")
    
    # Calculate how many files we actually need
    total_files_needed = config["num_servers"] * config["pods_per_server"] * config["files_per_pod"]
    
    # Check if we have enough source files
    if len(all_source_files) < total_files_needed:
        raise ValueError(
            f"Not enough source files. Need {total_files_needed}, "
            f"but only {len(all_source_files)} available in {source_dir}."
        )
    
    # Select only the number of files we need
    source_files = random.sample(all_source_files, total_files_needed)
    print(f"Using {len(source_files)} out of {len(all_source_files)} available source files")
    
    # Generate WebIDs
    webid_generator = WebIDGenerator()
    web_ids = webid_generator.get_webids(config["number_of_webids"])
    
    # Generate social agents with power levels
    power_percentages = config.get("social_agent_powers", [100, 75, 50, 25])
    social_agents = webid_generator.init_sanode_list(power_percentages)
    
    # Display social agent information
    print("Social agents with power levels:")
    for agent in social_agents:
        print(f"  {agent['webid']}: power={agent['power']}%")
    
    # Choose pod distribution strategy
    pod_strategy = config.get("pod_distribution_strategy", "uniform")
    file_strategy = config.get("file_distribution_strategy", "uniform")
    
    print(f"Using {pod_strategy} distribution for pods and {file_strategy} distribution for files")
    
    # Distribute pods to servers
    pod_counts = DistributionStrategies.distribute_pods_to_servers(
        config["num_servers"],
        config["pods_per_server"],
        strategy=pod_strategy,
        alpha=config.get("pareto_alpha", 1.5),
        min_pods_per_server=config.get("min_pods_per_server", 1)
    )
    
    # Distribute files to pods
    distribution = DistributionStrategies.distribute_files_to_pods(
        source_files,
        pod_counts,
        strategy=file_strategy,
        alpha=config.get("pareto_alpha", 1.5),
        min_files_per_pod=config.get("min_files_per_pod", 1)
    )
    
    # Get distribution statistics
    distribution_stats = DistributionStrategies.get_distribution_stats(distribution)
    
    # Create list of (source_path, dest_path) tuples for all files
    file_mappings = []
    for server_id, pod_count in enumerate(pod_counts, 1):
        for pod_id in range(1, pod_count + 1):
            pod_files = distribution.get((server_id, pod_id), [])
            for file_num, source_file in enumerate(pod_files, 1):
                # KEEP ORIGINAL FILENAME
                dest_filename = source_file.name
                dest_path = base_dir / f"server{server_id}" / f"pod{pod_id}" / dest_filename
                file_mappings.append((source_file, dest_path))
    
    # Create special access control based on social agent power levels
    access_control = imagineaclspecial(file_mappings, social_agents, 'medical-record')
    
    # Also add random access for regular agents
    for source_path, dest_path in file_mappings:
        dest_str = str(dest_path)
        if dest_str not in access_control:
            access_control[dest_str] = []
        
        # Add random regular agents to each file
        num_regular_agents = random.randint(1, len(web_ids))
        regular_access = random.sample(web_ids, num_regular_agents)
        access_control[dest_str].extend(regular_access)
    
    # Create the WebID-based structure
    webid_structure = create_webid_structure(access_control)
    
    files_distributed = 0
    pods_created = 0
    server_metaindexes = {}  

    # Now copy files and create directory structure
    for server_id, pod_count in enumerate(pod_counts, 1):
        server_dir = base_dir / f"server{server_id}"
        server_dir.mkdir(exist_ok=True)
        
        for pod_id in range(1, pod_count + 1):
            pod_dir = server_dir / f"pod{pod_id}"
            pod_dir.mkdir(exist_ok=True)
            pods_created += 1
            
            # Create pod index directory
            index_dir = pod_dir / "espressoindex"
            index_dir.mkdir(exist_ok=True)
            
            # Get files for this pod from the distribution
            pod_files = distribution.get((server_id, pod_id), [])
            
            for file_num, source_file in enumerate(pod_files, 1):
                # KEEP ORIGINAL FILENAME
                dest_filename = source_file.name
                dest_path = pod_dir / dest_filename
                
                # Copy the file
                shutil.copy2(source_file, dest_path)
                files_distributed += 1
        
        # Create server-level metaindex.csv after creating all pods
        metaindex_path = create_server_metaindex(base_dir, server_id, pod_count,config=config)
        server_metaindexes[server_id] = str(metaindex_path)
    
    # Save access control data
    with open(base_dir / "access_control.json", 'w') as f:
        json.dump(access_control, f, indent=2)
    
    # Save WebID-based structure
    with open(base_dir / "webid_structure.json", 'w') as f:
        json.dump(webid_structure, f, indent=2)
    
    # Save WebID information
    webid_info = {
        "web_ids": web_ids,
        "agents": webid_generator.init_anode_list(config["number_of_webids"]),
        "social_agents": social_agents,
        "social_agent_powers": power_percentages
    }
    with open(base_dir / "webid_info.json", 'w') as f:
        json.dump(webid_info, f, indent=2)
    
    # Save distribution information
    distribution_info = {
        "pod_strategy": pod_strategy,
        "file_strategy": file_strategy,
        "stats": distribution_stats,
        "total_files_needed": total_files_needed,
        "total_files_available": len(all_source_files),
        "pareto_alpha": config.get("pareto_alpha", 1.5) if pod_strategy == "pareto" or file_strategy == "pareto" else None,
        "zipf_alpha": config.get("zipf_alpha", 1.2) if pod_strategy == "zipf" or file_strategy == "zipf" else None
    }
    with open(base_dir / "distribution_info.json", 'w') as f:
        json.dump(distribution_info, f, indent=2)
    
    # Save metaindex information
    with open(base_dir / "server_metaindexes.json", 'w') as f:
        json.dump(server_metaindexes, f, indent=2)
    
    # Create LTO overlay servers CSV
    create_ltoverlay_servers_csv(base_dir, config["num_servers"])

    print(f"Created experiment structure with {config['num_servers']} servers, "
          f"{pods_created} total pods using {pod_strategy} pod distribution and {file_strategy} file distribution.")
    print(f"Distribution statistics: {distribution_stats}")
    print(f"Distributed {files_distributed} files (needed {total_files_needed}, had {len(all_source_files)} available)")
    print(f"Generated {len(web_ids)} regular WebIDs and {len(social_agents)} social WebIDs with power levels")
    print(f"Created server metaindex files: {len(server_metaindexes)} servers")
    
    return access_control, webid_structure

if __name__ == "__main__":
    config = load_config()
    distribute_files(config)