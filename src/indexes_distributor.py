import json
import shutil
import csv
import random
from pathlib import Path



def normalize_index_filename(filename: str) -> str:
    """
    Normalize index filenames by removing '_' and '.' from the stem,
    while preserving the file extension.

    Example:
    http___example.org_agent1_profile_card_me-pods.zip
    -> httpexampleorgagent1profilecardme-pods.zip
    """
    p = Path(filename)
    normalized_stem = (
        p.stem
        .replace("_", "")
        .replace(".", "")
    )
    return normalized_stem + p.suffix

def distribute_indexes(experiment_base_dir, index_output_dir):
    """
    Move indexes from WebID_indexer output to their appropriate locations
    and create overlaynetwork.csv for server-level index distribution
    
    Args:
        experiment_base_dir: Base directory of the experiment (where servers are located)
        index_output_dir: Directory where WebID_indexer generated the indexes
    """
    experiment_base = Path(experiment_base_dir)
    index_output = Path(index_output_dir)
    
    # Load webid info to get all agent webids
    webid_info_path = experiment_base / "webid_info.json"
    with open(webid_info_path, 'r') as f:
        webid_info = json.load(f)
    
    all_webids = webid_info["web_ids"] + [agent["webid"] for agent in webid_info["social_agents"]]
    
    overlay_entries = []
    moved_files = 0
    
    print("Starting index distribution...")
    print(f"Looking for indexes in: {index_output}")
    
    # Check if the index output directory exists and has the expected structure
    if not index_output.exists():
        print(f"Error: Index output directory {index_output} does not exist!")
        return
    
    # Process the directory structure as shown in your tree output
    http_dir = index_output / "http:"
    if not http_dir.exists():
        print(f"Error: Expected directory structure not found. Looking for 'http:' in {index_output}")
        return
    
    example_org_dir = http_dir / "example.org"
    if not example_org_dir.exists():
        print(f"Error: Expected 'example.org' directory not found in {http_dir}")
        return
    
    print(f"Found index structure at: {example_org_dir}")
    
    # Process each agent directory
    for agent_dir in example_org_dir.iterdir():
        if agent_dir.is_dir():
            print(f"Processing agent directory: {agent_dir.name}")
            
            # Navigate through profile/card#me structure
            profile_dir = agent_dir / "profile"
            if profile_dir.exists():
                card_dir = profile_dir / "card#me"
                if card_dir.exists():
                    # Extract webid from directory structure
                    agent_name = agent_dir.name
                    webid = f"http://example.org/{agent_name}/profile/card#me"
                    
                    print(f"Processing agent: {webid}")
                    
                    # Process file-level indexes (move to pod espressoindex directories)
                    file_level_dir = card_dir / "file-level"
                    if file_level_dir.exists():
                        for server_dir in file_level_dir.iterdir():
                            if server_dir.is_dir() and server_dir.name.startswith("server"):
                                server_id = server_dir.name.replace("server", "")
                                for pod_dir in server_dir.iterdir():
                                    if pod_dir.is_dir() and pod_dir.name.startswith("pod"):
                                        pod_id = pod_dir.name.replace("pod", "")
                                        for index_file in pod_dir.glob("*.zip"):
                                            # Destination: serverX/podY/espressoindex/
                                            dest_dir = experiment_base / f"server{server_id}" / f"pod{pod_id}" / "espressoindex"
                                            dest_dir.mkdir(exist_ok=True, parents=True)

                                            
                                            normalized_name = normalize_index_filename(index_file.name)
                                            dest_path = dest_dir / normalized_name
                                            shutil.move(str(index_file), str(dest_path))

                                            # shutil.move(str(index_file), str(dest_dir / index_file.name))
                                            moved_files += 1
                                            # print(f"Moved file-level index: {index_file.name} -> {dest_dir}")
                    
                    # Process pod-level indexes (move to server ESPRESSO directories)
                    pod_level_dir = card_dir / "pod-level"
                    if pod_level_dir.exists():
                        for server_dir in pod_level_dir.iterdir():
                            if server_dir.is_dir() and server_dir.name.startswith("server"):
                                server_id = server_dir.name.replace("server", "")
                                for index_file in server_dir.glob("*.zip"):
                                    # Destination: serverX/ESPRESSO/
                                    dest_dir = experiment_base / f"server{server_id}" / "ESPRESSO"
                                    dest_dir.mkdir(exist_ok=True, parents=True)
                                    
                                    normalized_name = normalize_index_filename(index_file.name)
                                    dest_path = dest_dir / normalized_name
                                    shutil.move(str(index_file), str(dest_path))
                                    
                                    # shutil.move(str(index_file), str(dest_dir / index_file.name))

                                    moved_files += 1
                                    # print(f"Moved pod-level index: {index_file.name} -> {dest_dir}")
                    
                    # Process server-level indexes (collect for random distribution)
                    server_level_dir = card_dir / "server-level"
                    if server_level_dir.exists():
                        for server_dir in server_level_dir.iterdir():
                            if server_dir.is_dir() and server_dir.name.startswith("server"):
                                server_id = server_dir.name.replace("server", "")
                                for index_file in server_dir.glob("*.zip"):
                                    # Store for random distribution later
                                    overlay_entries.append({
                                        'webid': webid,
                                        'index_file': index_file,
                                        'original_server': server_id
                                    })
                else:
                    print(f"Warning: card#me directory not found in {profile_dir}")
            else:
                print(f"Warning: profile directory not found in {agent_dir}")
    
    print(f"Processed agents, moved {moved_files} indexes")
    
    # Randomly distribute server-level indexes and create overlaynetwork.csv
    if overlay_entries:
        print("Distributing server-level indexes randomly...")
        
        # Get all available servers
        servers = [d for d in experiment_base.iterdir() if d.is_dir() and d.name.startswith("server")]
        server_ids = [d.name.replace("server", "") for d in servers]
        
        if not server_ids:
            print("Warning: No servers found for server-level index distribution")
            return
        
        overlay_csv_path = experiment_base / "overlaynetwork.csv"
        
        with open(overlay_csv_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # writer.writerow(['webid', 'serverID', 'destination_path'])
            
            for entry in overlay_entries:
                # Randomly select a destination server (could be different from original)
                dest_server_id = random.choice(server_ids)
                dest_dir = experiment_base / f"server{dest_server_id}" / "ESPRESSO"
                dest_dir.mkdir(exist_ok=True, parents=True)
                
                # Move the index file
                index_file = entry['index_file']
                dest_path = dest_dir / index_file.name

                normalized_name = normalize_index_filename(index_file.name)
                dest_path = dest_dir / normalized_name
                shutil.move(str(index_file), str(dest_path))
                # shutil.move(str(index_file), str(dest_path))
                
                # Add to overlay CSV
                # writer.writerow([entry['webid'], str(dest_path.resolve())])
                host_port = f"localhost:{3000 + int(dest_server_id)}"

                normalized_webid = normalize_webid(entry['webid'])
                writer.writerow([
                    normalized_webid,
                    host_port,
                    str(dest_path.resolve())
                ])

                # print(f"Distributed server-level index: {index_file.name} -> {dest_dir}")
        
        print(f"Created overlay network file: {overlay_csv_path}")
        print(f"Randomly distributed {len(overlay_entries)} server-level indexes")
    
    # # Clean up empty directories
    # print("Cleaning up empty directories...")
    
    # # Clean up from the bottom up
    # for agent_dir in example_org_dir.iterdir():
    #     if agent_dir.is_dir():
    #         profile_dir = agent_dir / "profile"
    #         if profile_dir.exists():
    #             card_dir = profile_dir / "card#me"
    #             if card_dir.exists():
    #                 # Remove empty level directories
    #                 for level_dir in ['file-level', 'pod-level', 'server-level']:
    #                     level_path = card_dir / level_dir
    #                     if level_path.exists() and not any(level_path.iterdir()):
    #                         shutil.rmtree(level_path)
                    
    #                 # Remove card#me if empty
    #                 if not any(card_dir.iterdir()):
    #                     shutil.rmtree(card_dir)
                
    #             # Remove profile if empty
    #             if not any(profile_dir.iterdir()):
    #                 shutil.rmtree(profile_dir)
            
    #         # Remove agent directory if empty
    #         if not any(agent_dir.iterdir()):
    #             shutil.rmtree(agent_dir)
    
    # # Remove empty parent directories
    # if not any(example_org_dir.iterdir()):
    #     shutil.rmtree(example_org_dir)
    # if not any(http_dir.iterdir()):
    #     shutil.rmtree(http_dir)
    
    print("Index distribution completed successfully!")

def normalize_webid(webid: str) -> str:
    """
    Convert WebID to a filesystem-safe, character-only identifier.
    Example:
    http://example.org/agent0/profile/card#me
    -> httpexampleorgagent0profilecardme
    """
    return (
        webid
        .replace("http://", "http")
        .replace("https://", "https")
        .replace("://", "")
        .replace("/", "")
        .replace("#", "")
        .replace(".", "")
    )



def main():
    # Configuration - adjust these paths as needed, (1) Where your servers are located (2) Where WebID_indexer generated indexes
    experiment_base_dir = "../data/experiment_data"  
    index_output_dir = "../data/experiment_Index"    
    
    distribute_indexes(experiment_base_dir, index_output_dir)

if __name__ == "__main__":
    main()