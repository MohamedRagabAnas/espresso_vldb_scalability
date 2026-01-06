import subprocess
import json
import os
from pathlib import Path



PROJECT_ROOT = Path(__file__).resolve().parent.parent


# Specify file paths
# json_file_path = "../data/experiment_data/webid_structure.json"
# source_dir = "../data/source_files"
# output_dir = "../data/experiment_Index"
# jar_file = "../jars/Webid-Specific-Indexer.jar"

json_file_path = PROJECT_ROOT / "data" / "experiment_data" / "webid_structure.json"
source_dir = PROJECT_ROOT / "data" / "source_files"
output_dir = PROJECT_ROOT / "data" / "experiment_Index"
jar_file = PROJECT_ROOT / "jars" / "WebID-Specific-Indexer.jar"

# Ensure the JSON file exists
if not json_file_path.exists():
    raise FileNotFoundError(f"The JSON file {json_file_path} does not exist.")

# Optional: validate JSON file
with open(json_file_path, "r", encoding="utf-8") as f:
    json.load(f)  # Will raise an error if JSON is invalid

# Run the Java program with the JSON file path
result = subprocess.run(
    ["java", "-jar", str(jar_file), str(json_file_path), str(source_dir), str(output_dir)],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    universal_newlines=True  
)


# Print the output from the Java program
print("STDOUT:")
print(result.stdout)
print("\nSTDERR:")
print(result.stderr)
