import subprocess
import json
import os

# Specify file paths
json_file_path = "../data/experiment_data/webid_structure.json"
source_dir = "../data/source_files"
output_dir = "../data/experiment_Index"
jar_file = "../jars/Webid-Specific-Indexer.jar"

# Ensure the JSON file exists
if not os.path.exists(json_file_path):
    raise FileNotFoundError(f"The JSON file {json_file_path} does not exist.")

# Optional: validate JSON file
with open(json_file_path, "r", encoding="utf-8") as f:
    json.load(f)  # Will raise an error if JSON is invalid

# Run the Java program with the JSON file path
result = subprocess.run(
    ["java", "-jar", jar_file, json_file_path, source_dir, output_dir],
    capture_output=True,
    text=True
)

# Print the output from the Java program
print("STDOUT:")
print(result.stdout)
print("\nSTDERR:")
print(result.stderr)
