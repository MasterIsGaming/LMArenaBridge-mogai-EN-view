# update_script.py
import os
import shutil
import time
import subprocess
import sys
import json
import re

def _parse_jsonc(jsonc_string: str) -> dict:
    """
    Robustly parses a JSONC string, removing comments.
    """
    lines = jsonc_string.splitlines()
    no_comments_lines = []
    in_block_comment = False
    for line in lines:
        stripped_line = line.strip()
        if in_block_comment:
            if '*/' in stripped_line:
                in_block_comment = False
                line = stripped_line.split('*/', 1)[1]
            else:
                continue

        if '/*' in line and not in_block_comment:
            before_comment, _, after_comment = line.partition('/*')
            if '*/' in after_comment:
                _, _, after_block = after_comment.partition('*/')
                line = before_comment + after_block
            else:
                line = before_comment
                in_block_comment = True

        if line.strip().startswith('//'):
            continue

        no_comments_lines.append(line)

    return json.loads("\n".join(no_comments_lines))

def load_jsonc_values(path):
    """Loads data from a .jsonc file, ignoring comments, and returns only key-value pairs."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        return _parse_jsonc(content)
    except (FileNotFoundError, json.JSONDecodeError, Exception) as e:
        print(f"Error loading or parsing values from {path}: {e}")
        return None

def get_all_relative_paths(directory):
    """Gets a set of relative paths for all files and empty folders within a directory."""
    paths = set()
    for root, dirs, files in os.walk(directory):
        # Add files
        for name in files:
            path = os.path.join(root, name)
            paths.add(os.path.relpath(path, directory))
        # Add empty folders
        for name in dirs:
            dir_path = os.path.join(root, name)
            if not os.listdir(dir_path):
                paths.add(os.path.relpath(dir_path, directory) + os.sep)
    return paths

def main():
    print("--- Update Script Started ---")

    # 1. Wait for the main program to exit
    print("Waiting for the main program to shut down (3 seconds)...")
    time.sleep(3)

    # 2. Define paths
    destination_dir = os.getcwd()
    update_dir = "update_temp"
    source_dir_inner = os.path.join(update_dir, "LMArenaBridge-mogai-mogai-version")
    config_filename = 'config.jsonc'
    models_filename = 'models.json'
    model_endpoint_map_filename = 'model_endpoint_map.json'

    if not os.path.exists(source_dir_inner):
        print(f"Error: Source directory {source_dir_inner} not found. Update failed.")
        return

    print(f"Source directory: {os.path.abspath(source_dir_inner)}")
    print(f"Destination directory: {os.path.abspath(destination_dir)}")

    # 3. Backup critical files
    print("Backing up current configuration and model files...")
    old_config_path = os.path.join(destination_dir, config_filename)
    old_models_path = os.path.join(destination_dir, models_filename)
    old_config_values = load_jsonc_values(old_config_path)

    # 4. Determine files and folders to preserve
    # Preserve update_temp itself, the .git directory, and any hidden files/folders users might have added
    preserved_items = {update_dir, ".git", ".github"}

    # 5. Get lists of new and old files
    new_files = get_all_relative_paths(source_dir_inner)
    # Exclude .git and .github directories as they should not be deployed
    new_files = {f for f in new_files if not (f.startswith('.git') or f.startswith('.github'))}

    current_files = get_all_relative_paths(destination_dir)

    print("\n--- File Change Analysis ---")
    print("[*] File deletion functionality is disabled to protect user data. Only file copying and configuration updates will be performed.")

    # 7. Copy new files (excluding configuration files)
    print("\n[+] Copying new files...")
    try:
        new_config_template_path = os.path.join(source_dir_inner, config_filename)

        for item in os.listdir(source_dir_inner):
            s = os.path.join(source_dir_inner, item)
            d = os.path.join(destination_dir, item)

            # Skip .git and .github directories
            if item in {".git", ".github"}:
                continue

            if os.path.basename(s) == config_filename:
                continue # Skip main configuration file, handle later

            if os.path.basename(s) == model_endpoint_map_filename:
                continue # Skip model endpoint map file, preserve user's local version

            if os.path.basename(s) == models_filename:
                continue # Skip models.json file, preserve user's local version

            if os.path.isdir(s):
                shutil.copytree(s, d, dirs_exist_ok=True)
            else:
                shutil.copy2(s, d)
        print("Files copied successfully.")

    except Exception as e:
        print(f"Error during file copying: {e}")
        return

    # 8. Smartly merge configuration
    if old_config_values and os.path.exists(new_config_template_path):
        print("\n[*] Smartly merging configuration (preserving comments)...")
        try:
            with open(new_config_template_path, 'r', encoding='utf-8') as f:
                new_config_content = f.read()

            new_version_values = load_jsonc_values(new_config_template_path)
            new_version = new_version_values.get("version", "unknown")
            old_config_values["version"] = new_version

            for key, value in old_config_values.items():
                if isinstance(value, str):
                    replacement_value = f'"{value}"'
                elif isinstance(value, bool):
                    replacement_value = str(value).lower()
                else:
                    replacement_value = str(value)

                pattern = re.compile(f'("{key}"\\s*:\\s*)(?:".*?"|true|false|[\\d\\.]+)')
                if pattern.search(new_config_content):
                    new_config_content = pattern.sub(f'\\g<1>{replacement_value}', new_config_content)

            with open(old_config_path, 'w', encoding='utf-8') as f:
                f.write(new_config_content)
            print("Configuration merged successfully.")

        except Exception as e:
            print(f"Serious error during configuration merge: {e}")
    else:
        print("Smart merge not possible, using the new configuration file directly.")
        if os.path.exists(new_config_template_path):
            shutil.copy2(new_config_template_path, old_config_path)

    # 9. Clean up temporary folder
    print("\n[*] Cleaning up temporary files...")
    try:
        shutil.rmtree(update_dir)
        print("Cleanup complete.")
    except Exception as e:
        print(f"Error cleaning up temporary files: {e}")

    # 10. Restart main program
    print("\n[*] Restarting main program...")
    try:
        main_script_path = os.path.join(destination_dir, "api_server.py")
        if not os.path.exists(main_script_path):
             print(f"Error: Main program script {main_script_path} not found.")
             return

        subprocess.Popen([sys.executable, main_script_path])
        print("Main program restarted in the background.")
    except Exception as e:
        print(f"Failed to restart main program: {e}")
        print(f"Please manually run {main_script_path}")

    print("--- Update Complete ---")

if __name__ == "__main__":
    main()
