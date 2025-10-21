#!/usr/bin/env python3
# organize_images.py
# Organizes images in the downloaded_images folder by date

import os
import shutil
from pathlib import Path
from datetime import datetime
import re
import sys

# Configuration
IMAGE_SAVE_DIR = Path("./downloaded_images")
SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'}

# Statistics
stats = {
    'total_files': 0,
    'organized_files': 0,
    'already_organized': 0,
    'invalid_files': 0,
    'errors': 0
}

def extract_date_from_filename(filename):
    """
    Extracts the date from the filename.
    Supported formats:
    - YYYYMMDD_HHMMSS_mmm_xxxxxxxx.ext (current format)
    - YYYYMMDD_*.ext
    """
    # Attempt to match YYYYMMDD date format
    date_pattern = r'(\d{8})_'
    match = re.match(date_pattern, filename)

    if match:
        date_str = match.group(1)
        # Validate date format
        try:
            datetime.strptime(date_str, '%Y%m%d')
            return date_str
        except ValueError:
            return None

    return None

def extract_date_from_mtime(filepath):
    """
    Extracts the date from the file's modification time.
    """
    try:
        mtime = os.path.getmtime(filepath)
        date_obj = datetime.fromtimestamp(mtime)
        return date_obj.strftime('%Y%m%d')
    except Exception as e:
        print(f"  ⚠️  Could not get file modification time: {e}")
        return None

def is_valid_date_folder(folder_name):
    """
    Checks if it's a valid date folder (8 digits, YYYYMMDD format).
    """
    if not folder_name.isdigit() or len(folder_name) != 8:
        return False

    try:
        datetime.strptime(folder_name, '%Y%m%d')
        return True
    except ValueError:
        return False

def organize_images(dry_run=True, verbose=False):
    """
    Organizes image files.

    Args:
        dry_run: If True, only shows the operations to be performed, does not actually move files.
        verbose: Verbose output mode.
    """
    global stats

    print("="*60)
    print("📁 Image Organization Tool")
    print("="*60)

    if not IMAGE_SAVE_DIR.exists():
        print(f"❌ Error: Directory '{IMAGE_SAVE_DIR}' does not exist")
        return

    print(f"📂 Scanning directory: {IMAGE_SAVE_DIR.absolute()}")
    print(f"🔍 Mode: {'Preview mode (no files will be moved)' if dry_run else 'Execution mode (files will be moved)'}")
    print()

    # Collect files to be organized
    files_to_organize = []

    # Iterate through files in the main directory
    for item in IMAGE_SAVE_DIR.iterdir():
        if item.is_file():
            stats['total_files'] += 1

            # Check if it's an image file
            if item.suffix.lower() not in SUPPORTED_EXTENSIONS:
                if verbose:
                    print(f"  ⏭️  Skipping non-image file: {item.name}")
                stats['invalid_files'] += 1
                continue

            # Extract date
            date_str = extract_date_from_filename(item.name)

            # If no date in filename, use modification time
            if not date_str:
                if verbose:
                    print(f"  ℹ️  No date info in filename, using modification time: {item.name}")
                date_str = extract_date_from_mtime(item)

            if date_str:
                files_to_organize.append((item, date_str))
            else:
                print(f"  ⚠️  Could not determine date, skipping: {item.name}")
                stats['invalid_files'] += 1

        elif item.is_dir():
            # Check existing date folders
            if is_valid_date_folder(item.name):
                folder_files = list(item.glob('*'))
                image_files = [f for f in folder_files if f.suffix.lower() in SUPPORTED_EXTENSIONS]
                stats['already_organized'] += len(image_files)
                if verbose:
                    print(f"  ✅ Already organized folder: {item.name} ({len(image_files)} files)")
            elif verbose:
                print(f"  ℹ️  Non-date folder: {item.name}")

    # Display statistics
    print()
    print("📊 Scan Statistics:")
    print(f"  - Total files in main directory: {stats['total_files']}")
    print(f"  - Files to be organized: {len(files_to_organize)}")
    print(f"  - Files already in date folders: {stats['already_organized']}")
    print(f"  - Skipped files: {stats['invalid_files']}")
    print()

    if not files_to_organize:
        print("✅ No files to organize")
        return

    # Group by date
    date_groups = {}
    for filepath, date_str in files_to_organize:
        if date_str not in date_groups:
            date_groups[date_str] = []
        date_groups[date_str].append(filepath)

    print(f"📅 Grouping files into {len(date_groups)} date folders:")
    for date_str in sorted(date_groups.keys()):
        print(f"  - {date_str}: {len(date_groups[date_str])} files")
    print()

    # Perform organization
    if dry_run:
        print("🔍 Preview of operations to be performed:")
        print()
    else:
        print("🚀 Starting file organization:")
        print()

    for date_str, files in sorted(date_groups.items()):
        # Create date folder
        date_folder = IMAGE_SAVE_DIR / date_str

        if dry_run:
            print(f"📁 [{date_str}] Will create/use folder: {date_folder}")
        else:
            date_folder.mkdir(exist_ok=True)
            print(f"📁 [{date_str}] Folder prepared: {date_folder}")

        # Move files
        for filepath in files:
            target_path = date_folder / filepath.name

            # Check if target file already exists
            if target_path.exists():
                print(f"  ⚠️  File already exists, skipping: {filepath.name}")
                continue

            if dry_run:
                print(f"  ➡️  {filepath.name} -> {date_str}/{filepath.name}")
            else:
                try:
                    shutil.move(str(filepath), str(target_path))
                    print(f"  ✅ {filepath.name} -> {date_str}/{filepath.name}")
                    stats['organized_files'] += 1
                except Exception as e:
                    print(f"  ❌ Failed to move: {filepath.name} - {e}")
                    stats['errors'] += 1

        print()

    # Final statistics
    print("="*60)
    print("📈 Organization complete!")
    print("="*60)

    if not dry_run:
        print(f"  ✅ Successfully organized: {stats['organized_files']} files")
        print(f"  ⚠️  Errors occurred: {stats['errors']} files")
    else:
        print(f"  📋 Estimated to organize: {len(files_to_organize)} files")
        print()
        print("💡 Tip: Use --execute parameter to perform actual organization")

    print("="*60)

def main():
    """
    Main function.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description='Organizes images in the downloaded_images folder by date',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python organize_images.py                  # Preview mode (does not actually move files)
  python organize_images.py --execute        # Execute organization
  python organize_images.py --execute -v     # Execute organization (verbose output)
        """
    )

    parser.add_argument(
        '--execute',
        action='store_true',
        help='Actually performs organization (defaults to preview mode)'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output mode'
    )

    args = parser.parse_args()

    # Confirm execution
    if args.execute:
        print()
        response = input("⚠️  Are you sure you want to perform the organization operation? This will move files to date folders. (y/N): ")
        if response.lower() != 'y':
            print("❌ Operation cancelled")
            return
        print()

    organize_images(dry_run=not args.execute, verbose=args.verbose)

if __name__ == "__main__":
    main()
