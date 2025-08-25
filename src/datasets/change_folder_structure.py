#!/usr/bin/env python3
"""
Script to restructure legislation data from nested folder structure to year-based consolidated structure.

Original structure: {main_folder}/year/norm_type/norm_situation/norm_data.json
New structure: {main_folder}/year/data.json

This reduces the number of files by consolidating all norms of a given year into a single JSON file.
"""

import os
import json
import shutil
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
from collections import defaultdict
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# No custom imports needed

# =============================================================================
# QUICK CONFIGURATION (You can also modify these in the main() function)
# =============================================================================

# Default folder path - change this to your actual legislation folder path
# DEFAULT_FOLDER_PATH = "/mnt/c/Users/Docker/SynologyDrive/LEGISLACAO_ESPECIFICA"
# DEFAULT_FOLDER_PATH = "/mnt/c/Users/Docker/SynologyDrive/LEGISLACAO_FEDERAL"
DEFAULT_FOLDER_PATH = "/mnt/c/Users/Docker/SynologyDrive/LEGISLACAO_ESTADUAL"

# Default restructured folder path - change this to where you want restructured data stored
DEFAULT_RESTRUCTURED_FOLDER_PATH = "/home/bilaboung/PROJECTS/COCORUTA3/legislation-scraper/restructured_data"

# Default settings
DEFAULT_VALIDATE_DATA = True

# =============================================================================

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_json_file(file_path: Path) -> Dict[Any, Any]:
    """Load and return JSON data from a file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON file {file_path}: {e}")
        return {}


def save_json_file(file_path: Path, data: List[Dict[Any, Any]]) -> bool:
    """Save data to JSON file."""
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Error saving JSON file {file_path}: {e}")
        return False


def find_json_files(main_folder: Path) -> List[Path]:
    """Find all JSON files in the current nested structure using efficient single-pass scanning."""
    if not main_folder.exists():
        logger.error(f"Main folder does not exist: {main_folder}")
        return []
    
    logger.info("🔍 Scanning directory structure for JSON files...")
    
    json_files = []
    files_scanned = 0
    
    # Use a single os.walk pass with indeterminate progress bar
    progress_bar = tqdm(
        desc="🔍 Scanning files",
        unit="files",
        ncols=100
    )
    
    for root, dirs, files in os.walk(main_folder):
        # Process files in current directory
        for file in files:
            files_scanned += 1
            progress_bar.update(1)
            
            if file.endswith('.json'):
                json_files.append(Path(root) / file)
    
    progress_bar.close()
    logger.info(f"✅ Found {len(json_files)} JSON files out of {files_scanned} total files")
    return json_files


def find_json_files_generator(main_folder: Path):
    """Generator that yields JSON files as they are found - more memory efficient."""
    if not main_folder.exists():
        logger.error(f"Main folder does not exist: {main_folder}")
        return
    
    logger.info("🔍 Scanning directory structure for JSON files (generator mode)...")
    
    for root, dirs, files in os.walk(main_folder):
        for file in files:
            if file.endswith('.json'):
                yield Path(root) / file


def group_by_year(json_files: List[Path]) -> Dict[str, List[Path]]:
    """Group JSON files by year based on their folder structure, preserving state structure for state legislation."""
    year_groups = defaultdict(list)
    
    logger.info("📅 Grouping files by year...")
    
    progress_bar = tqdm(
        json_files, 
        desc="📅 Grouping by year", 
        unit="files",
        ncols=100
    )
    
    for json_file in progress_bar:
        # Extract year from the path structure
        # Supports both structures:
        # Federal: main_folder/year/type/situation/file.json
        # State: main_folder/state/year/type/situation/file.json
        parts = json_file.parts
        
        # Find the year in the path (should be a 4-digit number)
        year = None
        state = None
        year_index = -1
        
        for i, part in enumerate(parts):
            if part.isdigit() and len(part) == 4:
                year = part
                year_index = i
                # Check if there's a state folder before the year
                if i > 0 and not parts[i-1].isdigit():
                    # Assume the folder before year is the state
                    state = parts[i-1]
                break
        
        if year:
            # Create a key that preserves state structure
            if state:
                group_key = f"{state}/{year}"
            else:
                group_key = year
                
            year_groups[group_key].append(json_file)
            progress_bar.set_postfix({
                'Groups found': len(year_groups),
                'Current': group_key
            })
        else:
            logger.warning(f"Could not extract year from path: {json_file}")
            progress_bar.set_postfix({
                'Groups found': len(year_groups),
                'Current': 'No year'
            })
    
    progress_bar.close()
    
    # Show summary of groups found
    logger.info(f"✅ Grouped files into {len(year_groups)} groups:")
    sorted_groups = sorted(year_groups.keys())
    for group in sorted_groups:
        logger.info(f"   � {group}: {len(year_groups[group])} files")
    
    return year_groups


def consolidate_year_data(year_files: List[Path], group_key: str) -> List[Dict[Any, Any]]:
    """Consolidate all JSON files for a given year into a single list using concurrent processing."""
    consolidated_data = []
    
    # Use the group key for better progress description
    display_name = group_key
    
    # Use ThreadPoolExecutor for concurrent file reading
    max_workers = min((os.cpu_count() or 32), len(year_files), 32)  # Optimal thread count
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(load_json_file, json_file): json_file 
            for json_file in year_files
        }
        
        # Create progress bar
        progress_bar = tqdm(
            total=len(year_files),
            desc=f"📄 Consolidating {display_name}", 
            unit="files", 
            leave=False,
            ncols=100
        )
        
        # Collect results as they complete
        for future in as_completed(future_to_file):
            try:
                data = future.result()
                if data:
                    consolidated_data.append(data)
                
                # Update progress bar
                progress_bar.update(1)
                progress_bar.set_postfix({'consolidated': len(consolidated_data)})
                
            except Exception as e:
                file_path = future_to_file[future]
                logger.error(f"Error processing file {file_path}: {e}")
                progress_bar.update(1)
        
        progress_bar.close()
    
    return consolidated_data


def restructure_folders(main_folder: Path, restructured_base_path: Optional[Path] = None) -> tuple[bool, Optional[Path]]:
    """
    Main function to restructure the folder organization.
    
    Args:
        main_folder: Path to the main legislation folder
        restructured_base_path: Path where restructured data should be stored (if None, uses default)
    
    Returns:
        tuple: (success_status, restructured_folder_path)
    """
    main_folder = Path(main_folder)
    
    logger.info(f"Starting folder restructure for: {main_folder}")
    
    # Find all JSON files
    json_files = find_json_files(main_folder)
    if not json_files:
        logger.error("No JSON files found")
        return False, None
   
    # Group files by year
    year_groups = group_by_year(json_files)
    
    # Create new structure folder
    if restructured_base_path is None:
        restructured_base_path = Path(DEFAULT_RESTRUCTURED_FOLDER_PATH)
    
    # Ensure restructured base directory exists
    restructured_base_path.mkdir(parents=True, exist_ok=True)
    
    new_structure_folder = restructured_base_path / f"{main_folder.name}_restructured"
    new_structure_folder.mkdir(exist_ok=True)
    
    # Process each group (year or state/year)
    success_count = 0
    total_original_norms = len(json_files)
    total_consolidated_norms = 0
    
    logger.info(f"Processing {len(year_groups)} groups...")
    
    def process_group(group_data):
        """Process a single group's data concurrently."""
        group_key, files = group_data
        try:
            # Consolidate data for this group
            consolidated_data = consolidate_year_data(files, group_key)
            
            if consolidated_data:
                # Create the appropriate folder structure
                if '/' in group_key:
                    # State legislation: state/year structure
                    state, year = group_key.split('/', 1)
                    target_folder = new_structure_folder / state / year
                else:
                    # Federal legislation: year structure
                    target_folder = new_structure_folder / group_key
                
                target_folder.mkdir(parents=True, exist_ok=True)
                
                # Save consolidated data
                output_file = target_folder / "data.json"
                if save_json_file(output_file, consolidated_data):
                    return True, group_key, len(files), len(consolidated_data)
                else:
                    logger.error(f"Failed to create file for {group_key}")
                    return False, group_key, len(files), 0
            else:
                logger.warning(f"No valid data found for {group_key}")
                return False, group_key, len(files), 0
        except Exception as e:
            logger.error(f"Error processing {group_key}: {e}")
            return False, group_key, len(files), 0
    
    # Use ThreadPoolExecutor for concurrent group processing
    max_workers = min((os.cpu_count() or 4), len(year_groups), 8)  # Limit concurrent groups
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all group processing tasks
        future_to_group = {
            executor.submit(process_group, group_data): group_data[0] 
            for group_data in year_groups.items()
        }
        
        # Add progress tracking for group processing
        group_progress = tqdm(total=len(year_groups), desc="🗂️  Processing groups", unit="groups")
        
        # Collect results as they complete
        for future in as_completed(future_to_group):
            try:
                success, group_key, file_count, norm_count = future.result()
                if success:
                    success_count += 1
                
                total_consolidated_norms += norm_count
                
                group_progress.set_postfix({
                    'current': group_key,
                    'files': file_count, 
                    'norms': norm_count,
                    'total_norms': total_consolidated_norms,
                    'success': success_count
                })
                group_progress.update(1)
                
            except Exception as e:
                group_key = future_to_group[future]
                logger.error(f"Error processing group {group_key}: {e}")
                group_progress.update(1)
        
        group_progress.close()
    
    # Verify data integrity
    logger.info("="*60)
    logger.info(f"FINAL RESULTS:")
    logger.info(f"Original JSON files: {total_original_norms}")
    logger.info(f"Consolidated norms: {total_consolidated_norms}")
    logger.info(f"Successfully processed groups: {success_count}/{len(year_groups)}")
    
    if total_original_norms == total_consolidated_norms:
        logger.info("✅ SUCCESS: All norms have been preserved!")
    else:
        logger.error(f"❌ ERROR: Data loss detected! Missing {total_original_norms - total_consolidated_norms} norms")
    
    logger.info(f"New structure created at: {new_structure_folder}")
    logger.info("="*60)
    
    return success_count > 0, new_structure_folder


def validate_restructured_data(original_folder: Path, restructured_folder: Path) -> bool:
    """
    Validate that the restructured data contains all original data using concurrent processing.
    
    Args:
        original_folder: Path to original folder structure
        restructured_folder: Path to restructured folder
    
    Returns:
        bool: Validation success status
    """
    logger.info("Starting comprehensive data validation...")
    
    # Count original files with progress bar using generator for memory efficiency
    logger.info("Counting original files...")
    
    original_count = 0
    progress_bar = tqdm(desc="📊 Counting original files", unit="files", ncols=100)
    
    for json_file in find_json_files_generator(original_folder):
        original_count += 1
        progress_bar.update(1)
        if original_count % 1000 == 0:  # Update display every 1000 files
            progress_bar.set_postfix({'found': original_count})
    
    progress_bar.close()
    logger.info(f"Found {original_count} original JSON files")
    
    # Count items in restructured files with concurrent processing
    logger.info("Counting restructured items...")
    restructured_count = 0
    
    # Find all data.json files in the restructured folder (handles both federal and state structures)
    data_files = []
    for root, dirs, files in os.walk(restructured_folder):
        for file in files:
            if file == "data.json":
                data_files.append(Path(root) / file)
    
    logger.info(f"Found {len(data_files)} data.json files in restructured folder")
    
    def count_norms_in_file(data_file: Path) -> int:
        """Helper function to count norms in a data.json file."""
        try:
            data = load_json_file(data_file)
            if isinstance(data, list):
                return len(data)
        except Exception as e:
            logger.error(f"Error reading {data_file}: {e}")
        return 0
    
    # Use ThreadPoolExecutor for concurrent validation
    max_workers = min(os.cpu_count() or 32, len(data_files))  # Reasonable limit for file I/O
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all counting tasks
        future_to_file = {
            executor.submit(count_norms_in_file, data_file): data_file 
            for data_file in data_files
        }
        
        # Collect results with progress bar
        progress_bar = tqdm(data_files, desc="Validating data files", unit="files")
        
        for future in as_completed(future_to_file):
            try:
                count = future.result()
                restructured_count += count
                data_file = future_to_file[future]
                # Extract group identifier from path for logging
                relative_path = data_file.relative_to(restructured_folder)
                group_id = str(relative_path.parent)
                logger.debug(f"Group {group_id}: {count} norms")
                progress_bar.update(1)
            except Exception as e:
                data_file = future_to_file[future]
                logger.error(f"Error validating data file {data_file}: {e}")
                progress_bar.update(1)
        
        progress_bar.close()
    
    logger.info("="*60)
    logger.info(f"VALIDATION RESULTS:")
    logger.info(f"Original files: {original_count}")
    logger.info(f"Restructured items: {restructured_count}")
    logger.info(f"Difference: {original_count - restructured_count}")
    
    if original_count == restructured_count:
        logger.info("✅ Validation successful: All data preserved")
        success = True
    else:
        logger.error("❌ Validation failed: Data count mismatch")
        if original_count > restructured_count:
            logger.error(f"Missing {original_count - restructured_count} norms in restructured data")
        else:
            logger.error(f"Extra {restructured_count - original_count} norms in restructured data")
        success = False
    
    logger.info("="*60)
    return success


def main():
    """Main entry point for the script with hardcoded parameters."""
    
    # =============================================================================
    # CONFIGURATION - MODIFY THESE PARAMETERS AS NEEDED
    # =============================================================================
    
    # Path to the main legislation folder to be restructured
    FOLDER_PATH = DEFAULT_FOLDER_PATH  # You can change this path here
    
    # Path where restructured data will be stored (optional - set to None to use default)
    RESTRUCTURED_FOLDER_PATH = None  # Uses DEFAULT_RESTRUCTURED_FOLDER_PATH if None
    
    # Configuration options (you can modify these)
    VALIDATE_DATA = DEFAULT_VALIDATE_DATA      # Set to False to skip comprehensive validation
    
    # =============================================================================
    
    print("="*80)
    print("📁 LEGISLATION FOLDER RESTRUCTURING TOOL")
    print("="*80)
    
    main_folder = Path(FOLDER_PATH)
    
    if not main_folder.exists():
        logger.error(f"❌ Folder does not exist: {main_folder}")
        logger.error("💡 Please update the FOLDER_PATH variable in the main() function")
        logger.error(f"💡 Or modify DEFAULT_FOLDER_PATH at the top of this file")
        return 1
    
    restructured_path = Path(RESTRUCTURED_FOLDER_PATH) if RESTRUCTURED_FOLDER_PATH else None
    
    logger.info(f"🎯 Starting restructuring process...")
    logger.info(f"📂 Target folder: {main_folder}")
    restructured_display = restructured_path or Path(DEFAULT_RESTRUCTURED_FOLDER_PATH)
    logger.info(f"📁 Restructured location: {restructured_display}")
    logger.info(f"✅ Validation enabled: {VALIDATE_DATA}")
    print()
    
    # Start timing
    start_time = time.time()
    
    # Perform restructuring
    success, restructured_folder = restructure_folders(main_folder, restructured_path)
    
    if not success or not restructured_folder:
        logger.error("❌ Restructuring failed")
        return 1

    # Calculate total processing time
    end_time = time.time()
    total_time_seconds = end_time - start_time
    total_time_minutes = total_time_seconds / 60
    
    print("\n" + "="*80)
    print("🎉 RESTRUCTURING COMPLETED SUCCESSFULLY!")
    print("="*80)
    print(f"📁 Original folder: {main_folder}")
    print(f"📂 Restructured folder: {restructured_folder}")
    print(f"⏱️  Total processing time: {total_time_minutes:.2f} minutes ({total_time_seconds:.1f} seconds)")
    print(f"💡 You can now use the restructured folder for better performance!")
    print("="*80)


    # Validate data if enabled
    if VALIDATE_DATA:
        if not validate_restructured_data(main_folder, restructured_folder):
            logger.error("❌ Validation failed - please check the logs above")
            return 1
    else:
        # Always perform basic validation even if comprehensive validation is disabled
        logger.info("Performing basic validation...")
        validate_restructured_data(main_folder, restructured_folder)
    
    return 0


if __name__ == "__main__":
    exit(main())
