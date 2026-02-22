#!/usr/bin/env python3.12
"""
Duplicate File Finder - Finds and optionally deletes duplicate files.

Usage:
    python duplicate_finder.py --mode find --target /path/to/search
    python duplicate_finder.py --mode delete
    python duplicate_finder.py --mode dryrun
"""

import argparse
import hashlib
import logging
import os
import pickle
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Configuration
DEFAULT_CHUNK_SIZE = 8192  # 8KB chunks for better performance
DEFAULT_MINI_HASH_SIZE = 1024

# File extension categories
VM_EXTENSIONS = {".vmdk", ".vmx", ".vmsd", ".vmxf", ".lck", ".appinfo", ".nvram", ".vmem", ".vmss"}
MEDIA_EXTENSIONS = {".jpg", ".jpeg", ".bmp", ".tiff", ".gif", ".mov", ".mp3", ".mp4", 
                    ".wav", ".mpeg", ".mpg", ".png", ".tif", ".tiff", ".webp", ".avi"}


# Version check
def check_python_version():
    """Check if running Python 3.12 or higher."""
    if sys.version_info < (3, 12):
        print(f"Error: Python 3.12+ required. You are running Python {sys.version_info.major}.{sys.version_info.minor}")
        print("Please install Python 3.12 or higher:")
        print("  sudo apt install python3.12  # Debian/Ubuntu")
        print("  sudo dnf install python3.12  # Fedora")
        print("  brew install python3.12      # macOS")
        sys.exit(1)


check_python_version()


@dataclass
class DuplicateGroup:
    """Represents a group of duplicate files."""
    hash_value: bytes
    files: list[Path] = field(default_factory=list)
    
    @property
    def count(self) -> int:
        return len(self.files)
    
    @property
    def total_size(self) -> int:
        """Total size of all files in this group."""
        total = 0
        for f in self.files:
            try:
                total += f.stat().st_size
            except OSError:
                pass
        return total


@dataclass
class ScanResult:
    """Results from a duplicate scan."""
    duplicates: list[DuplicateGroup] = field(default_factory=list)
    files_scanned: int = 0
    unique_sizes: int = 0
    duplicate_files: int = 0
    
    @property
    def duplicate_groups(self) -> int:
        return len(self.duplicates)


class DuplicateFinder:
    """Finds duplicate files using a multi-stage hashing approach."""
    
    def __init__(self, 
                 mini_hash_size: int = DEFAULT_MINI_HASH_SIZE,
                 chunk_size: int = DEFAULT_CHUNK_SIZE,
                 hash_algorithm: str = "sha1"):
        self.mini_hash_size = mini_hash_size
        self.chunk_size = chunk_size
        self.hash_algorithm = hash_algorithm
        self.logger = logging.getLogger(__name__)
        
    def _get_hash_object(self):
        """Get a fresh hash object based on the configured algorithm."""
        return hashlib.new(self.hash_algorithm)
    
    def _read_file_chunks(self, file_path: Path):
        """Generator that yields chunks from a file."""
        with open(file_path, 'rb') as f:
            while chunk := f.read(self.chunk_size):
                yield chunk
    
    def get_partial_hash(self, file_path: Path) -> bytes:
        """Get hash of first N bytes (mini hash)."""
        hash_obj = self._get_hash_object()
        with open(file_path, 'rb') as f:
            hash_obj.update(f.read(self.mini_hash_size))
        return hash_obj.digest()
    
    def get_full_hash(self, file_path: Path) -> bytes:
        """Get hash of entire file."""
        hash_obj = self._get_hash_object()
        for chunk in self._read_file_chunks(file_path):
            hash_obj.update(chunk)
        return hash_obj.digest()
    
    def should_exclude_path(self, path: Path, exclude_patterns: Optional[list[str]] = None) -> bool:
        """Check if path should be excluded based on patterns."""
        if not exclude_patterns:
            return False
        
        path_str = str(path).lower()
        return any(pattern.lower() in path_str for pattern in exclude_patterns)
    
    def should_include_path(self, path: Path, include_patterns: Optional[list[str]] = None) -> bool:
        """Check if path should be included based on patterns."""
        if not include_patterns:
            return True
        
        path_str = str(path).lower()
        return any(pattern.lower() in path_str for pattern in include_patterns)
    
    def _is_media_file(self, file_path: Path) -> bool:
        """Check if file is a media file based on extension."""
        return file_path.suffix.lower() in MEDIA_EXTENSIONS
    
    def scan_for_duplicates(self, 
                           paths: list[Path], 
                           exclude_patterns: Optional[list[str]] = None,
                           include_patterns: Optional[list[str]] = None,
                           exclude_vm_files: bool = True,
                           media_only: bool = False) -> ScanResult:
        """
        Scan directories for duplicate files.
        
        Uses a three-stage approach for efficiency:
        1. Group by file size (fast)
        2. Hash first N bytes (medium)
        3. Hash entire file (thorough)
        """
        result = ScanResult()
        
        # Stage 1: Group by file size
        self.logger.info("Stage 1: Grouping files by size...")
        files_by_size: dict[int, list[Path]] = {}
        
        for search_path in paths:
            for file_path in search_path.rglob('*'):
                if not file_path.is_file():
                    continue
                
                # Check if media-only mode is enabled
                if media_only and not self._is_media_file(file_path):
                    continue
                
                # Check exclusions/inclusions
                if self.should_exclude_path(file_path, exclude_patterns):
                    continue
                if not self.should_include_path(file_path, include_patterns):
                    continue
                
                # Skip VM files if configured
                if exclude_vm_files and file_path.suffix.lower() in VM_EXTENSIONS:
                    continue
                
                try:
                    # Resolve symlinks to avoid duplicates from symlinks
                    real_path = file_path.resolve()
                    size = real_path.stat().st_size
                    
                    if size not in files_by_size:
                        files_by_size[size] = []
                    files_by_size[size].append(real_path)
                    result.files_scanned += 1
                    
                except (OSError, PermissionError) as e:
                    self.logger.debug(f"Cannot access {file_path}: {e}")
                    continue
        
        result.unique_sizes = len(files_by_size)
        self.logger.info(f"Found {result.files_scanned} files with {result.unique_sizes} unique sizes")
        
        # Stage 2: Hash first N bytes for files with same size
        self.logger.info("Stage 2: Computing partial hashes...")
        hashes_by_partial: dict[bytes, list[Path]] = {}
        
        for size, file_list in files_by_size.items():
            if len(file_list) < 2:
                continue  # Unique size, skip
            
            for file_path in file_list:
                try:
                    partial_hash = self.get_partial_hash(file_path)
                    if partial_hash not in hashes_by_partial:
                        hashes_by_partial[partial_hash] = []
                    hashes_by_partial[partial_hash].append(file_path)
                except (OSError, PermissionError):
                    continue
        
        self.logger.info(f"Found {len(hashes_by_partial)} files with matching partial hashes")
        
        # Stage 3: Full hash for final duplicate detection
        self.logger.info("Stage 3: Computing full hashes...")
        hashes_full: dict[bytes, DuplicateGroup] = {}
        
        for partial_hash, file_list in hashes_by_partial.items():
            if len(file_list) < 2:
                continue
            
            for file_path in file_list:
                try:
                    full_hash = self.get_full_hash(file_path)
                    
                    if full_hash in hashes_full:
                        hashes_full[full_hash].files.append(file_path)
                    else:
                        hashes_full[full_hash] = DuplicateGroup(
                            hash_value=full_hash,
                            files=[file_path]
                        )
                except (OSError, PermissionError):
                    continue
        
        # Convert to list, filter to only actual duplicates
        result.duplicates = [dg for dg in hashes_full.values() if dg.count > 1]
        result.duplicate_files = sum(dg.count for dg in result.duplicates)
        
        self.logger.info(f"Found {result.duplicate_files} duplicate files in {len(result.duplicates)} groups")
        
        return result


class DuplicateManager:
    """Manages duplicate file deletion and reporting."""
    
    def __init__(self, scan_result: ScanResult):
        self.scan_result = scan_result
        self.delete_candidates: list[Path] = []
        self.keep_paths: list[Path] = []
        
    def set_priority_paths(self, paths: list[str]):
        """Set paths that should be kept when deleting duplicates."""
        self.keep_paths = [Path(p).resolve() for p in paths]
        
    def select_duplicates_to_delete(self):
        """
        Select which duplicates to delete.
        
        Strategy: Keep files in priority paths, delete from others.
        For each duplicate group, keep the first file, mark rest for deletion.
        """
        for group in self.scan_result.duplicates:
            # Sort files: priority paths first, then by path length (shorter = more likely to be original)
            sorted_files = sorted(
                group.files,
                key=lambda f: (
                    not any(self._is_subpath(f, p) for p in self.keep_paths),
                    len(str(f))
                )
            )
            
            # Keep first file, mark rest for deletion
            for file_path in sorted_files[1:]:
                if file_path.exists():
                    self.delete_candidates.append(file_path)
    
    def _is_subpath(self, path: Path, parent: Path) -> bool:
        """Check if path is under parent directory."""
        try:
            path.resolve().relative_to(parent.resolve())
            return True
        except ValueError:
            return False
    
    def delete_files(self, dry_run: bool = True) -> tuple[int, int]:
        """
        Delete selected files.
        
        Returns:
            Tuple of (success_count, failure_count)
        """
        success = 0
        failed = 0
        
        for file_path in self.delete_candidates:
            try:
                if dry_run:
                    print(f"[DRY RUN] Would delete: {file_path}")
                else:
                    file_path.unlink()
                    print(f"Deleted: {file_path}")
                success += 1
            except OSError as e:
                print(f"Failed to delete {file_path}: {e}")
                failed += 1
        
        return success, failed
    
    def save_results(self, base_path: Path = Path(".")):
        """Save scan results to files."""
        # Save duplicate groups
        output_file = base_path / "duplicate_groups.txt"
        with open(output_file, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("DUPLICATE FILES REPORT\n")
            f.write("=" * 60 + "\n\n")
            
            for i, group in enumerate(self.scan_result.duplicates, 1):
                f.write(f"Group {i} ({group.count} files, {group.total_size:,} bytes):\n")
                for file_path in group.files:
                    f.write(f"  - {file_path}\n")
                f.write("\n")
        
        # Save deletion list
        delete_file = base_path / "delete_list.txt"
        with open(delete_file, 'w') as f:
            for file_path in self.delete_candidates:
                f.write(f"{file_path}\n")
        
        # Save serialized data for delete mode
        data_file = base_path / "scan_data.pkl"
        with open(data_file, 'wb') as f:
            pickle.dump(self.scan_result, f)
        
        # Save priority paths
        paths_file = base_path / "priority_paths.txt"
        with open(paths_file, 'w') as f:
            for path in self.keep_paths:
                f.write(f"{path}\n")
        
        return output_file, delete_file
    
    @classmethod
    def load_results(cls, base_path: Path = Path(".")) -> 'DuplicateManager':
        """Load previous scan results for deletion."""
        data_file = base_path / "scan_data.pkl"
        with open(data_file, 'rb') as f:
            scan_result = pickle.load(f)
        
        manager = cls(scan_result)
        
        # Load priority paths
        paths_file = base_path / "priority_paths.txt"
        if paths_file.exists():
            with open(paths_file, 'r') as f:
                manager.keep_paths = [Path(line.strip()) for line in f if line.strip()]
        
        return manager


def setup_logging(log_file: str = "find.log"):
    """Configure logging to file and console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )


def validate_arguments(args) -> list[Path]:
    """Validate command line arguments."""
    if args.mode == 'find' and not args.target:
        print("Error: --target is required in find mode")
        sys.exit(1)
    
    if not args.target:
        return []
    
    # Convert to Path objects and validate
    target_paths = [Path(t).resolve() for t in args.target]
    
    for path in target_paths:
        if not path.exists():
            print(f"Error: Path does not exist: {path}")
            sys.exit(1)
    
    # Check for duplicate paths
    if len(target_paths) != len(set(target_paths)):
        print("Error: Duplicate paths provided")
        sys.exit(1)
    
    # Check for parent/child relationships
    for i, path1 in enumerate(target_paths):
        for path2 in target_paths[i+1:]:
            try:
                path1.relative_to(path2)
                print(f"Error: '{path1}' is a subdirectory of '{path2}'")
                sys.exit(1)
            except ValueError:
                pass
            try:
                path2.relative_to(path1)
                print(f"Error: '{path2}' is a subdirectory of '{path1}'")
                sys.exit(1)
            except ValueError:
                pass
    
    return target_paths


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Find and optionally delete duplicate files.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode find --target /home/user/documents
  %(prog)s --mode find --target /path1 /path2 --exclude ".git" "node_modules"
  %(prog)s --mode delete
  %(prog)s --mode dryrun
        """
    )
    
    parser.add_argument(
        '--mode', 
        choices=['find', 'delete', 'dryrun'],
        required=True,
        help='Operation mode: find (scan only), delete (actually delete), dryrun (simulate delete)'
    )
    parser.add_argument(
        '--target',
        nargs='+',
        help='Directory(ies) to search for duplicates'
    )
    parser.add_argument(
        '--exclude',
        nargs='*',
        help='Strings to exclude from search (case-insensitive)'
    )
    parser.add_argument(
        '--include',
        nargs='*',
        help='Only include files matching these strings (case-insensitive)'
    )
    parser.add_argument(
        '--media',
        action='store_true',
        help='Only search media files (photos, videos, audio)'
    )
    parser.add_argument(
        '--mini-hash-size',
        type=int,
        default=DEFAULT_MINI_HASH_SIZE,
        help=f'Size in bytes for partial hash (default: {DEFAULT_MINI_HASH_SIZE})'
    )
    parser.add_argument(
        '--include-vm',
        action='store_true',
        help='Include VM files in duplicate detection'
    )
    parser.add_argument(
        '--log-file',
        default='find.log',
        help='Log file path (default: find.log)'
    )
    
    args = parser.parse_args()
    
    setup_logging(args.log_file)
    logger = logging.getLogger(__name__)
    start_time = time.time()
    
    try:
        if args.mode == 'find':
            # Validate and get target paths
            target_paths = validate_arguments(args)
            
            logger.info(f"Starting duplicate scan in: {target_paths}")
            logger.info(f"Exclude patterns: {args.exclude}")
            logger.info(f"Include patterns: {args.include}")
            logger.info(f"Media only mode: {args.media}")
            
            # Create finder and scan
            finder = DuplicateFinder(mini_hash_size=args.mini_hash_size)
            result = finder.scan_for_duplicates(
                paths=target_paths,
                exclude_patterns=args.exclude,
                include_patterns=args.include,
                exclude_vm_files=not args.include_vm,
                media_only=args.media
            )
            
            # Create manager and save results
            manager = DuplicateManager(result)
            manager.set_priority_paths([str(p) for p in target_paths])
            output_file, delete_file = manager.save_results()
            
            # Print summary
            print("\n" + "=" * 60)
            print("SCAN COMPLETE")
            print("=" * 60)
            print(f"Files scanned:      {result.files_scanned}")
            print(f"Unique sizes:       {result.unique_sizes}")
            print(f"Duplicate groups:   {result.duplicate_groups}")
            print(f"Duplicate files:    {result.duplicate_files}")
            print(f"\nResults saved to:")
            print(f"  - {output_file}")
            print(f"  - {delete_file}")
            
        elif args.mode in ('delete', 'dryrun'):
            # Load previous results
            logger.info(f"Loading previous scan results...")
            manager = DuplicateManager.load_results()
            
            # Select files to delete
            manager.select_duplicates_to_delete()
            
            print(f"\nFiles marked for deletion: {len(manager.delete_candidates)}")
            
            # Perform deletion
            success, failed = manager.delete_files(dry_run=(args.mode == 'dryrun'))
            
            print(f"\nDeletion complete: {success} deleted, {failed} failed")
            
            # Update delete list
            with open("delete_list.txt", 'w') as f:
                for path in manager.delete_candidates:
                    if path.exists():
                        f.write(f"{path}\n")
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Error: {e}")
        sys.exit(1)
    
    elapsed = time.time() - start_time
    print(f"\nCompleted in {elapsed:.3f} seconds")


if __name__ == "__main__":
    main()
