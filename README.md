# Duplicate File Finder

A fast, efficient duplicate file finder and remover for Linux, macOS, and Synology NAS.

## Features

- **Multi-stage hashing** - Fast detection using file size, then partial hash, then full hash
- **Flexible filtering** - Include/exclude files by path patterns
- **Media-only mode** - Scan only media files (photos, videos, audio)
- **Safe deletion** - Dry-run mode to preview deletions before committing
- **Priority paths** - Keep files from specific directories when deleting duplicates
- **VM file protection** - Excludes virtual machine files by default
- **Detailed reporting** - Saves results to text files for review

## Requirements

- **Python 3.12 or higher**
- No external dependencies (uses only standard library)

### Installing Python 3.12

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install python3.12
```
**Synology:**

Install python3.12 from community packages.
