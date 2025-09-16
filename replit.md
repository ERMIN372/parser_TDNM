# TDNM Parser Project

## Overview
This is a Python-based parsing application for TDNM format files. The project was imported from GitHub repository `ERMIN372/parser_TDNM` but the repository was empty, so a basic parser structure has been created.

## Recent Changes
- **September 10, 2025**: Created initial project structure with main.py parser implementation
- Added basic command-line interface for file parsing
- Created sample test file for demonstration

## Project Architecture
- **main.py**: Core parser application with TDNMParser class
- **requirements.txt**: Python dependencies (currently minimal)
- **sample.txt**: Sample data file for testing
- Language: Python 3.11
- Environment: Replit with Nix

## Features
- Command-line file parsing
- Interactive mode for direct input
- Basic line-by-line content analysis
- Extensible parser class for future TDNM format specifics

## Usage
```bash
# Parse a file
python main.py sample.txt

# Interactive mode
python main.py

# Show version
python main.py --version
```