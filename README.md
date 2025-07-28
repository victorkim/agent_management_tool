cat << 'EOF' > README.md
# Agent Management Tool

Automated system for processing and managing Claude AI conversation history.

## Phase 1: Historical Data Processing

Converts large JSON exports from Claude into digestible CSV files for analysis and project classification.

## Quick Start

```bash
# Setup environment
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Place your Claude export files in data/historical_export/
# conversations.json (38MB+)
# projects.json (600KB)

# Run the parser
python scripts/historical_parser.py

# Find processed output in data/filtered_output/
