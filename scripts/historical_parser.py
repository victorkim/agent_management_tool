#!/usr/bin/env python3
"""
Historical Conversation Parser for Agent Management Tool

Processes large Claude JSON export files and creates manageable CSV outputs
for conversation analysis and project classification.

Usage:
    python scripts/historical_parser.py

Input:
    - data/historical_export/conversations.json (38MB+)
    - data/historical_export/projects.json (600KB)

Output:
    - data/filtered_output/conversations_YYYY-MM-DD_to_YYYY-MM-DD.csv
    - Each file under 20MB for Claude processing
"""

import json
import pandas as pd
import ijson
from datetime import datetime, timedelta
import os
import logging
from pathlib import Path

class HistoricalParser:
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent
        self.export_dir = self.base_dir / "data" / "historical_export"
        self.output_dir = self.base_dir / "data" / "filtered_output"
        self.config_dir = self.base_dir / "config"
        self.logs_dir = self.base_dir / "logs"
        
        # Ensure directories exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.logs_dir / 'processing.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def get_date_cutoff(self, days_back=90):
        """Calculate cutoff date for filtering conversations"""
        cutoff = datetime.now() - timedelta(days=days_back)
        self.logger.info(f"Filtering conversations since: {cutoff.strftime('%Y-%m-%d')}")
        return cutoff
    
    def parse_conversations_file(self, days_back=90):
        """Parse conversations.json with memory-efficient streaming"""
        conversations_file = self.export_dir / "conversations.json"
        
        if not conversations_file.exists():
            self.logger.error(f"Conversations file not found: {conversations_file}")
            return []
        
        cutoff_date = self.get_date_cutoff(days_back)
        filtered_conversations = []
        
        self.logger.info(f"Processing conversations file: {conversations_file}")
        self.logger.info(f"File size: {conversations_file.stat().st_size / (1024*1024):.1f} MB")
        
        try:
            with open(conversations_file, 'rb') as file:
                # Stream parse the JSON to handle large files
                conversations = ijson.items(file, 'item')
                
                for i, conversation in enumerate(conversations):
                    if i % 100 == 0:  # Progress indicator
                        self.logger.info(f"Processed {i} conversations...")
                    
                    # Extract conversation date
                    created_at = conversation.get('created_at')
                    if not created_at:
                        continue
                    
                    try:
                        conv_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        conv_date = conv_date.replace(tzinfo=None)  # Remove timezone for comparison
                    except:
                        continue
                    
                    # Filter by date
                    if conv_date >= cutoff_date:
                        processed_conv = self.process_single_conversation(conversation)
                        if processed_conv:
                            filtered_conversations.append(processed_conv)
        
        except Exception as e:
            self.logger.error(f"Error parsing conversations file: {e}")
            return []
        
        self.logger.info(f"Found {len(filtered_conversations)} conversations in last {days_back} days")
        return filtered_conversations
    
    def process_single_conversation(self, conversation):
        """Extract relevant data from a single conversation"""
        try:
            # Basic metadata
            conv_id = conversation.get('uuid', 'unknown')
            created_at = conversation.get('created_at', '')
            name = conversation.get('name', 'Untitled')
            
            # Extract messages
            chat_messages = conversation.get('chat_messages', [])
            
            if not chat_messages:
                return None
            
            # Process messages
            user_messages = []
            claude_responses = []
            total_chars = 0
            
            for message in chat_messages:
                content = message.get('text', '')
                sender = message.get('sender', '')
                
                if sender == 'human':
                    user_messages.append(content)
                elif sender == 'assistant':
                    claude_responses.append(content)
                
                total_chars += len(content)
            
            # Create conversation text (truncated if too long for CSV)
            conversation_text = f"USER MESSAGES:\n" + "\n---\n".join(user_messages[:3])
            conversation_text += f"\n\nCLAUDE RESPONSES:\n" + "\n---\n".join(claude_responses[:3])
            
            # Truncate if too long (keep under 10K chars for CSV readability)
            if len(conversation_text) > 10000:
                conversation_text = conversation_text[:10000] + "...[TRUNCATED]"
            
            return {
                'conversation_id': conv_id,
                'date': created_at.split('T')[0] if 'T' in created_at else created_at,
                'title': name,
                'message_count': len(chat_messages),
                'user_message_count': len(user_messages),
                'claude_response_count': len(claude_responses),
                'total_characters': total_chars,
                'conversation_preview': conversation_text.replace('\n', ' ').replace('\r', ' ')[:500] + "...",
                'conversation_text': conversation_text
            }
        
        except Exception as e:
            self.logger.warning(f"Error processing conversation {conversation.get('uuid', 'unknown')}: {e}")
            return None
    
    def create_csv_chunks(self, conversations, max_size_mb=18):
        """Split conversations into CSV files under size limit"""
        if not conversations:
            self.logger.warning("No conversations to process")
            return
        
        # Sort by date
        conversations.sort(key=lambda x: x['date'])
        
        # Group into chunks by date ranges
        chunks = []
        current_chunk = []
        current_size = 0
        start_date = None
        
        for conv in conversations:
            if not start_date:
                start_date = conv['date']
            
            # Estimate size (rough calculation)
            conv_size = len(str(conv)) / (1024 * 1024)  # Convert to MB
            
            if current_size + conv_size > max_size_mb and current_chunk:
                # Save current chunk
                end_date = current_chunk[-1]['date']
                chunks.append({
                    'data': current_chunk.copy(),
                    'start_date': start_date,
                    'end_date': end_date
                })
                
                # Start new chunk
                current_chunk = [conv]
                current_size = conv_size
                start_date = conv['date']
            else:
                current_chunk.append(conv)
                current_size += conv_size
        
        # Add final chunk
        if current_chunk:
            end_date = current_chunk[-1]['date']
            chunks.append({
                'data': current_chunk,
                'start_date': start_date,
                'end_date': end_date
            })
        
        # Save chunks as CSV files
        for i, chunk in enumerate(chunks):
            filename = f"conversations_{chunk['start_date']}_to_{chunk['end_date']}.csv"
            filepath = self.output_dir / filename
            
            df = pd.DataFrame(chunk['data'])
            df.to_csv(filepath, index=False)
            
            file_size = filepath.stat().st_size / (1024 * 1024)
            self.logger.info(f"Created: {filename} ({file_size:.1f} MB, {len(chunk['data'])} conversations)")
    
    def load_projects_info(self):
        """Load and process projects.json for future reference"""
        projects_file = self.export_dir / "projects.json"
        
        if not projects_file.exists():
            self.logger.warning(f"Projects file not found: {projects_file}")
            return {}
        
        try:
            with open(projects_file, 'r') as file:
                projects_data = json.load(file)
            
            self.logger.info(f"Loaded projects data: {len(projects_data)} projects")
            
            # Save simplified project mapping for future use
            project_mapping = {
                'total_projects': len(projects_data),
                'project_names': [p.get('name', 'Unnamed') for p in projects_data],
                'processed_date': datetime.now().isoformat()
            }
            
            mapping_file = self.config_dir / "project_mapping.json"
            with open(mapping_file, 'w') as file:
                json.dump(project_mapping, file, indent=2)
            
            return projects_data
        
        except Exception as e:
            self.logger.error(f"Error loading projects file: {e}")
            return {}
    
    def run(self, days_back=90):
        """Main execution method"""
        self.logger.info("Starting historical conversation processing...")
        
        # Load projects info
        projects = self.load_projects_info()
        
        # Parse conversations
        conversations = self.parse_conversations_file(days_back)
        
        if not conversations:
            self.logger.error("No conversations found to process")
            return
        
        # Create CSV outputs
        self.create_csv_chunks(conversations)
        
        # Summary
        total_files = len(list(self.output_dir.glob("conversations_*.csv")))
        self.logger.info(f"Processing complete! Created {total_files} CSV files in {self.output_dir}")
        self.logger.info("Next step: Upload CSV files to Claude for analysis and project classification")

if __name__ == "__main__":
    parser = HistoricalParser()
    parser.run()