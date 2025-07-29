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
    - data/filtered_output/selected_conversations.csv
    - Complete conversations for specified IDs only
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
        
        # SELECTED CONVERSATION IDs - CHANGE THESE AS NEEDED
        self.selected_conversation_ids = [
            "b1e4c718-a2a6-4a24-8259-e10b6388f613",
            "2f4d1d04-d52c-4c6b-a667-8f8249d87abe",
            "36897026-def1-4e86-a7ca-123cf662b23a",
            "005f7c9c-68c5-4226-83b5-4bed77daceea",
            "524eb90a-30fc-4054-92b4-76488ad346e3",
            "7c880d93-2777-4302-9f4c-5de0f3b451da",
            "7518ebce-c1a0-427e-9fa9-f6f0f025596b",
            "e8704fc9-6d54-485b-9a33-415045e86768"
        ]
        
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
    
    def parse_conversations_file(self):
        """Parse conversations.json and filter by selected IDs"""
        conversations_file = self.export_dir / "conversations.json"
        
        if not conversations_file.exists():
            self.logger.error(f"Conversations file not found: {conversations_file}")
            return []
        
        filtered_conversations = []
        total_processed = 0
        
        self.logger.info(f"Processing conversations file: {conversations_file}")
        self.logger.info(f"File size: {conversations_file.stat().st_size / (1024*1024):.1f} MB")
        self.logger.info(f"Looking for {len(self.selected_conversation_ids)} specific conversations")
        
        try:
            with open(conversations_file, 'rb') as file:
                # Stream parse the JSON to handle large files
                conversations = ijson.items(file, 'item')
                
                for conversation in conversations:
                    total_processed += 1
                    
                    if total_processed % 100 == 0:  # Progress indicator
                        self.logger.info(f"Processed {total_processed} conversations, found {len(filtered_conversations)} matches...")
                    
                    # Check if this conversation ID is in our selected list
                    conv_id = conversation.get('uuid', '')
                    if conv_id in self.selected_conversation_ids:
                        self.logger.info(f"Found selected conversation: {conv_id}")
                        processed_conv = self.process_single_conversation(conversation)
                        if processed_conv:
                            filtered_conversations.append(processed_conv)
        
        except Exception as e:
            self.logger.error(f"Error parsing conversations file: {e}")
            return []
        
        self.logger.info(f"Total conversations processed: {total_processed}")
        self.logger.info(f"Found {len(filtered_conversations)} selected conversations")
        
        # Log which conversations were found vs missing
        found_ids = [conv['conversation_id'] for conv in filtered_conversations]
        missing_ids = [conv_id for conv_id in self.selected_conversation_ids if conv_id not in found_ids]
        
        if missing_ids:
            self.logger.warning(f"Could not find {len(missing_ids)} conversations:")
            for missing_id in missing_ids:
                self.logger.warning(f"  - {missing_id}")
        
        return filtered_conversations
    
    def process_single_conversation(self, conversation):
        """Extract complete data from a selected conversation - NO TRUNCATION"""
        try:
            # Basic metadata
            conv_id = conversation.get('uuid', 'unknown')
            created_at = conversation.get('created_at', '')
            updated_at = conversation.get('updated_at', '')
            name = conversation.get('name', 'Untitled')
            
            # Extract messages
            chat_messages = conversation.get('chat_messages', [])
            
            if not chat_messages:
                self.logger.warning(f"No messages found in conversation {conv_id}")
                return None
            
            # Process messages - KEEP EVERYTHING
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
            
            # Create COMPLETE conversation text (no truncation)
            conversation_text = f"=== CONVERSATION: {name} ===\n"
            conversation_text += f"Created: {created_at}\n"
            conversation_text += f"Updated: {updated_at}\n\n"
            
            # Interleave messages chronologically
            full_conversation = []
            for message in chat_messages:
                sender = message.get('sender', 'unknown')
                content = message.get('text', '')
                timestamp = message.get('created_at', '')
                
                if sender == 'human':
                    full_conversation.append(f"USER [{timestamp}]:\n{content}\n")
                elif sender == 'assistant':
                    full_conversation.append(f"CLAUDE [{timestamp}]:\n{content}\n")
            
            conversation_text += "\n---\n".join(full_conversation)
            
            # Create a shorter preview for scanning
            conversation_preview = f"{name} | {len(user_messages)} user msgs, {len(claude_responses)} Claude responses"
            if user_messages:
                first_user_msg = user_messages[0][:200] + ("..." if len(user_messages[0]) > 200 else "")
                conversation_preview += f" | First message: {first_user_msg}"
            
            return {
                'conversation_id': conv_id,
                'date': created_at.split('T')[0] if 'T' in created_at else created_at,
                'updated_date': updated_at.split('T')[0] if 'T' in updated_at else updated_at,
                'title': name,
                'message_count': len(chat_messages),
                'user_message_count': len(user_messages),
                'claude_response_count': len(claude_responses),
                'total_characters': total_chars,
                'conversation_preview': conversation_preview,
                'conversation_text': conversation_text
            }
        
        except Exception as e:
            self.logger.warning(f"Error processing conversation {conversation.get('uuid', 'unknown')}: {e}")
            return None
    
    def save_conversations_csv(self, conversations):
        """Save selected conversations to CSV"""
        if not conversations:
            self.logger.warning("No conversations to save")
            return
        
        # Create output filename
        filename = f"selected_conversations_{len(conversations)}_items.csv"
        filepath = self.output_dir / filename
        
        # Create DataFrame and save
        df = pd.DataFrame(conversations)
        df.to_csv(filepath, index=False)
        
        file_size = filepath.stat().st_size / (1024 * 1024)
        self.logger.info(f"Created: {filename} ({file_size:.1f} MB, {len(conversations)} conversations)")
        
        # Log summary of what was saved
        total_chars = sum(conv['total_characters'] for conv in conversations)
        avg_chars = total_chars // len(conversations) if conversations else 0
        
        self.logger.info(f"Summary:")
        self.logger.info(f"  - Total characters: {total_chars:,}")
        self.logger.info(f"  - Average per conversation: {avg_chars:,} characters")
        self.logger.info(f"  - Date range: {min(conv['date'] for conv in conversations)} to {max(conv['date'] for conv in conversations)}")
    
    def load_projects_info(self):
        """Load and process projects.json for reference"""
        projects_file = self.export_dir / "projects.json"
        
        if not projects_file.exists():
            self.logger.warning(f"Projects file not found: {projects_file}")
            return {}
        
        try:
            with open(projects_file, 'r') as file:
                projects_data = json.load(file)
            
            self.logger.info(f"Loaded projects data: {len(projects_data)} projects")
            
            # Save simplified project mapping for reference
            project_mapping = {
                'total_projects': len(projects_data),
                'project_names': [p.get('name', 'Unnamed') for p in projects_data],
                'processed_date': datetime.now().isoformat(),
                'selected_conversations_count': len(self.selected_conversation_ids),
                'selected_conversation_ids': self.selected_conversation_ids
            }
            
            mapping_file = self.config_dir / "project_mapping.json"
            with open(mapping_file, 'w') as file:
                json.dump(project_mapping, file, indent=2)
            
            return projects_data
        
        except Exception as e:
            self.logger.error(f"Error loading projects file: {e}")
            return {}
    
    def run(self):
        """Main execution method - process selected conversations only"""
        self.logger.info("Starting selected conversation processing...")
        self.logger.info(f"Target conversations: {len(self.selected_conversation_ids)}")
        
        # Load projects info
        projects = self.load_projects_info()
        
        # Parse selected conversations
        conversations = self.parse_conversations_file()
        
        if not conversations:
            self.logger.error("No selected conversations found to process")
            self.logger.info("Check that your conversation IDs are correct and exist in the JSON file")
            return
        
        # Save conversations to CSV
        self.save_conversations_csv(conversations)
        
        # Summary
        self.logger.info(f"Processing complete!")
        self.logger.info(f"Found {len(conversations)} out of {len(self.selected_conversation_ids)} requested conversations")
        self.logger.info("Next step: Upload the CSV file to Claude for analysis and project classification")

if __name__ == "__main__":
    parser = HistoricalParser()
    parser.run()