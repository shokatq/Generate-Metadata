import os
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import asyncio
from io import BytesIO
import threading
from functools import wraps
import time
import re
from collections import defaultdict
import uuid

# File processing libraries
import PyPDF2
import docx
import openpyxl
from pptx import Presentation
import pandas as pd

# Azure libraries
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from openai import AzureOpenAI

# Flask
from flask import Flask, request, jsonify
from werkzeug.exceptions import BadRequest, NotFound, InternalServerError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileMetadataGenerator:
    def __init__(self):
        # Azure Blob Storage
        self.blob_service_client = BlobServiceClient.from_connection_string(
            os.getenv('AZURE_STORAGE_CONNECTION_STRING_1')
        )
        self.container_name = "weezyaifiles"
        
        # Azure Cosmos DB
        self.cosmos_client = CosmosClient(
            url=os.getenv('COSMOS_ENDPOINT'),
            credential=os.getenv('COSMOS_KEY')
        )
        self.database = self.cosmos_client.get_database_client('weezyai')
        self.container = self.database.get_container_client('files')
        
        # Azure OpenAI for embeddings
        self.openai_embedding_client = AzureOpenAI(
            api_key=os.getenv('OPENAI_API_KEY'),
            api_version="2024-12-01-preview",
            azure_endpoint="https://weez-openai-resource.openai.azure.com/"
        )
        
        # Azure OpenAI for text generation (GPT-4o)
        self.openai_text_client = AzureOpenAI(
            api_key=os.getenv('OPENAI_API_KEY'),
            api_version="2024-12-01-preview",
            azure_endpoint="https://weez-openai-resource.openai.azure.com/"
        )
        
        # Supported file types
        self.supported_extensions = {
            '.pdf': 'application/pdf',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.doc': 'application/msword',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.xls': 'application/vnd.ms-excel',
            '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            '.ppt': 'application/vnd.ms-powerpoint',
            '.txt': 'text/plain'
        }
        
        # Fields that indicate new metadata format
        self.new_metadata_fields = ['textSummary', 'embedding', 'processed_at']
        
        # Chunk size for embeddings (optimize for text-embedding-3-large)
        self.chunk_size = 1000  # Characters per chunk
        self.chunk_overlap = 200  # Overlap between chunks

    def extract_departments_from_path(self, file_path: str) -> List[str]:
        """Extract departments from file path based on directory structure"""
        departments = []
        
        # Normalize path separators
        path = file_path.replace('\\', '/').lower()
        
        # Define department patterns and keywords
        department_patterns = {
            'marketing': ['marketing', 'brand', 'advertising', 'promotion', 'campaign'],
            'engineering': ['engineering', 'development', 'dev', 'tech', 'software', 'backend', 'frontend'],
            'sales': ['sales', 'revenue', 'deals', 'prospects', 'crm'],
            'hr': ['hr', 'human-resources', 'people', 'talent', 'recruitment', 'hiring'],
            'finance': ['finance', 'accounting', 'budget', 'financial', 'accounting'],
            'operations': ['operations', 'ops', 'logistics', 'supply-chain'],
            'legal': ['legal', 'contracts', 'compliance', 'regulatory'],
            'product': ['product', 'pm', 'product-management', 'roadmap'],
            'design': ['design', 'ui', 'ux', 'creative', 'graphics'],
            'data': ['data', 'analytics', 'data-science', 'bi', 'reporting'],
            'security': ['security', 'infosec', 'cybersecurity', 'privacy'],
            'support': ['support', 'customer-service', 'help-desk', 'customer-success']
        }
        
        # Check for cross-functional patterns
        cross_functional_patterns = [
            'company-wide', 'all-hands', 'cross-functional', 'multi-department',
            'organization', 'company', 'global', 'enterprise'
        ]
        
        # Split path into segments
        path_segments = [seg.strip() for seg in path.split('/') if seg.strip()]
        
        # Check for cross-functional indicators
        is_cross_functional = any(pattern in path for pattern in cross_functional_patterns)
        
        if is_cross_functional:
            # For cross-functional files, try to identify specific departments mentioned
            for dept, keywords in department_patterns.items():
                if any(keyword in path for keyword in keywords):
                    departments.append(dept)
            
            # If no specific departments found in cross-functional, mark as company-wide
            if not departments:
                departments = ['company-wide']
        else:
            # Regular department detection
            for dept, keywords in department_patterns.items():
                if any(keyword in path for keyword in keywords):
                    departments.append(dept)
        
        # If no departments detected, try to infer from common folder structures
        if not departments:
            for segment in path_segments:
                for dept, keywords in department_patterns.items():
                    if any(keyword in segment for keyword in keywords):
                        departments.append(dept)
                        break
        
        # Default fallback
        if not departments:
            departments = ['general']
        
        return list(set(departments))  # Remove duplicates

    def determine_visibility(self, file_path: str, platform: str, shared_with: List[str] = None, created_by: List[str] = None) -> str:
        """Determine file visibility based on path, platform, and sharing info"""
        path_lower = file_path.lower()
        
        # Public indicators
        public_indicators = ['public', 'open', 'everyone', 'all-access', 'external']
        if any(indicator in path_lower for indicator in public_indicators):
            return 'public'
        
        # Private indicators
        private_indicators = ['private', 'personal', 'confidential', 'restricted']
        if any(indicator in path_lower for indicator in private_indicators):
            return 'private'
        
        # Department-specific indicators
        department_indicators = ['department', 'team', 'group', 'unit']
        if any(indicator in path_lower for indicator in department_indicators):
            return 'department'
        
        # Check sharing information
        if shared_with and len(shared_with) > 0:
            if len(shared_with) > 10:  # Shared with many people
                return 'internal'
            else:
                return 'department'
        
        # Platform-based defaults
        platform_defaults = {
            'google_drive': 'internal',
            'onedrive': 'internal', 
            'dropbox': 'department',
            'notion': 'internal',
            'platform_sync': 'internal'
        }
        
        return platform_defaults.get(platform, 'internal')

    def generate_sas_url(self, blob_name: str) -> Optional[str]:
        """Generate SAS URL for blob with 1-year expiration"""
        try:
            from azure.storage.blob import generate_blob_sas, BlobSasPermissions
            from datetime import timedelta
            
            # Extract account name and key from connection string
            conn_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING_1')
            conn_parts = dict(item.split('=', 1) for item in conn_string.split(';') if '=' in item)
            account_name = conn_parts.get('AccountName')
            account_key = conn_parts.get('AccountKey')
            
            if not account_name or not account_key:
                logger.error("Could not extract account credentials from connection string")
                return None
            
            # Generate SAS token with 1 year expiration
            sas_token = generate_blob_sas(
                account_name=account_name,
                container_name=self.container_name,
                blob_name=blob_name,
                account_key=account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(days=365)  # 1 year
            )
            
            # Construct full SAS URL
            sas_url = f"https://{account_name}.blob.core.windows.net/{self.container_name}/{blob_name}?{sas_token}"
            return sas_url
            
        except Exception as e:
            logger.error(f"Error generating SAS URL: {e}")
            return None

    def is_metadata_updated(self, metadata: Dict[str, Any]) -> bool:
        """Check if metadata has been updated with new fields"""
        # Check if any of the new metadata fields are present and not empty
        for field in self.new_metadata_fields:
            if field in metadata and metadata[field]:
                return True
        return False

    def get_all_old_metadata(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all metadata records that need updating"""
        try:
            if user_id:
                # Query for specific user's old metadata
                query = "SELECT * FROM c WHERE c.user_id = @user_id"
                parameters = [{"name": "@user_id", "value": user_id}]
            else:
                # Query for all old metadata
                query = "SELECT * FROM c"
                parameters = []
            
            items = list(self.container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            # Filter out items that already have updated metadata
            old_metadata_items = []
            for item in items:
                if not self.is_metadata_updated(item):
                    old_metadata_items.append(item)
            
            logger.info(f"Found {len(old_metadata_items)} items that need metadata updates")
            return old_metadata_items
        
        except Exception as e:
            logger.error(f"Error retrieving old metadata: {str(e)}")
            return []

    def download_file_from_blob(self, file_path: str) -> bytes:
        """Download file from Azure Blob Storage with better error handling"""
        try:
            logger.info(f"Attempting to download file from blob: {file_path}")
            
            # Clean the file path - remove any leading slashes
            cleaned_path = file_path.lstrip('/')
            logger.info(f"Cleaned file path: {cleaned_path}")
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=cleaned_path
            )
            
            # Check if blob exists first
            if not blob_client.exists():
                raise Exception(f"Blob does not exist: {cleaned_path}")
            
            # Get blob properties for debugging
            blob_properties = blob_client.get_blob_properties()
            logger.info(f"Blob properties - Size: {blob_properties.size}, Content-Type: {blob_properties.content_settings.content_type}")
            
            blob_data = blob_client.download_blob()
            content = blob_data.readall()
            
            logger.info(f"Successfully downloaded {len(content)} bytes from blob")
            return content
        
        except Exception as e:
            logger.error(f"Error downloading file from blob: {str(e)}")
            logger.error(f"Container: {self.container_name}, Blob: {file_path}")
            raise Exception(f"Failed to download file from blob storage: {str(e)}")

    def clean_text(self, text: str) -> str:
        """Clean and normalize text for better processing"""
        # Remove excessive whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove special characters that might interfere with processing
        text = re.sub(r'[^\w\s\.,!?;:()\-\'"\/]', ' ', text)
        
        # Normalize quotes
        text = re.sub(r'["""]', '"', text)
        text = re.sub(r"[''']", "'", text)

        return text

    def create_text_chunks(self, text: str, metadata: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Create overlapping text chunks for better embedding accuracy"""
        chunks = []
        text = self.clean_text(text)
        
        # If text is short, return as single chunk
        if len(text) <= self.chunk_size:
            chunks.append({
                'text': text,
                'chunk_index': 0,
                'start_char': 0,
                'end_char': len(text),
                'metadata': metadata or {}
            })
            return chunks
        
        # Create overlapping chunks
        start = 0
        chunk_index = 0
        
        while start < len(text):
            end = min(start + self.chunk_size, len(text))
            
            # Try to break at sentence boundaries
            if end < len(text):
                # Look for sentence endings within overlap range
                sentence_end = max(
                    text.rfind('.', start, end),
                    text.rfind('!', start, end),
                    text.rfind('?', start, end)
                )
                
                if sentence_end > start:
                    end = sentence_end + 1
            
            chunk_text = text[start:end].strip()
            
            if chunk_text:
                chunks.append({
                    'text': chunk_text,
                    'chunk_index': chunk_index,
                    'start_char': start,
                    'end_char': end,
                    'metadata': metadata or {}
                })
                chunk_index += 1
            
            # Move start position with overlap
            start = max(start + self.chunk_size - self.chunk_overlap, end)
            
            if start >= len(text):
                break
        
        logger.info(f"Created {len(chunks)} text chunks")
        return chunks

    def extract_text_from_pdf(self, file_content: bytes) -> str:
        """Extract text from PDF file with improved structure preservation"""
        try:
            pdf_file = BytesIO(file_content)
            reader = PyPDF2.PdfReader(pdf_file)
            text = ""
            
            for page_num, page in enumerate(reader.pages):
                page_text = page.extract_text()
                # Add page break indicators
                text += f"\n--- Page {page_num + 1} ---\n"
                text += page_text + "\n"
            
            return self.clean_text(text)
        
        except Exception as e:
            logger.error(f"Error extracting text from PDF: {str(e)}")
            return ""

    def extract_text_from_docx(self, file_content: bytes) -> str:
        """Extract text from DOCX file with structure preservation"""
        try:
            doc_file = BytesIO(file_content)
            doc = docx.Document(doc_file)
            text = ""
            
            for para in doc.paragraphs:
                if para.text.strip():
                    # Preserve paragraph structure
                    text += para.text + "\n\n"
            
            # Extract table content
            for table in doc.tables:
                text += "\n--- Table ---\n"
                for row in table.rows:
                    row_text = " | ".join([cell.text.strip() for cell in row.cells])
                    text += row_text + "\n"
                text += "--- End Table ---\n\n"
            
            return self.clean_text(text)
        
        except Exception as e:
            logger.error(f"Error extracting text from DOCX: {str(e)}")
            return ""

    def extract_text_from_xlsx(self, file_content: bytes) -> str:
        """Extract text from XLSX file with improved structure"""
        try:
            excel_file = BytesIO(file_content)
            workbook = openpyxl.load_workbook(excel_file, data_only=True)
            text = ""
            
            for sheet_name in workbook.sheetnames:
                sheet = workbook[sheet_name]
                text += f"\n--- Sheet: {sheet_name} ---\n"
                
                # Get headers from first row
                headers = []
                first_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True), None)
                if first_row:
                    headers = [str(cell) if cell is not None else "" for cell in first_row]
                    text += "Headers: " + " | ".join(headers) + "\n"
                
                # Extract data rows
                for row_num, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), 2):
                    if any(cell is not None for cell in row):
                        row_text = " | ".join([str(cell) if cell is not None else "" for cell in row])
                        text += f"Row {row_num}: {row_text}\n"
                
                text += f"--- End Sheet: {sheet_name} ---\n\n"
            
            return self.clean_text(text)
        
        except Exception as e:
            logger.error(f"Error extracting text from XLSX: {str(e)}")
            return ""

    def extract_text_from_pptx(self, file_content: bytes) -> str:
        """Extract text from PPTX file with slide structure"""
        try:
            ppt_file = BytesIO(file_content)
            presentation = Presentation(ppt_file)
            text = ""
            
            for slide_num, slide in enumerate(presentation.slides, 1):
                text += f"\n--- Slide {slide_num} ---\n"
                
                for shape in slide.shapes:
                    if hasattr(shape, "text") and shape.text.strip():
                        text += shape.text + "\n"
                
                text += f"--- End Slide {slide_num} ---\n\n"
            
            return self.clean_text(text)
        
        except Exception as e:
            logger.error(f"Error extracting text from PPTX: {str(e)}")
            return ""

    def extract_text_from_txt(self, file_content: bytes) -> str:
        """Extract text from TXT file"""
        try:
            # Try different encodings
            encodings = ['utf-8', 'utf-16', 'latin-1', 'cp1252']
            
            for encoding in encodings:
                try:
                    text = file_content.decode(encoding)
                    logger.info(f"Successfully decoded text file using {encoding} encoding")
                    return self.clean_text(text)
                except UnicodeDecodeError:
                    continue
            
            # If all encodings fail, use utf-8 with errors='ignore'
            text = file_content.decode('utf-8', errors='ignore')
            logger.warning("Used utf-8 with errors='ignore' for text file")
            return self.clean_text(text)
        
        except Exception as e:
            logger.error(f"Error extracting text from TXT: {str(e)}")
            return ""

    def extract_text_from_file(self, file_content: bytes, file_extension: str) -> str:
        """Extract text based on file extension"""
        extraction_methods = {
            '.pdf': self.extract_text_from_pdf,
            '.docx': self.extract_text_from_docx,
            '.doc': self.extract_text_from_docx,
            '.xlsx': self.extract_text_from_xlsx,
            '.xls': self.extract_text_from_xlsx,
            '.pptx': self.extract_text_from_pptx,
            '.ppt': self.extract_text_from_pptx,
            '.txt': self.extract_text_from_txt
        }
        
        method = extraction_methods.get(file_extension.lower())
        if method:
            return method(file_content)
        else:
            logger.warning(f"Unsupported file extension: {file_extension}")
            return ""

    def generate_chunk_embeddings(self, text_chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate embeddings for text chunks"""
        try:
            chunk_embeddings = []
            
            for chunk in text_chunks:
                # Generate embedding for chunk
                response = self.openai_embedding_client.embeddings.create(
                    model="text-embedding-3-large",
                    input=chunk['text'],
                    encoding_format="float"
                )
                
                embedding_data = {
                    'chunk_index': chunk['chunk_index'],
                    'text': chunk['text'],
                    'embedding': response.data[0].embedding,
                    'start_char': chunk['start_char'],
                    'end_char': chunk['end_char'],
                    'text_length': len(chunk['text']),
                    'metadata': chunk['metadata']
                }
                
                chunk_embeddings.append(embedding_data)
            
            logger.info(f"Generated embeddings for {len(chunk_embeddings)} chunks")
            return chunk_embeddings
        
        except Exception as e:
            logger.error(f"Error generating chunk embeddings: {str(e)}")
            return []

    def store_chunk_documents(self, file_id: str, file_name: str, user_id: str, 
                             chunk_embeddings: List[Dict[str, Any]], 
                             base_metadata: Dict[str, Any]) -> int:
        """Store each chunk as a separate document in Cosmos DB with RBAC fields"""
        try:
            stored_count = 0
            current_time = datetime.utcnow().isoformat() + 'Z'
            
            # Extract base metadata fields
            created_at = base_metadata.get('created_at', current_time)
            platform = base_metadata.get('platform', 'platform_sync')
            mime_type = base_metadata.get('mime_type', 'unknown')
            file_path = base_metadata.get('filePath', '')
            
            # Extract RBAC fields from base metadata
            departments = base_metadata.get('department', self.extract_departments_from_path(file_path))
            shared_with = base_metadata.get('shared_with', [])
            created_by = base_metadata.get('created_by', [user_id])
            visibility = base_metadata.get('visibility', self.determine_visibility(file_path, platform, shared_with, created_by))
            platform_metadata = base_metadata.get('platform_metadata', {})
            
            # Generate SAS URL if not present
            sas_url = base_metadata.get('sas_url')
            if not sas_url and file_path:
                sas_url = self.generate_sas_url(file_path)
            
            for chunk_data in chunk_embeddings:
                # Create unique chunk ID
                chunk_id = f"{file_id}-chunk-{chunk_data['chunk_index']}"
                
                # Create chunk document with RBAC fields
                chunk_document = {
                    'id': chunk_id,
                    'chunk_index': chunk_data['chunk_index'],
                    'text': chunk_data['text'],
                    'embedding': chunk_data['embedding'],
                    'file_id': file_id,
                    'fileName': file_name,
                    'user_id': user_id,
                    'platform': platform,
                    'created_at': created_at,
                    'mime_type': mime_type,
                    'processed_at': current_time,
                    'source': 'platform_sync',
                    'summary_type': None,
                    
                    # RBAC fields
                    'sas_url': sas_url,
                    'department': departments if isinstance(departments, list) else [departments],
                    'shared_with': shared_with if isinstance(shared_with, list) else ([shared_with] if shared_with else []),
                    'created_by': created_by if isinstance(created_by, list) else ([created_by] if created_by else [user_id]),
                    'visibility': visibility,
                    
                    # Platform-specific metadata
                    'platform_metadata': platform_metadata,
                    
                    # Chunk-specific metadata
                    'metadata': {
                        'start_char': chunk_data['start_char'],
                        'end_char': chunk_data['end_char'],
                        'text_length': chunk_data['text_length'],
                        'embedding_model': 'text-embedding-3-large',
                        'processing_version': '2.0'
                    }
                }
                
                # Store chunk document
                try:
                    self.container.upsert_item(chunk_document)
                    stored_count += 1
                    logger.info(f"Stored chunk {chunk_data['chunk_index']} for file {file_id}")
                    
                except Exception as e:
                    logger.error(f"Error storing chunk {chunk_data['chunk_index']} for file {file_id}: {str(e)}")
                    continue
            
            logger.info(f"Successfully stored {stored_count} chunks for file {file_id}")
            return stored_count
            
        except Exception as e:
            logger.error(f"Error storing chunk documents: {str(e)}")
            return 0

    def get_existing_metadata(self, file_id: str, user_id: str) -> Dict[str, Any]:
        """Retrieve existing metadata from Cosmos DB"""
        try:
            response = self.container.read_item(
                item=file_id,
                partition_key=user_id
            )
            return response
        except CosmosResourceNotFoundError:
            logger.error(f"Metadata not found for file ID: {file_id} with user ID: {user_id}")
            raise Exception(f"File metadata not found for ID: {file_id}")
        except Exception as e:
            logger.error(f"Error retrieving existing metadata: {str(e)}")
            raise Exception(f"Failed to retrieve metadata: {str(e)}")

    def process_single_file_metadata(self, metadata: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        """Process a single file's metadata and store chunks separately with RBAC support"""
        try:
            file_id = metadata.get('id')
            file_path = metadata.get('filePath')
            filename = metadata.get('fileName', '')
            user_id = metadata.get('user_id')
            platform = metadata.get('platform', 'platform_sync')
            
            logger.info(f"Processing file metadata for: {file_id}")
            
            # Check if file exists in blob storage
            try:
                file_content = self.download_file_from_blob(file_path)
                logger.info(f"Successfully downloaded file, size: {len(file_content)} bytes")
            except Exception as e:
                logger.warning(f"Could not download file {file_path}: {str(e)}")
                return False, f"File not accessible: {str(e)}", metadata
            
            # Determine file extension
            file_extension = os.path.splitext(filename)[-1].lower()
            
            if file_extension not in self.supported_extensions:
                logger.warning(f"Unsupported file type: {file_extension}")
                return False, f"Unsupported file type: {file_extension}", metadata
            
            # Extract text from file
            logger.info(f"Extracting text from file: {filename}")
            extracted_text = self.extract_text_from_file(file_content, file_extension)
            
            if not extracted_text.strip():
                logger.warning(f"No text extracted from file: {filename}")
                extracted_text = f"Content from {filename}"
            
            logger.info(f"Extracted text length: {len(extracted_text)} characters")
            
            # Create text chunks
            logger.info("Creating text chunks...")
            text_chunks = self.create_text_chunks(extracted_text, {
                'filename': filename,
                'file_id': file_id,
                'file_type': file_extension
            })
            
            # Generate chunk embeddings
            logger.info("Generating chunk embeddings...")
            chunk_embeddings = self.generate_chunk_embeddings(text_chunks)
            
            if not chunk_embeddings:
                logger.error("No embeddings generated for chunks")
                return False, "Failed to generate embeddings", metadata
            
            # Store chunks as separate documents with RBAC fields
            logger.info("Storing chunks as separate documents...")
            stored_count = self.store_chunk_documents(
                file_id=file_id,
                file_name=filename,
                user_id=user_id,
                chunk_embeddings=chunk_embeddings,
                base_metadata=metadata
            )
            
            if stored_count == 0:
                logger.error("No chunks were stored successfully")
                return False, "Failed to store chunks", metadata
            
            # Update original metadata to mark as processed with enhanced RBAC fields
            updated_metadata = metadata.copy()
            
            # Extract RBAC fields if not present
            if 'department' not in updated_metadata:
                updated_metadata['department'] = self.extract_departments_from_path(file_path)
            
            if 'visibility' not in updated_metadata:
                updated_metadata['visibility'] = self.determine_visibility(
                    file_path, 
                    platform, 
                    updated_metadata.get('shared_with', []),
                    updated_metadata.get('created_by', [user_id])
                )
            
            if 'sas_url' not in updated_metadata:
                updated_metadata['sas_url'] = self.generate_sas_url(file_path)
            
            # Ensure lists are properly formatted
            if 'shared_with' in updated_metadata and not isinstance(updated_metadata['shared_with'], list):
                updated_metadata['shared_with'] = [updated_metadata['shared_with']] if updated_metadata['shared_with'] else []
            
            if 'created_by' in updated_metadata and not isinstance(updated_metadata['created_by'], list):
                updated_metadata['created_by'] = [updated_metadata['created_by']] if updated_metadata['created_by'] else [user_id]
            elif 'created_by' not in updated_metadata:
                updated_metadata['created_by'] = [user_id]
            
            # Add processing metadata
            current_time = datetime.utcnow().isoformat() + 'Z'
            updated_metadata.update({
                'textSummary': f"Document contains {len(extracted_text)} characters across {len(chunk_embeddings)} chunks",
                'embedding': True,  # Flag to indicate embeddings are stored separately
                'processed_at': current_time,
                'chunk_count': len(chunk_embeddings),
                'processing_version': '2.0',
                'text_length': len(extracted_text)
            })
            
            # Update the original metadata document
            try:
                self.container.upsert_item(updated_metadata)
                logger.info(f"Successfully updated metadata for file: {file_id}")
            except Exception as e:
                logger.error(f"Error updating metadata: {str(e)}")
                return False, f"Failed to update metadata: {str(e)}", metadata
            
            logger.info(f"Successfully processed file {file_id} with {stored_count} chunks")
            return True, f"Successfully processed with {stored_count} chunks", updated_metadata
            
        except Exception as e:
            logger.error(f"Error processing file metadata: {str(e)}")
            return False, f"Error processing file: {str(e)}", metadata

    def generate_text_summary(self, text: str, filename: str) -> str:
        """Generate a comprehensive text summary using GPT-4o"""
        try:
            # Truncate text if too long (GPT-4o context limit consideration)
            max_chars = 12000  # Conservative limit
            if len(text) > max_chars:
                text = text[:max_chars] + "... [truncated]"
            
            prompt = f"""
            Please provide a comprehensive summary of the following document titled "{filename}".
            
            Include:
            1. Main purpose and content overview
            2. Key topics and themes discussed
            3. Important data, figures, or findings (if any)
            4. Document structure and organization
            5. Target audience or use case
            
            Document content:
            {text}
            
            Summary:
            """
            
            response = self.openai_text_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an expert document analyst. Provide clear, concise, and informative summaries."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=500,
                temperature=0.3
            )
            
            summary = response.choices[0].message.content.strip()
            logger.info(f"Generated summary for {filename}: {len(summary)} characters")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating text summary: {str(e)}")
            return f"Summary could not be generated for {filename}"

    def batch_process_metadata(self, user_id: Optional[str] = None, batch_size: int = 10) -> Dict[str, Any]:
        """Process metadata in batches with improved error handling and RBAC support"""
        try:
            logger.info(f"Starting batch metadata processing for user: {user_id or 'all users'}")
            
            # Get all old metadata that needs updating
            old_metadata_items = self.get_all_old_metadata(user_id)
            
            if not old_metadata_items:
                logger.info("No metadata items found that need updating")
                return {
                    'status': 'success',
                    'total_items': 0,
                    'processed': 0,
                    'successful': 0,
                    'failed': 0,
                    'results': []
                }
            
            total_items = len(old_metadata_items)
            logger.info(f"Found {total_items} items to process")
            
            results = {
                'status': 'success',
                'total_items': total_items,
                'processed': 0,
                'successful': 0,
                'failed': 0,
                'results': []
            }
            
            # Process in batches
            for i in range(0, total_items, batch_size):
                batch = old_metadata_items[i:i + batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}/{(total_items + batch_size - 1)//batch_size}")
                
                for metadata in batch:
                    try:
                        file_id = metadata.get('id', 'unknown')
                        filename = metadata.get('fileName', 'unknown')
                        
                        logger.info(f"Processing file: {filename} (ID: {file_id})")
                        
                        success, message, updated_metadata = self.process_single_file_metadata(metadata)
                        
                        result = {
                            'file_id': file_id,
                            'filename': filename,
                            'success': success,
                            'message': message
                        }
                        
                        results['results'].append(result)
                        results['processed'] += 1
                        
                        if success:
                            results['successful'] += 1
                            logger.info(f"✓ Successfully processed: {filename}")
                        else:
                            results['failed'] += 1
                            logger.error(f"✗ Failed to process: {filename} - {message}")
                    
                    except Exception as e:
                        results['processed'] += 1
                        results['failed'] += 1
                        error_message = f"Unexpected error: {str(e)}"
                        
                        results['results'].append({
                            'file_id': metadata.get('id', 'unknown'),
                            'filename': metadata.get('fileName', 'unknown'),
                            'success': False,
                            'message': error_message
                        })
                        
                        logger.error(f"✗ Unexpected error processing {metadata.get('fileName', 'unknown')}: {str(e)}")
                
                # Small delay between batches to avoid overwhelming services
                if i + batch_size < total_items:
                    time.sleep(1)
            
            # Update overall status based on results
            if results['failed'] == 0:
                results['status'] = 'success'
            elif results['successful'] > 0:
                results['status'] = 'partial_success'
            else:
                results['status'] = 'failed'
            
            logger.info(f"Batch processing completed. Successful: {results['successful']}, Failed: {results['failed']}")
            return results
            
        except Exception as e:
            logger.error(f"Error in batch processing: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'total_items': 0,
                'processed': 0,
                'successful': 0,
                'failed': 0,
                'results': []
            }

    def delete_chunk_documents(self, file_id: str, user_id: str) -> bool:
        """Delete all chunk documents for a specific file"""
        try:
            # Query for all chunks belonging to this file
            query = "SELECT c.id FROM c WHERE c.file_id = @file_id AND c.user_id = @user_id"
            parameters = [
                {"name": "@file_id", "value": file_id},
                {"name": "@user_id", "value": user_id}
            ]
            
            chunk_items = list(self.container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            deleted_count = 0
            for chunk in chunk_items:
                try:
                    self.container.delete_item(item=chunk['id'], partition_key=user_id)
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"Error deleting chunk {chunk['id']}: {str(e)}")
            
            logger.info(f"Deleted {deleted_count} chunk documents for file {file_id}")
            return deleted_count > 0
            
        except Exception as e:
            logger.error(f"Error deleting chunk documents: {str(e)}")
            return False

    def reprocess_file_metadata(self, file_id: str, user_id: str) -> Tuple[bool, str, Dict[str, Any]]:
        """Reprocess a specific file's metadata, replacing existing chunks"""
        try:
            logger.info(f"Reprocessing file metadata for: {file_id}")
            
            # Get existing metadata
            existing_metadata = self.get_existing_metadata(file_id, user_id)
            
            # Delete existing chunk documents
            self.delete_chunk_documents(file_id, user_id)
            
            # Reset processing flags in metadata
            reset_metadata = existing_metadata.copy()
            reset_metadata.pop('textSummary', None)
            reset_metadata.pop('embedding', None)
            reset_metadata.pop('processed_at', None)
            reset_metadata.pop('chunk_count', None)
            reset_metadata.pop('text_length', None)
            
            # Process the file again
            return self.process_single_file_metadata(reset_metadata)
            
        except Exception as e:
            logger.error(f"Error reprocessing file metadata: {str(e)}")
            return False, f"Error reprocessing file: {str(e)}", {}

    def get_processing_stats(self, user_id: Optional[str] = None) -> Dict[str, Any]:
        """Get statistics about processed vs unprocessed files"""
        try:
            if user_id:
                query = "SELECT * FROM c WHERE c.user_id = @user_id"
                parameters = [{"name": "@user_id", "value": user_id}]
            else:
                query = "SELECT * FROM c"
                parameters = []
            
            items = list(self.container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            total_files = len(items)
            processed_files = 0
            unprocessed_files = 0
            chunk_documents = 0
            
            for item in items:
                if self.is_metadata_updated(item):
                    processed_files += 1
                else:
                    unprocessed_files += 1
                
                # Count chunk documents (they have chunk_index field)
                if 'chunk_index' in item:
                    chunk_documents += 1
            
            return {
                'total_files': total_files,
                'processed_files': processed_files,
                'unprocessed_files': unprocessed_files,
                'chunk_documents': chunk_documents,
                'processing_percentage': (processed_files / total_files * 100) if total_files > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error getting processing stats: {str(e)}")
            return {
                'error': str(e),
                'total_files': 0,
                'processed_files': 0,
                'unprocessed_files': 0,
                'chunk_documents': 0,
                'processing_percentage': 0
            }


# Flask application
app = Flask(__name__)

# Initialize the metadata generator
metadata_generator = FileMetadataGenerator()

def handle_exceptions(f):
    """Decorator to handle exceptions in Flask routes"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except BadRequest as e:
            logger.error(f"Bad request error: {str(e)}")
            return jsonify({'error': 'Bad request', 'message': str(e)}), 400
        except NotFound as e:
            logger.error(f"Not found error: {str(e)}")
            return jsonify({'error': 'Not found', 'message': str(e)}), 404
        except Exception as e:
            logger.error(f"Internal server error: {str(e)}")
            return jsonify({'error': 'Internal server error', 'message': str(e)}), 500
    return decorated_function

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'service': 'file-metadata-generator'
    }), 200

@app.route('/process-metadata', methods=['POST'])
@handle_exceptions
def process_metadata():
    """Process metadata for files"""
    try:
        data = request.get_json()
        
        if not data:
            raise BadRequest("No JSON data provided")
        
        user_id = data.get('user_id')
        batch_size = data.get('batch_size', 10)
        
        if not user_id:
            raise BadRequest("user_id is required")
        
        logger.info(f"Starting metadata processing for user: {user_id}")
        
        # Process metadata in batches
        results = metadata_generator.batch_process_metadata(user_id, batch_size)
        
        return jsonify(results), 200
        
    except Exception as e:
        logger.error(f"Error in process_metadata endpoint: {str(e)}")
        raise

@app.route('/process-single-file', methods=['POST'])
@handle_exceptions
def process_single_file():
    """Process metadata for a single file"""
    try:
        data = request.get_json()
        
        if not data:
            raise BadRequest("No JSON data provided")
        
        file_id = data.get('file_id')
        user_id = data.get('user_id')
        
        if not file_id or not user_id:
            raise BadRequest("file_id and user_id are required")
        
        logger.info(f"Processing single file: {file_id} for user: {user_id}")
        
        # Get existing metadata
        existing_metadata = metadata_generator.get_existing_metadata(file_id, user_id)
        
        # Process the file
        success, message, updated_metadata = metadata_generator.process_single_file_metadata(existing_metadata)
        
        return jsonify({
            'success': success,
            'message': message,
            'file_id': file_id,
            'metadata': updated_metadata if success else None
        }), 200 if success else 400
        
    except Exception as e:
        logger.error(f"Error in process_single_file endpoint: {str(e)}")
        raise

@app.route('/reprocess-file', methods=['POST'])
@handle_exceptions
def reprocess_file():
    """Reprocess metadata for a specific file"""
    try:
        data = request.get_json()
        
        if not data:
            raise BadRequest("No JSON data provided")
        
        file_id = data.get('file_id')
        user_id = data.get('user_id')
        
        if not file_id or not user_id:
            raise BadRequest("file_id and user_id are required")
        
        logger.info(f"Reprocessing file: {file_id} for user: {user_id}")
        
        # Reprocess the file
        success, message, updated_metadata = metadata_generator.reprocess_file_metadata(file_id, user_id)
        
        return jsonify({
            'success': success,
            'message': message,
            'file_id': file_id,
            'metadata': updated_metadata if success else None
        }), 200 if success else 400
        
    except Exception as e:
        logger.error(f"Error in reprocess_file endpoint: {str(e)}")
        raise

@app.route('/processing-stats', methods=['GET'])
@handle_exceptions
def get_processing_stats():
    """Get processing statistics"""
    try:
        user_id = request.args.get('user_id')
        
        logger.info(f"Getting processing stats for user: {user_id or 'all users'}")
        
        stats = metadata_generator.get_processing_stats(user_id)
        
        return jsonify(stats), 200
        
    except Exception as e:
        logger.error(f"Error in get_processing_stats endpoint: {str(e)}")
        raise

@app.route('/delete-chunks', methods=['DELETE'])
@handle_exceptions
def delete_file_chunks():
    """Delete all chunks for a specific file"""
    try:
        data = request.get_json()
        
        if not data:
            raise BadRequest("No JSON data provided")
        
        file_id = data.get('file_id')
        user_id = data.get('user_id')
        
        if not file_id or not user_id:
            raise BadRequest("file_id and user_id are required")
        
        logger.info(f"Deleting chunks for file: {file_id} user: {user_id}")
        
        success = metadata_generator.delete_chunk_documents(file_id, user_id)
        
        return jsonify({
            'success': success,
            'message': f"Chunks {'deleted' if success else 'not found or failed to delete'}",
            'file_id': file_id
        }), 200 if success else 404
        
    except Exception as e:
        logger.error(f"Error in delete_file_chunks endpoint: {str(e)}")
        raise

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    # Run the Flask application
    port = int(os.getenv('PORT', 8000))
    debug = os.getenv('DEBUG', 'False').lower() == 'true'
    
    logger.info(f"Starting File Metadata Generator service on port {port}")
    logger.info(f"Debug mode: {debug}")
    
    app.run(host='0.0.0.0', port=port, debug=debug)
