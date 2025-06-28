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
        text = re.sub(r"[’‘']", "'", text)

        return text

    def extract_structured_content(self, text: str, file_extension: str) -> Dict[str, Any]:
        """Extract structured content including headers, lists, tables, etc."""
        content_structure = {
            'headings': [],
            'key_phrases': [],
            'lists': [],
            'tables': [],
            'metadata_tags': []
        }
        
        # Extract headings (common patterns)
        heading_patterns = [
            r'^([A-Z][A-Z\s]{10,})\s*$',  # ALL CAPS headings
            r'^\d+\.\s+([A-Z][a-zA-Z\s]{5,})\s*$',  # Numbered headings
            r'^([A-Z][a-zA-Z\s]{5,}):\s*$',  # Colon-terminated headings
        ]
        
        for pattern in heading_patterns:
            matches = re.findall(pattern, text, re.MULTILINE)
            content_structure['headings'].extend(matches)
        
        # Extract lists
        list_items = re.findall(r'^\s*[-•*]\s+(.+)$', text, re.MULTILINE)
        content_structure['lists'] = list_items[:20]  # Limit to first 20 items
        
        # Extract numbered lists
        numbered_items = re.findall(r'^\s*\d+\.\s+(.+)$', text, re.MULTILINE)
        content_structure['lists'].extend(numbered_items[:20])
        
        # Extract key phrases (noun phrases and important terms)
        key_phrase_patterns = [
            r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}\b',  # Proper nouns
            r'\b(?:important|key|critical|essential|main|primary|significant)\s+\w+\b',  # Important terms
        ]
        
        for pattern in key_phrase_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            content_structure['key_phrases'].extend(matches)
        
        # Remove duplicates and limit results
        content_structure['headings'] = list(set(content_structure['headings']))[:10]
        content_structure['key_phrases'] = list(set(content_structure['key_phrases']))[:20]
        
        return content_structure

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

    def generate_document_title(self, text_content: str, filename: str, content_structure: Dict[str, Any]) -> str:
        """Generate document title using GPT-4o with enhanced context"""
        try:
            # Use structured content for better title generation
            context_info = ""
            
            if content_structure['headings']:
                context_info += f"Main headings: {', '.join(content_structure['headings'][:5])}\n"
            
            if content_structure['key_phrases']:
                context_info += f"Key phrases: {', '.join(content_structure['key_phrases'][:10])}\n"
            
            # Use first 1500 characters for title generation
            text_preview = text_content[:1500] if len(text_content) > 1500 else text_content
            
            prompt = f"""
            Based on the following document content and structure, generate a clear, descriptive title.
            The title should be concise (max 12 words) and capture the main topic or purpose.

            Filename: {filename}
            
            Document Structure:
            {context_info}

            Content Preview:
            {text_preview}

            Generate a professional title that would help someone understand what this document contains.
            Return only the title, nothing else.
            """
            
            response = self.openai_text_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an expert at creating clear, professional document titles that accurately reflect content."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=60,
                temperature=0.2
            )
            
            title = response.choices[0].message.content.strip()
            # Remove quotes if present
            title = title.strip('"').strip("'")
            return title
        
        except Exception as e:
            logger.error(f"Error generating document title: {str(e)}")
            # Fallback to filename without extension
            return os.path.splitext(filename)[0]

    def generate_multi_level_summary(self, text_content: str, document_title: str, content_structure: Dict[str, Any]) -> Dict[str, str]:
        """Generate multiple summary levels for different use cases"""
        try:
            # Truncate text if too long
            max_chars = 12000
            if len(text_content) > max_chars:
                text_content = text_content[:max_chars] + "..."
            
            # Prepare context from structure
            structure_context = ""
            if content_structure['headings']:
                structure_context += f"Main sections: {', '.join(content_structure['headings'][:5])}\n"
            
            # Generate comprehensive summary
            comprehensive_prompt = f"""
            Analyze this document titled "{document_title}" and provide a comprehensive summary.
            
            Document Structure:
            {structure_context}
            
            Content:
            {text_content}
            
            Provide a detailed summary (3-4 sentences) that covers:
            1. Main purpose/topic
            2. Key points or findings
            3. Important details or conclusions
            4. Target audience or use case
            """
            
            # Generate brief summary
            brief_prompt = f"""
            Based on this document titled "{document_title}", provide a brief summary.
            
            Content:
            {text_content[:5000]}
            
            Provide a concise summary (1-2 sentences) that captures the core purpose and main point.
            """
            
            # Generate keyword summary
            keywords_prompt = f"""
            Extract the most important keywords and phrases from this document titled "{document_title}".
            
            Content:
            {text_content[:5000]}
            
            Return 10-15 key terms/phrases that best represent this document's content, separated by commas.
            """
            
            summaries = {}
            
            # Generate comprehensive summary
            response = self.openai_text_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an expert at creating detailed, accurate document summaries."},
                    {"role": "user", "content": comprehensive_prompt}
                ],
                max_tokens=250,
                temperature=0.3
            )
            summaries['comprehensive'] = response.choices[0].message.content.strip()
            
            # Generate brief summary
            response = self.openai_text_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an expert at creating concise document summaries."},
                    {"role": "user", "content": brief_prompt}
                ],
                max_tokens=100,
                temperature=0.3
            )
            summaries['brief'] = response.choices[0].message.content.strip()
            
            # Generate keywords
            response = self.openai_text_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an expert at extracting key terms from documents."},
                    {"role": "user", "content": keywords_prompt}
                ],
                max_tokens=100,
                temperature=0.2
            )
            summaries['keywords'] = response.choices[0].message.content.strip()
            
            return summaries
        
        except Exception as e:
            logger.error(f"Error generating multi-level summary: {str(e)}")
            return {
                'comprehensive': f"Document containing content related to {document_title}",
                'brief': f"Document about {document_title}",
                'keywords': document_title
            }

    def generate_chunk_embeddings(self, text_chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate embeddings for text chunks with enhanced metadata"""
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

    def generate_document_embedding(self, text_content: str, summaries: Dict[str, str]) -> List[float]:
        """Generate document-level embedding using comprehensive summary and key content"""
        try:
            # Combine summary and key content for document embedding
            embedding_text = f"{summaries['comprehensive']} {summaries['keywords']}"
            
            # Add key excerpts from the document
            if len(text_content) > 2000:
                # Take beginning and end portions
                beginning = text_content[:1000]
                ending = text_content[-1000:]
                embedding_text += f" {beginning} {ending}"
            else:
                embedding_text += f" {text_content}"
            
            # Truncate if too long
            if len(embedding_text) > 8000:
                embedding_text = embedding_text[:8000]
            
            response = self.openai_embedding_client.embeddings.create(
                model="text-embedding-3-large",
                input=embedding_text,
                encoding_format="float"
            )
            
            return response.data[0].embedding
        
        except Exception as e:
            logger.error(f"Error generating document embedding: {str(e)}")
            # Return zero vector on error
            return [0.0] * 3072

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

    def update_metadata_in_cosmos(self, updated_metadata: Dict[str, Any]) -> None:
        """Update metadata in Cosmos DB"""
        try:
            self.container.upsert_item(updated_metadata)
            logger.info(f"Successfully updated metadata for file ID: {updated_metadata['id']}")
        
        except Exception as e:
            logger.error(f"Error updating metadata in Cosmos DB: {str(e)}")
            raise Exception(f"Failed to update metadata: {str(e)}")

    def process_single_file_metadata(self, metadata: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        """Process a single file's metadata with enhanced RAG and summarization"""
        try:
            file_id = metadata.get('id')
            file_path = metadata.get('filePath')
            filename = metadata.get('fileName', '')
            
            logger.info(f"Processing enhanced metadata for: {file_id}")
            
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
            
            # Extract structured content
            logger.info("Extracting structured content...")
            content_structure = self.extract_structured_content(extracted_text, file_extension)
            
            # Create text chunks for embeddings
            logger.info("Creating text chunks for embeddings...")
            text_chunks = self.create_text_chunks(extracted_text, {
                'filename': filename,
                'file_id': file_id,
                'file_type': file_extension
            })
            
            # Generate document title
            logger.info("Generating enhanced document title...")
            document_title = self.generate_document_title(extracted_text, filename, content_structure)
            
            # Generate multi-level summaries
            logger.info("Generating multi-level summaries...")
            summaries = self.generate_multi_level_summary(extracted_text, document_title, content_structure)
            
            # Generate chunk embeddings
            logger.info("Generating chunk embeddings...")
            chunk_embeddings = self.generate_chunk_embeddings(text_chunks)
            
            # Generate document-level embedding
            logger.info("Generating document-level embedding...")
            document_embedding = self.generate_document_embedding(extracted_text, summaries)
            
            # Update metadata with enhanced fields
            updated_metadata = metadata.copy()
            updated_metadata.update({
                # Basic fields
                'document_title': document_title,
                'textSummary': summaries['comprehensive'],  # Keep for backward compatibility
                'embedding': document_embedding,  # Document-level embedding
                'processed_at': datetime.utcnow().isoformat() + 'Z',
                
                # Enhanced summarization
                'summaries': summaries,
                'content_structure': content_structure,
                
                # Enhanced RAG fields
                'chunk_embeddings': chunk_embeddings,
                'total_chunks': len(chunk_embeddings),
                'text_length': len(extracted_text),
                'chunk_size': self.chunk_size,
                'chunk_overlap': self.chunk_overlap,
                
                # Model information
                'embedding_model': 'text-embedding-3-large',
                'summary_model': 'gpt-4o',
                'title_model': 'gpt-4o',
                'processing_version': '2.0'
            })
            
            # Save updated metadata to Cosmos DB
            logger.info("Updating enhanced metadata in Cosmos DB...")
            self.update_metadata_in_cosmos(updated_metadata)
            
            return True, "Success", updated_metadata
        
        except Exception as e:
            logger.error(f"Error processing enhanced file metadata: {str(e)}")
            return False, str(e), metadata

   

    def process_file_metadata(self, file_id: str, user_id: str, file_path: str) -> Dict[str, Any]:
        """Process file metadata - enhanced implementation"""
        try:
            logger.info(f"Starting enhanced metadata processing for file_id: {file_id}")
            
            # Get existing metadata with user_id as partition key
            existing_metadata = self.get_existing_metadata(file_id, user_id)
            logger.info(f"Retrieved existing metadata for file: {file_id}")
            
            # Check if metadata is already updated
            if self.is_metadata_updated(existing_metadata):
                logger.info(f"Metadata already updated for file: {file_id}")
                return existing_metadata
            
            # Process the file with enhanced metadata
            success, message, updated_metadata = self.process_single_file_metadata(existing_metadata)
            
            if success:
                logger.info(f"Successfully processed enhanced metadata for file: {file_id}")
                return updated_metadata
            else:
                logger.error(f"Failed to process enhanced metadata for file: {file_id}, Error: {message}")
                raise Exception(f"Processing failed: {message}")
        
        except Exception as e:
            logger.error(f"Error in process_file_metadata: {str(e)}")
            raise Exception(f"Failed to process file metadata: {str(e)}")

    def bulk_update_metadata(self, user_id: Optional[str] = None, batch_size: int = 10) -> Dict[str, Any]:
        """Bulk update metadata for old records"""
        try:
            logger.info(f"Starting bulk metadata update for user: {user_id or 'all users'}")
            
            # Get all old metadata records
            old_metadata_items = self.get_all_old_metadata(user_id)
            
            if not old_metadata_items:
                logger.info("No items found that need metadata updates")
                return {
                    'total_items': 0,
                    'processed': 0,
                    'successful': 0,
                    'failed': 0,
                    'errors': []
                }
            
            # Process in batches
            total_items = len(old_metadata_items)
            processed = 0
            successful = 0
            failed = 0
            errors = []
            
            logger.info(f"Processing {total_items} items in batches of {batch_size}")
            
            for i in range(0, total_items, batch_size):
                batch = old_metadata_items[i:i + batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}/{(total_items + batch_size - 1)//batch_size}")
                
                for metadata in batch:
                    try:
                        file_id = metadata.get('id')
                        logger.info(f"Processing file: {file_id}")
                        
                        success, message, updated_metadata = self.process_single_file_metadata(metadata)
                        processed += 1
                        
                        if success:
                            successful += 1
                            logger.info(f"Successfully updated: {file_id}")
                        else:
                            failed += 1
                            error_msg = f"Failed to update {file_id}: {message}"
                            logger.error(error_msg)
                            errors.append(error_msg)
                    
                    except Exception as e:
                        processed += 1
                        failed += 1
                        error_msg = f"Exception processing {metadata.get('id', 'unknown')}: {str(e)}"
                        logger.error(error_msg)
                        errors.append(error_msg)
                
                # Add small delay between batches to avoid overwhelming services
                if i + batch_size < total_items:
                    time.sleep(1)
            
            result = {
                'total_items': total_items,
                'processed': processed,
                'successful': successful,
                'failed': failed,
                'errors': errors[:50]  # Limit errors to first 50
            }
            
            logger.info(f"Bulk update completed: {result}")
            return result
        
        except Exception as e:
            logger.error(f"Error in bulk metadata update: {str(e)}")
            raise Exception(f"Bulk update failed: {str(e)}")

    def search_similar_content(self, query_text: str, user_id: str, limit: int = 10, similarity_threshold: float = 0.7) -> List[Dict[str, Any]]:
        """Search for similar content using embeddings"""
        try:
            # Generate embedding for query
            response = self.openai_embedding_client.embeddings.create(
                model="text-embedding-3-large",
                input=query_text,
                encoding_format="float"
            )
            query_embedding = response.data[0].embedding
            
            # Query Cosmos DB for documents with embeddings
            query = """
                SELECT c.id, c.fileName, c.document_title, c.summaries, c.chunk_embeddings, c.embedding
                FROM c 
                WHERE c.user_id = @user_id 
                AND IS_DEFINED(c.embedding) 
                AND IS_DEFINED(c.chunk_embeddings)
            """
            
            parameters = [{"name": "@user_id", "value": user_id}]
            
            items = list(self.container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            # Calculate similarities
            results = []
            for item in items:
                # Document-level similarity
                doc_embedding = item.get('embedding', [])
                if doc_embedding:
                    doc_similarity = self.calculate_cosine_similarity(query_embedding, doc_embedding)
                    
                    # Chunk-level similarities
                    chunk_results = []
                    chunk_embeddings = item.get('chunk_embeddings', [])
                    
                    for chunk in chunk_embeddings:
                        chunk_embedding = chunk.get('embedding', [])
                        if chunk_embedding:
                            chunk_similarity = self.calculate_cosine_similarity(query_embedding, chunk_embedding)
                            
                            if chunk_similarity >= similarity_threshold:
                                chunk_results.append({
                                    'chunk_index': chunk.get('chunk_index'),
                                    'text': chunk.get('text', '')[:200] + '...',  # Truncate for response
                                    'similarity': chunk_similarity,
                                    'start_char': chunk.get('start_char'),
                                    'end_char': chunk.get('end_char')
                                })
                    
                    # Sort chunks by similarity
                    chunk_results.sort(key=lambda x: x['similarity'], reverse=True)
                    
                    if doc_similarity >= similarity_threshold or chunk_results:
                        results.append({
                            'file_id': item['id'],
                            'filename': item.get('fileName', ''),
                            'document_title': item.get('document_title', ''),
                            'document_similarity': doc_similarity,
                            'brief_summary': item.get('summaries', {}).get('brief', ''),
                            'matching_chunks': chunk_results[:5],  # Top 5 matching chunks
                            'total_matching_chunks': len(chunk_results)
                        })
            
            # Sort by document similarity
            results.sort(key=lambda x: x['document_similarity'], reverse=True)
            
            return results[:limit]
        
        except Exception as e:
            logger.error(f"Error in similarity search: {str(e)}")
            return []

    def calculate_cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        try:
            if len(vec1) != len(vec2):
                return 0.0
            
            dot_product = sum(a * b for a, b in zip(vec1, vec2))
            magnitude1 = sum(a * a for a in vec1) ** 0.5
            magnitude2 = sum(b * b for b in vec2) ** 0.5
            
            if magnitude1 == 0.0 or magnitude2 == 0.0:
                return 0.0
            
            return dot_product / (magnitude1 * magnitude2)
        
        except Exception as e:
            logger.error(f"Error calculating cosine similarity: {str(e)}")
            return 0.0

    def get_file_analytics(self, user_id: str) -> Dict[str, Any]:
        """Get analytics about processed files"""
        try:
            query = """
                SELECT 
                    COUNT(1) as total_files,
                    SUM(c.text_length) as total_text_length,
                    SUM(c.total_chunks) as total_chunks,
                    AVG(c.text_length) as avg_text_length,
                    AVG(c.total_chunks) as avg_chunks_per_file
                FROM c 
                WHERE c.user_id = @user_id 
                AND IS_DEFINED(c.processed_at)
            """
            
            parameters = [{"name": "@user_id", "value": user_id}]
            
            results = list(self.container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            if results:
                analytics = results[0]
                
                # Get file type distribution
                type_query = """
                    SELECT c.fileType, COUNT(1) as count
                    FROM c 
                    WHERE c.user_id = @user_id 
                    AND IS_DEFINED(c.processed_at)
                    GROUP BY c.fileType
                """
                
                type_results = list(self.container.query_items(
                    query=type_query,
                    parameters=parameters,
                    enable_cross_partition_query=True
                ))
                
                analytics['file_type_distribution'] = {item['fileType']: item['count'] for item in type_results}
                
                return analytics
            
            return {
                'total_files': 0,
                'total_text_length': 0,
                'total_chunks': 0,
                'avg_text_length': 0,
                'avg_chunks_per_file': 0,
                'file_type_distribution': {}
            }
        
        except Exception as e:
            logger.error(f"Error getting file analytics: {str(e)}")
            return {}


# Flask Application
app = Flask(__name__)

# Initialize the metadata generator
metadata_generator = FileMetadataGenerator()

def rate_limit_decorator(max_requests: int = 100, time_window: int = 3600):
    """Simple rate limiting decorator"""
    request_counts = defaultdict(list)
    
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            client_ip = request.remote_addr
            current_time = time.time()
            
            # Clean old requests
            request_counts[client_ip] = [
                req_time for req_time in request_counts[client_ip]
                if current_time - req_time < time_window
            ]
            
            # Check rate limit
            if len(request_counts[client_ip]) >= max_requests:
                return jsonify({'error': 'Rate limit exceeded'}), 429
            
            # Add current request
            request_counts[client_ip].append(current_time)
            
            return f(*args, **kwargs)
        return wrapper
    return decorator

@app.errorhandler(BadRequest)
def handle_bad_request(e):
    return jsonify({'error': 'Bad request', 'message': str(e)}), 400

@app.errorhandler(NotFound)
def handle_not_found(e):
    return jsonify({'error': 'Not found', 'message': str(e)}), 404

@app.errorhandler(InternalServerError)
def handle_internal_error(e):
    return jsonify({'error': 'Internal server error', 'message': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()}), 200

@app.route('/process-metadata', methods=['POST'])
@rate_limit_decorator(max_requests=50, time_window=3600)
def process_metadata():
    """Process file metadata endpoint"""
    try:
        data = request.get_json()
        
        if not data:
            raise BadRequest("No JSON data provided")
        
        file_id = data.get('fileId')
        user_id = data.get('userId')
        file_path = data.get('filePath')
        
        if not all([file_id, user_id, file_path]):
            raise BadRequest("Missing required fields: fileId, userId, filePath")
        
        logger.info(f"Processing metadata request for file: {file_id}")
        
        # Process the metadata
        result = metadata_generator.process_file_metadata(file_id, user_id, file_path)
        
        return jsonify({
            'success': True,
            'message': 'Metadata processed successfully',
            'fileId': file_id,
            'enhanced_features': {
                'document_title': result.get('document_title'),
                'total_chunks': result.get('total_chunks'),
                'processing_version': result.get('processing_version')
            }
        }), 200
    
    except BadRequest as e:
        logger.error(f"Bad request in process_metadata: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 400
    
    except Exception as e:
        logger.error(f"Error in process_metadata: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/bulk-update', methods=['POST'])
@rate_limit_decorator(max_requests=5, time_window=3600)
def bulk_update():
    """Bulk update metadata endpoint"""
    try:
        data = request.get_json() or {}
        user_id = data.get('userId')
        batch_size = data.get('batchSize', 10)
        
        if batch_size > 50:
            batch_size = 50  # Limit batch size
        
        logger.info(f"Starting bulk metadata update for user: {user_id}")
        
        # Start bulk update
        result = metadata_generator.bulk_update_metadata(user_id, batch_size)
        
        return jsonify({
            'success': True,
            'message': 'Bulk update completed',
            'results': result
        }), 200
    
    except Exception as e:
        logger.error(f"Error in bulk_update: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/search-similar', methods=['POST'])
@rate_limit_decorator(max_requests=100, time_window=3600)
def search_similar():
    """Search for similar content endpoint"""
    try:
        data = request.get_json()
        
        if not data:
            raise BadRequest("No JSON data provided")
        
        query_text = data.get('query')
        user_id = data.get('userId')
        limit = data.get('limit', 10)
        similarity_threshold = data.get('similarity_threshold', 0.7)
        
        if not all([query_text, user_id]):
            raise BadRequest("Missing required fields: query, userId")
        
        if limit > 50:
            limit = 50  # Limit results
        
        logger.info(f"Searching similar content for user: {user_id}")
        
        # Search for similar content
        results = metadata_generator.search_similar_content(
            query_text, user_id, limit, similarity_threshold
        )
        
        return jsonify({
            'success': True,
            'query': query_text,
            'results': results,
            'total_results': len(results)
        }), 200
    
    except BadRequest as e:
        logger.error(f"Bad request in search_similar: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 400
    
    except Exception as e:
        logger.error(f"Error in search_similar: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/analytics', methods=['GET'])
@rate_limit_decorator(max_requests=100, time_window=3600)
def get_analytics():
    """Get file analytics endpoint"""
    try:
        user_id = request.args.get('userId')
        
        if not user_id:
            raise BadRequest("Missing required parameter: userId")
        
        logger.info(f"Getting analytics for user: {user_id}")
        
        # Get analytics
        analytics = metadata_generator.get_file_analytics(user_id)
        
        return jsonify({
            'success': True,
            'user_id': user_id,
            'analytics': analytics
        }), 200
    
    except BadRequest as e:
        logger.error(f"Bad request in get_analytics: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 400
    
    except Exception as e:
        logger.error(f"Error in get_analytics: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
