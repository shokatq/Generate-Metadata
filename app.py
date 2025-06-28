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
            logger.info(
                f"Blob properties - Size: {blob_properties.size}, "
                f"Content-Type: {blob_properties.content_settings.content_type}"
            )
            
            blob_data = blob_client.download_blob()
            content = blob_data.readall()
            
            logger.info(f"Successfully downloaded {len(content)} bytes from blob")
            return content
        
        except Exception as e:
            logger.error(f"Error downloading file from blob: {str(e)}")
            logger.error(f"Container: {self.container_name}, Blob: {file_path}")
            raise Exception(f"Failed to download file from blob storage: {str(e)}")

    def extract_text_from_pdf(self, file_content: bytes) -> str:
        """Extract text from PDF file"""
        try:
            pdf_file = BytesIO(file_content)
            reader = PyPDF2.PdfReader(pdf_file)
            text = ""
            
            for page in reader.pages:
                text += page.extract_text() + "\n"
            
            return text.strip()
        
        except Exception as e:
            logger.error(f"Error extracting text from PDF: {str(e)}")
            return ""

    def extract_text_from_docx(self, file_content: bytes) -> str:
        """Extract text from DOCX file"""
        try:
            doc_file = BytesIO(file_content)
            doc = docx.Document(doc_file)
            text = ""
            
            for paragraph in doc.paragraphs:
                text += paragraph.text + "\n"
            
            return text.strip()
        
        except Exception as e:
            logger.error(f"Error extracting text from DOCX: {str(e)}")
            return ""

    def extract_text_from_xlsx(self, file_content: bytes) -> str:
        """Extract text from XLSX file"""
        try:
            excel_file = BytesIO(file_content)
            workbook = openpyxl.load_workbook(excel_file, data_only=True)
            text = ""
            
            for sheet_name in workbook.sheetnames:
                sheet = workbook[sheet_name]
                text += f"Sheet: {sheet_name}\n"
                
                for row in sheet.iter_rows(values_only=True):
                    row_text = " | ".join([str(cell) if cell is not None else "" for cell in row])
                    if row_text.strip():
                        text += row_text + "\n"
                text += "\n"
            
            return text.strip()
        
        except Exception as e:
            logger.error(f"Error extracting text from XLSX: {str(e)}")
            return ""

    def extract_text_from_pptx(self, file_content: bytes) -> str:
        """Extract text from PPTX file"""
        try:
            ppt_file = BytesIO(file_content)
            presentation = Presentation(ppt_file)
            text = ""
            
            for slide_num, slide in enumerate(presentation.slides, 1):
                text += f"Slide {slide_num}:\n"
                
                for shape in slide.shapes:
                    if hasattr(shape, "text") and shape.text:
                        text += shape.text + "\n"
                
                text += "\n"
            
            return text.strip()
        
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
                    return text
                except UnicodeDecodeError:
                    continue
            
            # If all encodings fail, use utf-8 with errors='ignore'
            text = file_content.decode('utf-8', errors='ignore')
            logger.warning("Used utf-8 with errors='ignore' for text file")
            return text
        
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

    def generate_document_title(self, text_content: str, filename: str) -> str:
        """Generate document title using GPT-4o"""
        try:
            # Use first 2000 characters for title generation
            text_preview = text_content[:2000] if len(text_content) > 2000 else text_content
            
            prompt = f"""
            Based on the following content from a file named "{filename}", generate a clear, descriptive title for this document. 
            The title should be concise (max 10 words) and capture the main topic or purpose of the document.

            Content:
            {text_preview}

            Generate only the title, nothing else.
            """
            
            response = self.openai_text_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that creates clear, concise document titles."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=50,
                temperature=0.3
            )
            
            title = response.choices[0].message.content.strip()
            # Remove quotes if present
            title = title.strip('"').strip("'")
            return title
        
        except Exception as e:
            logger.error(f"Error generating document title: {str(e)}")
            # Fallback to filename without extension
            return os.path.splitext(filename)[0]

    def generate_text_summary(self, text_content: str, document_title: str) -> str:
        """Generate text summary using GPT-4o"""
        try:
            # Truncate text if too long
            max_chars = 10000
            if len(text_content) > max_chars:
                text_content = text_content[:max_chars] + "..."
            
            prompt = f"""
            Please provide a concise summary of the following document titled "{document_title}":

            {text_content}

            Summary should be 1-2 sentences capturing the main purpose and content of the document.
            """
            
            response = self.openai_text_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that creates concise document summaries."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=150,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
        
        except Exception as e:
            logger.error(f"Error generating text summary: {str(e)}")
            return f"Document containing content related to {document_title}"

    def generate_embeddings(self, text_content: str) -> List[float]:
        """Generate embeddings using text-embedding-3-large"""
        try:
            # Truncate text if too long for embedding model
            max_chars = 8000
            if len(text_content) > max_chars:
                text_content = text_content[:max_chars]
            
            response = self.openai_embedding_client.embeddings.create(
                model="text-embedding-3-large",
                input=text_content,
                encoding_format="float"
            )
            
            return response.data[0].embedding
        
        except Exception as e:
            logger.error(f"Error generating embeddings: {str(e)}")
            # Return empty embedding on error
            return [0.0] * 3072

    def get_existing_metadata(self, file_id: str, user_id: str) -> Dict[str, Any]:
        """Retrieve existing metadata from Cosmos DB"""
        try:
            response = self.container.read_item(
                item=file_id,
                partition_key=user_id  # Use user_id as partition key
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
        """Process a single file's metadata"""
        try:
            file_id = metadata.get('id')
            file_path = metadata.get('filePath')
            filename = metadata.get('fileName', '')
            
            logger.info(f"Processing file metadata for: {file_id}")
            logger.info(f"File path: {file_path}")
            logger.info(f"Filename: {filename}")
            
            # Check if file exists in blob storage
            try:
                file_content = self.download_file_from_blob(file_path)
                logger.info(f"Successfully downloaded file, size: {len(file_content)} bytes")
            except Exception as e:
                logger.warning(f"Could not download file {file_path}: {str(e)}")
                return False, f"File not accessible: {str(e)}", metadata
            
            # Determine file extension
            file_extension = os.path.splitext(filename)[-1].lower()
            logger.info(f"File extension: {file_extension}")
            
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
            
            # Generate document title
            logger.info("Generating document title...")
            document_title = self.generate_document_title(extracted_text, filename)
            logger.info(f"Generated title: {document_title}")
            
            # Generate text summary
            logger.info("Generating text summary...")
            text_summary = self.generate_text_summary(extracted_text, document_title)
            logger.info(f"Generated summary: {text_summary[:100]}...")
            
            # Generate embeddings
            logger.info("Generating embeddings...")
            embeddings = self.generate_embeddings(extracted_text)
            logger.info(f"Generated embeddings with {len(embeddings)} dimensions")
            
            # Update metadata with new fields
            updated_metadata = metadata.copy()
            updated_metadata.update({
                'document_title': document_title,
                'textSummary': text_summary,
                'embedding': embeddings,
                'processed_at': datetime.utcnow().isoformat() + 'Z',
                'text_length': len(extracted_text),
                'embedding_model': 'text-embedding-3-large',
                'summary_model': 'gpt-4o',
                'title_model': 'gpt-4o'
            })
            
            # Save updated metadata to Cosmos DB
            logger.info("Updating metadata in Cosmos DB...")
            self.update_metadata_in_cosmos(updated_metadata)
            
            return True, "Success", updated_metadata
        
        except Exception as e:
            logger.error(f"Error processing file metadata: {str(e)}")
            return False, str(e), metadata

    def process_file_metadata(self, file_id: str, user_id: str, file_path: str) -> Dict[str, Any]:
        """Process file metadata - complete implementation"""
        try:
            logger.info(f"Starting metadata processing for file_id: {file_id}")
            
            # Get existing metadata with user_id as partition key
            existing_metadata = self.get_existing_metadata(file_id, user_id)
            logger.info(f"Retrieved existing metadata for file: {file_id}")
            
            # Check if metadata is already updated
            if self.is_metadata_updated(existing_metadata):
                logger.info(f"Metadata already updated for file: {file_id}")
                return existing_metadata
            
            # Process the file using the existing single file processing method
            success, message, updated_metadata = self.process_single_file_metadata(existing_metadata)
            
            if not success:
                logger.error(f"Failed to process file metadata: {message}")
                raise Exception(f"Processing failed: {message}")
            
            logger.info(f"Successfully processed metadata for file: {file_id}")
            return updated_metadata
        
        except Exception as e:
            logger.error(f"Error processing file metadata: {str(e)}")
            raise

    def batch_process_old_metadata(self, user_id: Optional[str] = None, batch_size: int = 10) -> Dict[str, Any]:
        """Process all old metadata in batches"""
        try:
            # Get all old metadata
            old_metadata_items = self.get_all_old_metadata(user_id)
            
            if not old_metadata_items:
                return {
                    'total_items': 0,
                    'processed': 0,
                    'failed': 0,
                    'skipped': 0,
                    'results': []
                }
            
            processed_count = 0
            failed_count = 0
            skipped_count = 0
            results = []
            
            # Process in batches
            for i in range(0, len(old_metadata_items), batch_size):
                batch = old_metadata_items[i:i + batch_size]
                logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} items)")
                
                for item in batch:
                    try:
                        success, message, updated_metadata = self.process_single_file_metadata(item)
                        
                        result = {
                            'file_id': item.get('id'),
                            'filename': item.get('fileName'),
                            'success': success,
                            'message': message
                        }
                        
                        if success:
                            processed_count += 1
                        else:
                            failed_count += 1
                        
                        results.append(result)
                        
                    except Exception as e:
                        failed_count += 1
                        results.append({
                            'file_id': item.get('id'),
                            'filename': item.get('fileName'),
                            'success': False,
                            'message': str(e)
                        })
                
                # Add delay between batches to avoid rate limiting
                if i + batch_size < len(old_metadata_items):
                    time.sleep(2)
            
            return {
                'total_items': len(old_metadata_items),
                'processed': processed_count,
                'failed': failed_count,
                'skipped': skipped_count,
                'results': results
            }
        
        except Exception as e:
            logger.error(f"Error in batch processing: {str(e)}")
            raise


# Flask app
app = Flask(__name__)
metadata_generator = FileMetadataGenerator()


def handle_errors(f):
    """Decorator to handle errors consistently"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {f.__name__}: {str(e)}")
            return jsonify({
                'success': False,
                'message': str(e),
                'metadata': None
            }), 500
    return decorated_function


@app.route('/generate-metadata', methods=['POST'])
@handle_errors
def generate_file_metadata():
    """Generate enhanced metadata for a file including text summary and embeddings"""
    try:
        # Check if request has JSON data
        if not request.is_json:
            logger.error("Request does not contain JSON data")
            return jsonify({
                'success': False,
                'message': 'Request must contain JSON data with Content-Type: application/json'
            }), 400
        
        data = request.get_json()
        if not data:
            logger.error("Request JSON is empty")
            return jsonify({
                'success': False,
                'message': 'Request JSON is empty'
            }), 400
        
        logger.info(f"Received request data: {data}")
        
        # Validate required fields
        required_fields = ['file_id', 'user_id', 'file_path']
        missing_fields = [field for field in required_fields if field not in data or not data[field]]
        
        if missing_fields:
            logger.error(f"Missing required fields: {missing_fields}")
            return jsonify({
                'success': False,
                'message': f'Missing required fields: {missing_fields}'
            }), 400
        
        file_id = data['file_id']
        user_id = data['user_id']
        file_path = data['file_path']
        
        logger.info(f"Processing metadata for file_id: {file_id}")
        
        updated_metadata = metadata_generator.process_file_metadata(
            file_id, user_id, file_path
        )
        
        return jsonify({
            'success': True,
            'message': 'File metadata generated successfully',
            'metadata': updated_metadata
        }), 200
    
    except Exception as e:
        logger.error(f"Error in generate_file_metadata: {str(e)}")
        return jsonify({
            'success': False,
            'message': str(e),
            'metadata': None
        }), 500


@app.route('/check-metadata-status/<file_id>', methods=['GET'])
@handle_errors
def check_metadata_status(file_id):
    """Check if metadata is updated or old"""
    try:
        user_id = request.args.get('user_id')
        if not user_id:
            return jsonify({
                'success': False,
                'message': 'user_id parameter is required'
            }), 400
        
        metadata = metadata_generator.get_existing_metadata(file_id, user_id)
        is_updated = metadata_generator.is_metadata_updated(metadata)
        
        return jsonify({
            'success': True,
            'file_id': file_id,
            'is_updated': is_updated,
            'has_text_summary': 'textSummary' in metadata and metadata['textSummary'],
            'has_embedding': 'embedding' in metadata and metadata['embedding'],
            'has_document_title': 'document_title' in metadata and metadata['document_title'],
            'processed_at': metadata.get('processed_at'),
            'metadata': metadata
        }), 200
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 404


@app.route('/batch-process-old-metadata', methods=['POST'])
@handle_errors
def batch_process_old_metadata():
    """Process all old metadata records"""
    user_id = request.json.get('user_id') if request.json else None
    batch_size = request.json.get('batch_size', 10) if request.json else 10
    
    try:
        results = metadata_generator.batch_process_old_metadata(user_id, batch_size)
        
        return jsonify({
            'success': True,
            'message': 'Batch processing completed',
            'summary': results
        }), 200
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


def batch_process_background(user_id: Optional[str] = None, batch_size: int = 10):
    """Background task for batch processing"""
    try:
        results = metadata_generator.batch_process_old_metadata(user_id, batch_size)
        logger.info(f"Background batch processing completed: {results}")
    except Exception as e:
        logger.error(f"Background batch processing failed: {str(e)}")


@app.route('/batch-process-old-metadata-async', methods=['POST'])
@handle_errors
def batch_process_old_metadata_async():
    """Process all old metadata records asynchronously"""
    user_id = request.json.get('user_id') if request.json else None
    batch_size = request.json.get('batch_size', 10) if request.json else 10
    
    # Start background thread
    background_thread = threading.Thread(
        target=batch_process_background,
        args=(user_id, batch_size)
    )
    background_thread.daemon = True
    background_thread.start()
    
    return jsonify({
        'success': True,
        'message': 'Batch processing started in background',
        'user_id': user_id,
        'batch_size': batch_size
    }), 202


@app.route('/get-old-metadata-count', methods=['GET'])
@handle_errors
def get_old_metadata_count():
    """Get count of metadata records that need updating"""
    user_id = request.args.get('user_id')
    
    try:
        old_metadata_items = metadata_generator.get_all_old_metadata(user_id)
        
        return jsonify({
            'success': True,
            'user_id': user_id,
            'old_metadata_count': len(old_metadata_items),
            'message': f'Found {len(old_metadata_items)} records that need updating'
        }), 200
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


@app.route('/generate-metadata-async', methods=['POST'])
@handle_errors
def generate_file_metadata_async():
    """Generate enhanced metadata for a file asynchronously"""
    if not request.json:
        return jsonify({
            'success': False,
            'message': 'Request must contain JSON data'
        }), 400
    
    required_fields = ['file_id', 'user_id', 'file_path']
    for field in required_fields:
        if field not in request.json:
            return jsonify({
                'success': False,
                'message': f'Missing required field: {field}'
            }), 400
    
    file_id = request.json['file_id']
    user_id = request.json['user_id']
    file_path = request.json['file_path']
    
    def process_metadata_background():
        try:
            metadata_generator.process_file_metadata(file_id, user_id, file_path)
            logger.info(f"Background processing completed for file: {file_id}")
        except Exception as e:
            logger.error(f"Background processing failed for file {file_id}: {str(e)}")
    
    # Start background thread
    background_thread = threading.Thread(target=process_metadata_background)
    background_thread.daemon = True
    background_thread.start()
    
    return jsonify({
        'success': True,
        'message': 'File metadata generation started in background',
        'file_id': file_id
    }), 202


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'file-metadata-generator',
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }), 200


@app.route('/supported-formats', methods=['GET'])
def get_supported_formats():
    """Get list of supported file formats"""
    return jsonify({
        'supported_extensions': list(metadata_generator.supported_extensions.keys()),
        'mime_types': metadata_generator.supported_extensions
    }), 200


@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'message': 'Endpoint not found'
    }), 404
@app.errorhandler(500)
def internal_server_error(error):
    return jsonify({
        'success': False,
        'message': 'Internal server error occurred'
    }), 500


@app.errorhandler(BadRequest)
def bad_request(error):
    return jsonify({
        'success': False,
        'message': 'Bad request - invalid data provided'
    }), 400


@app.errorhandler(Exception)
def handle_exception(error):
    logger.error(f"Unhandled exception: {str(error)}")
    return jsonify({
        'success': False,
        'message': 'An unexpected error occurred'
    }), 500


if __name__ == '__main__':
    # Validate required environment variables
    required_env_vars = [
        'AZURE_STORAGE_CONNECTION_STRING_1',
        'COSMOS_ENDPOINT',
        'COSMOS_KEY',
        'OPENAI_API_KEY'
    ]
    
    missing_vars = []
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        exit(1)
    
    logger.info("Starting File Metadata Generator service...")
    logger.info(f"Supported file formats: {list(metadata_generator.supported_extensions.keys())}")
    
    # Run the Flask app
    app.run(
        host='0.0.0.0',
        port=int(os.getenv('PORT', 5000)),
        debug=os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    )
