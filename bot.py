import os
import logging
import requests
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import pdfplumber
from pdf2image import convert_from_path
import pytesseract
from PIL import Image
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION
# ============================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")

# Parse allowed dispatchers from environment variable
ALLOWED_DISPATCHERS = []
allowed_dispatchers_str = os.getenv("ALLOWED_DISPATCHERS", "")
if allowed_dispatchers_str:
    try:
        ALLOWED_DISPATCHERS = [int(x.strip()) for x in allowed_dispatchers_str.split(",") if x.strip()]
    except ValueError as e:
        logger.warning(f"Invalid ALLOWED_DISPATCHERS format: {e}")

# Set Tesseract path (adjust for your system)
# Windows default:
pytesseract.pytesseract.tesseract_cmd = os.getenv(
    'TESSERACT_CMD', 
    r'C:\Program Files\Tesseract-OCR\tesseract.exe'
)

# Linux/Mac (uncomment if needed):
# pytesseract.pytesseract.tesseract_cmd = '/usr/bin/tesseract'

# Rate limiting configuration
MAX_REQUESTS_PER_HOUR = 10
user_requests = defaultdict(list)

# Maintenance mode
MAINTENANCE_MODE = os.getenv("MAINTENANCE_MODE", "false").lower() == "true"
# ============================================

def init_db():
    """Initialize SQLite database for tracking processed documents"""
    try:
        conn = sqlite3.connect('dispatches.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS processed_docs
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      user_id INTEGER,
                      user_name TEXT,
                      file_name TEXT,
                      file_size_mb REAL,
                      processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                      extraction_method TEXT,
                      text_length INTEGER,
                      success BOOLEAN)''')
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")

def log_processing_result(user_id, user_name, file_name, file_size_mb, extraction_method, text_length, success):
    """Log processing results to database"""
    try:
        conn = sqlite3.connect('dispatches.db')
        c = conn.cursor()
        c.execute('''INSERT INTO processed_docs 
                     (user_id, user_name, file_name, file_size_mb, extraction_method, text_length, success)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''',
                  (user_id, user_name, file_name, file_size_mb, extraction_method, text_length, success))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to log processing result: {e}")

def check_rate_limit(user_id: int) -> bool:
    """Check if user has exceeded rate limit"""
    now = datetime.now()
    
    # Clean old requests
    user_requests[user_id] = [
        req_time for req_time in user_requests[user_id] 
        if now - req_time < timedelta(hours=1)
    ]
    
    # Check limit
    if len(user_requests[user_id]) >= MAX_REQUESTS_PER_HOUR:
        return False
    
    user_requests[user_id].append(now)
    return True

def get_rate_limit_info(user_id: int) -> str:
    """Get rate limit information for user"""
    now = datetime.now()
    user_requests[user_id] = [
        req_time for req_time in user_requests[user_id] 
        if now - req_time < timedelta(hours=1)
    ]
    
    remaining = MAX_REQUESTS_PER_HOUR - len(user_requests[user_id])
    return f"Requests this hour: {len(user_requests[user_id])}/{MAX_REQUESTS_PER_HOUR} ({remaining} remaining)"

def validate_pdf(file_path: str) -> bool:
    """Check if PDF is valid and not password protected"""
    try:
        with pdfplumber.open(file_path) as pdf:
            # Try to access first page
            if len(pdf.pages) > 0:
                _ = pdf.pages[0].extract_text()
            return True
    except Exception as e:
        logger.error(f"PDF validation failed: {e}")
        return False

def clean_extracted_text(text):
    """Clean and preprocess extracted text"""
    if not text:
        return ""
    
    # Remove excessive whitespace but preserve paragraph structure
    lines = text.split('\n')
    cleaned_lines = []
    
    for line in lines:
        line = ' '.join(line.split())  # Clean whitespace
        if line.strip():
            cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)

def extract_text_with_pdfplumber(pdf_path):
    """Extract text using pdfplumber (better for complex PDFs)"""
    try:
        text = ""
        with pdfplumber.open(pdf_path) as pdf:
            logger.info(f"PDF has {len(pdf.pages)} pages")
            for page_num, page in enumerate(pdf.pages):
                page_text = page.extract_text()
                if page_text:
                    text += f"\n--- Page {page_num + 1} ---\n"
                    text += page_text + "\n"
        
        text = clean_extracted_text(text)
        logger.info(f"Extracted {len(text)} characters with pdfplumber")
        return text if text.strip() else None
    except Exception as e:
        logger.error(f"pdfplumber extraction failed: {e}")
        return None

def extract_text_with_ocr(pdf_path):
    """Extract text using OCR for scanned/image PDFs"""
    try:
        logger.info("Attempting OCR extraction...")
        
        # Convert PDF to images (limit to 5 pages for performance)
        images = convert_from_path(pdf_path, dpi=200, first_page=1, last_page=5)
        logger.info(f"Converted {len(images)} pages to images")
        
        text = ""
        for i, image in enumerate(images):
            logger.info(f"OCR processing page {i + 1}...")
            
            # Preprocess image for better OCR
            image = image.convert('L')  # Convert to grayscale
            
            page_text = pytesseract.image_to_string(image)
            if page_text.strip():
                text += f"\n--- Page {i + 1} (OCR) ---\n"
                text += page_text + "\n"
        
        text = clean_extracted_text(text)
        logger.info(f"OCR extracted {len(text)} characters")
        return text if text.strip() else None
        
    except Exception as e:
        logger.error(f"OCR extraction failed: {e}")
        return None

def extract_text_from_pdf(pdf_path):
    """Try multiple methods to extract text from PDF"""
    
    # Validate PDF first
    if not validate_pdf(pdf_path):
        logger.error("PDF validation failed - file may be corrupted or password protected")
        return None
    
    # Method 1: Try pdfplumber first (fast, good for text PDFs)
    logger.info("Method 1: Trying pdfplumber...")
    text = extract_text_with_pdfplumber(pdf_path)
    
    if text and len(text.strip()) > 100:
        logger.info("✅ pdfplumber successful")
        return {"text": text, "method": "pdfplumber"}
    
    # Method 2: If pdfplumber fails or gets little text, try OCR
    logger.info("Method 2: Trying OCR...")
    text = extract_text_with_ocr(pdf_path)
    
    if text and len(text.strip()) > 50:
        logger.info("✅ OCR successful")
        return {"text": text, "method": "ocr"}
    
    logger.error("❌ All extraction methods failed")
    return None

def analyze_load_with_mistral(text):
    """Send PDF text to Mistral AI for load information extraction"""
    try:
        prompt = f"""Analyze this trucking rate confirmation and extract load information. Format the response clearly and concisely.

**EXTRACT THESE DETAILS:**

📋 **LOAD DETAILS:**
- Load Number / Reference #
- Broker Company Name
- Broker Contact (Phone & Email)
- Total Rate/Payment ($)
- Commodity / Freight Type
- Weight (lbs)
- Equipment Type (Dry Van/Reefer/Flatbed/etc)

📍 **PICKUP INFORMATION:**
- Date & Time (appointment or window)
- Full Address (Street, City, State, ZIP)
- Company Name
- Contact Person & Phone Number
- Special pickup instructions

🎯 **DELIVERY INFORMATION:**
- Date & Time (appointment or window)
- Full Address (Street, City, State, ZIP)
- Company Name
- Contact Person & Phone Number
- Special delivery instructions

💰 **ADDITIONAL RATES:**
- Detention rate (if any)
- Layover rate (if any)
- Lumper fee coverage
- TONU rate

⚠️ **SPECIAL INSTRUCTIONS:**
- Temperature requirements
- Appointment requirements
- Any other important notes

**FORMATTING:**
- Use clear sections with emojis
- Bold headings for each section
- If information is not found, write "Not specified"
- Keep it organized and easy to read

Rate Confirmation Document:
{text[:15000]}"""  # Reduced character limit for better performance

        headers = {
            "Authorization": f"Bearer {MISTRAL_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "mistral-large-latest",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,  # Lower temperature for more consistent results
            "max_tokens": 2000
        }
        
        logger.info("Sending to Mistral AI...")
        response = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=60  # Increased timeout
        )
        
        if response.status_code == 200:
            result = response.json()
            return result['choices'][0]['message']['content']
        else:
            logger.error(f"Mistral API error {response.status_code}: {response.text}")
            return None
            
    except requests.exceptions.Timeout:
        logger.error("Mistral API request timed out")
        return "❌ AI analysis timed out. Please try again with a smaller file or contact support."
    except Exception as e:
        logger.error(f"Error with Mistral AI: {e}")
        return None

async def check_maintenance(update: Update) -> bool:
    """Check if bot is in maintenance mode"""
    if MAINTENANCE_MODE:
        await update.message.reply_text(
            "🔧 **Maintenance Mode**\n\n"
            "The bot is currently under maintenance. Please try again later.\n"
            "We apologize for any inconvenience."
        )
        return False
    return True

async def check_access(update: Update) -> bool:
    """Check if user has access to the bot"""
    user_id = update.effective_user.id
    
    if ALLOWED_DISPATCHERS and user_id not in ALLOWED_DISPATCHERS:
        await update.message.reply_text(
            "⛔ **Access Denied**\n\n"
            f"Your Telegram ID: `{user_id}`\n"
            "Contact administrator to get access to this bot.",
            parse_mode='Markdown'
        )
        return False
    return True

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    if not await check_maintenance(update):
        return
    
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    
    if not await check_access(update):
        return
    
    welcome = f"""🚛 **Enhanced Dispatch Bot**
*Powered by Mistral AI with OCR Support*

Welcome, {user_name}! 👋

**Features:**
✅ Text-based PDFs (fast processing)
✅ Scanned/Image PDFs (OCR technology)
✅ Complex layout handling
✅ AI-powered data extraction
✅ Rate limiting: {MAX_REQUESTS_PER_HOUR} requests/hour

**Commands:**
/start - Show this message
/myid - Get your Telegram ID
/help - Get help and instructions
/stats - Get your usage statistics

**Simply send me any rate confirmation PDF!**"""
    
    await update.message.reply_text(welcome, parse_mode='Markdown')

async def myid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user their Telegram ID"""
    if not await check_maintenance(update):
        return
        
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    
    rate_limit_info = get_rate_limit_info(user_id)
    
    await update.message.reply_text(
        f"👤 **Your Information**\n"
        f"Name: {user_name}\n"
        f"Telegram ID: `{user_id}`\n\n"
        f"📊 **Usage:**\n{rate_limit_info}",
        parse_mode='Markdown'
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    if not await check_maintenance(update):
        return
        
    help_text = f"""❓ **Help & Instructions**

**Supported PDF Types:**
✅ Regular text PDFs (fastest)
✅ Scanned documents (OCR)
✅ Image-based PDFs
✅ Complex layouts and tables

**File Requirements:**
• Maximum size: 20 MB
• Not password protected
• Clear, readable text/images
• One rate confirmation per file

**Processing Times:**
• Text PDFs: 10-20 seconds
• Scanned PDFs: 30-90 seconds (OCR dependent)
• AI Analysis: 10-20 seconds

**Rate Limits:**
• {MAX_REQUESTS_PER_HOUR} requests per hour per user

**Tips for Best Results:**
• Ensure PDFs are high quality
• Avoid blurry or rotated scans
• Crop unnecessary borders
• Use well-lit, clear images

**Need Help?**
Contact your system administrator for support."""

    await update.message.reply_text(help_text, parse_mode='Markdown')

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user their statistics"""
    if not await check_maintenance(update):
        return
        
    user_id = update.effective_user.id
    
    try:
        conn = sqlite3.connect('dispatches.db')
        c = conn.cursor()
        
        # Get user stats
        c.execute('''SELECT COUNT(*) as total, 
                            SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                            AVG(file_size_mb) as avg_size
                     FROM processed_docs 
                     WHERE user_id = ?''', (user_id,))
        
        result = c.fetchone()
        conn.close()
        
        total_processed = result[0] or 0
        successful = result[1] or 0
        avg_size = result[2] or 0
        
        rate_limit_info = get_rate_limit_info(user_id)
        
        stats_text = f"""📊 **Your Statistics**

📁 Total PDFs Processed: {total_processed}
✅ Successful Extractions: {successful}
📦 Average File Size: {avg_size:.1f} MB

{rate_limit_info}"""

        await update.message.reply_text(stats_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        await update.message.reply_text("❌ Could not retrieve statistics at this time.")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process PDF documents"""
    if not await check_maintenance(update):
        return
        
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    
    if not await check_access(update):
        return
    
    # Check rate limiting
    if not check_rate_limit(user_id):
        rate_limit_info = get_rate_limit_info(user_id)
        await update.message.reply_text(
            f"⏰ **Rate Limit Exceeded**\n\n"
            f"You've reached the maximum number of requests per hour.\n\n"
            f"{rate_limit_info}\n\n"
            f"Please wait and try again later.",
            parse_mode='Markdown'
        )
        return
    
    document = update.message.document
    
    if not document.file_name.lower().endswith('.pdf'):
        await update.message.reply_text("⚠️ Please send a PDF file. Other formats are not supported.")
        return
    
    file_size_mb = document.file_size / (1024 * 1024)
    if document.file_size > 20 * 1024 * 1024:
        await update.message.reply_text(
            f"⚠️ **File Too Large**\n\n"
            f"File size: {file_size_mb:.1f} MB\n"
            f"Maximum allowed: 20 MB\n\n"
            f"Please compress the file or use a smaller PDF."
        )
        return
    
    file_path = None
    status_msg = None
    
    try:
        status_msg = await update.message.reply_text(
            "📥 **Downloading PDF...**\n"
            f"File: `{document.file_name}`\n"
            f"Size: {file_size_mb:.1f} MB\n"
            f"User: {user_name}",
            parse_mode='Markdown'
        )
        
        # Download file
        file = await context.bot.get_file(document.file_id)
        file_path = f"temp_{user_id}_{document.file_id}.pdf"
        await file.download_to_drive(file_path)
        
        await status_msg.edit_text(
            "📄 **Extracting Text...**\n"
            "⏳ This may take 10-90 seconds depending on PDF type and size...\n"
            "• Checking PDF validity\n"
            "• Attempting text extraction\n"
            "• Fallback to OCR if needed"
        )
        
        # Extract text from PDF
        extraction_result = extract_text_from_pdf(file_path)
        
        if not extraction_result:
            await status_msg.edit_text(
                "❌ **Extraction Failed**\n\n"
                "Could not extract text from this PDF.\n\n"
                "**Possible Issues:**\n"
                "• Password-protected PDF\n"
                "• Corrupted file\n"
                "• Unsupported format\n"
                "• Poor image quality (for scanned PDFs)\n\n"
                "**Solutions:**\n"
                "• Ensure PDF is not encrypted\n"
                "• Try a different PDF file\n"
                "• Use higher quality scans\n"
                "• Contact support if problem persists"
            )
            log_processing_result(user_id, user_name, document.file_name, file_size_mb, "failed", 0, False)
            return
        
        extraction_method = extraction_result["method"]
        pdf_text = extraction_result["text"]
        text_length = len(pdf_text)
        
        if text_length < 100:
            await status_msg.edit_text(
                "⚠️ **Very Little Text Extracted**\n\n"
                f"Only {text_length} characters found.\n"
                "The PDF might be:\n"
                "• Mostly blank or empty\n"
                "• Contain only images without text\n"
                "• Have unreadable content\n\n"
                "Try a different PDF with more text content."
            )
            log_processing_result(user_id, user_name, document.file_name, file_size_mb, extraction_method, text_length, False)
            return
        
        await status_msg.edit_text(
            "🤖 **Analyzing with AI...**\n"
            f"Extracted {text_length} characters using {extraction_method.upper()}\n"
            "• Processing with Mistral AI\n"
            "• Extracting load details\n"
            "• Formatting results...\n"
            "⏳ This may take 10-20 seconds..."
        )
        
        # Analyze with AI
        load_info = analyze_load_with_mistral(pdf_text)
        
        if not load_info:
            await status_msg.edit_text(
                "❌ **AI Analysis Failed**\n\n"
                "Could not analyze the document with AI.\n\n"
                "**Possible Causes:**\n"
                "• API service temporarily unavailable\n"
                "• Network connectivity issues\n"
                "• Document content too complex\n\n"
                "Please try again in a few minutes."
            )
            log_processing_result(user_id, user_name, document.file_name, file_size_mb, extraction_method, text_length, False)
            return
        
        # Delete status message
        await status_msg.delete()
        
        # Prepare and send results
        result_header = (
            f"✅ **LOAD INFORMATION EXTRACTED**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📄 **File:** {document.file_name}\n"
            f"👤 **Processed for:** {user_name}\n"
            f"📊 **Method:** {extraction_method.upper()}\n"
            f"🔢 **Text Length:** {text_length} characters\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        )
        
        full_response = result_header + load_info
        
        # Split message if too long for Telegram
        if len(full_response) <= 4000:
            await update.message.reply_text(full_response, parse_mode='Markdown')
        else:
            # Send header first
            await update.message.reply_text(result_header, parse_mode='Markdown')
            
            # Split the load info into chunks
            chunks = [load_info[i:i+3800] for i in range(0, len(load_info), 3800)]
            for chunk in chunks:
                await update.message.reply_text(chunk)
        
        # Log successful processing
        log_processing_result(user_id, user_name, document.file_name, file_size_mb, extraction_method, text_length, True)
        logger.info(f"✅ Successfully processed PDF for {user_name} using {extraction_method}")
        
    except Exception as e:
        logger.error(f"Unexpected error processing PDF: {e}")
        error_message = f"❌ **Unexpected Error**\n\n{str(e)[:300]}"
        
        if status_msg:
            await status_msg.edit_text(error_message)
        else:
            await update.message.reply_text(error_message)
        
        # Log failed processing
        log_processing_result(user_id, user_name, document.file_name, file_size_mb, "error", 0, False)
        
    finally:
        # Clean up temporary file
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Cleaned up temporary file: {file_path}")
            except Exception as e:
                logger.error(f"Failed to clean up temporary file: {e}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Global error handler"""
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)
    
    # Notify user about critical errors
    if update and update.effective_user:
        try:
            await update.message.reply_text(
                "❌ **An unexpected error occurred**\n\n"
                "The bot encountered an error while processing your request.\n"
                "Please try again or contact support if the problem persists."
            )
        except:
            pass

def main():
    """Start the bot"""
    
    # Validate required configuration
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN_HERE":
        print("❌ ERROR: TELEGRAM_BOT_TOKEN not set!")
        print("Please set the TELEGRAM_BOT_TOKEN environment variable")
        return
    
    if not MISTRAL_API_KEY or MISTRAL_API_KEY == "YOUR_MISTRAL_API_KEY_HERE":
        print("❌ ERROR: MISTRAL_API_KEY not set!")
        print("Please set the MISTRAL_API_KEY environment variable")
        return
    
    # Initialize database
    init_db()
    
    # Create application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("myid", myid_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_error_handler(error_handler)
    
    # Startup message
    print("=" * 60)
    print("🚛 ENHANCED DISPATCH BOT WITH OCR & AI")
    print("=" * 60)
    print(f"✅ Bot Token: {'Set' if TELEGRAM_BOT_TOKEN else 'Missing'}")
    print(f"✅ Mistral API Key: {'Set' if MISTRAL_API_KEY else 'Missing'}")
    print(f"✅ Allowed Users: {len(ALLOWED_DISPATCHERS)}")
    print(f"✅ Maintenance Mode: {'ON' if MAINTENANCE_MODE else 'OFF'}")
    print(f"✅ Rate Limit: {MAX_REQUESTS_PER_HOUR}/hour per user")
    print("✅ Database initialized")
    print("📱 Bot is ready to process rate confirmations!")
    print("⏹️  Press Ctrl+C to stop the bot")
    print("=" * 60)
    
    # Start polling
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == '__main__':
    main()