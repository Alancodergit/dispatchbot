import os
import logging
import requests
import sqlite3
import time
import sys
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import pdfplumber
from pdf2image import convert_from_path
import pytesseract
from PIL import Image, ImageEnhance, ImageFilter
import PyPDF2
import fitz  # PyMuPDF
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging with detailed output
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot_debug.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION
# ============================================
print("=" * 60)
print("🤖 BOT STARTUP INITIATED")
print("=" * 60)

try:
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
    
    print(f"✓ Bot Token loaded: {bool(TELEGRAM_BOT_TOKEN and TELEGRAM_BOT_TOKEN != 'YOUR_TELEGRAM_BOT_TOKEN_HERE')}")
    print(f"✓ Mistral API Key loaded: {bool(MISTRAL_API_KEY and MISTRAL_API_KEY != 'YOUR_MISTRAL_API_KEY_HERE')}")
    
    # Parse allowed dispatchers from environment variable
    ALLOWED_DISPATCHERS = []
    allowed_dispatchers_str = os.getenv("ALLOWED_DISPATCHERS", "")
    if allowed_dispatchers_str:
        try:
            ALLOWED_DISPATCHERS = [int(x.strip()) for x in allowed_dispatchers_str.split(",") if x.strip()]
            print(f"✓ Allowed dispatchers: {len(ALLOWED_DISPATCHERS)} users")
        except ValueError as e:
            logger.warning(f"Invalid ALLOWED_DISPATCHERS format: {e}")
            print(f"⚠ Warning: Invalid ALLOWED_DISPATCHERS format")
    else:
        print(f"✓ No user restrictions (all users allowed)")
    
    # Set Tesseract path
    tesseract_path = os.getenv('TESSERACT_CMD', r'C:\Program Files\Tesseract-OCR\tesseract.exe')
    if os.path.exists(tesseract_path):
        pytesseract.pytesseract.tesseract_cmd = tesseract_path
        print(f"✓ Tesseract found at: {tesseract_path}")
    else:
        print(f"⚠ Warning: Tesseract not found at: {tesseract_path}")
        print(f"  OCR features will not work!")
    
    # Rate limiting configuration
    MAX_REQUESTS_PER_HOUR = 10
    user_requests = defaultdict(list)
    
    # Maintenance mode
    MAINTENANCE_MODE = os.getenv("MAINTENANCE_MODE", "false").lower() == "true"
    print(f"✓ Maintenance mode: {'ON' if MAINTENANCE_MODE else 'OFF'}")
    
except Exception as e:
    print(f"❌ CONFIGURATION ERROR: {e}")
    traceback.print_exc()
    sys.exit(1)

# ============================================
# ADVANCED PDF EXTRACTOR CLASS
# ============================================

class AdvancedPDFExtractor:
    """Advanced PDF text extraction with multiple fallback methods"""
    
    def __init__(self):
        logger.info("Initializing AdvancedPDFExtractor")
    
    def preprocess_image(self, image: Image.Image) -> Image.Image:
        """Advanced image preprocessing for better OCR"""
        try:
            image = image.convert('L')
            enhancer = ImageEnhance.Contrast(image)
            image = enhancer.enhance(2.0)
            enhancer = ImageEnhance.Sharpness(image)
            image = enhancer.enhance(2.0)
            threshold = 150
            image = image.point(lambda p: 255 if p > threshold else 0)
            image = image.filter(ImageFilter.MedianFilter(size=3))
            return image
        except Exception as e:
            logger.warning(f"Image preprocessing failed: {e}")
            return image.convert('L')
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize extracted text"""
        if not text:
            return ""
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            line = ' '.join(line.split())
            if line.strip():
                cleaned_lines.append(line)
        return '\n'.join(cleaned_lines)
    
    def extract_with_pymupdf(self, pdf_path: str):
        """Method 1: PyMuPDF extraction"""
        try:
            logger.info("Method 1: Trying PyMuPDF...")
            text = ""
            doc = fitz.open(pdf_path)
            for page_num in range(len(doc)):
                page = doc[page_num]
                page_text = page.get_text("text")
                if page_text.strip():
                    text += f"\n--- Page {page_num + 1} ---\n{page_text}"
            doc.close()
            text = self.clean_text(text)
            if len(text.strip()) > 100:
                logger.info(f"✅ PyMuPDF extracted {len(text)} characters")
                return text
            return None
        except Exception as e:
            logger.error(f"PyMuPDF extraction failed: {e}")
            return None
    
    def extract_with_pdfplumber(self, pdf_path: str):
        """Method 2: PDFPlumber extraction"""
        try:
            logger.info("Method 2: Trying PDFPlumber...")
            text = ""
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    page_text = page.extract_text()
                    if page_text:
                        text += f"\n--- Page {page_num + 1} ---\n{page_text}\n"
            text = self.clean_text(text)
            if len(text.strip()) > 100:
                logger.info(f"✅ PDFPlumber extracted {len(text)} characters")
                return text
            return None
        except Exception as e:
            logger.error(f"PDFPlumber extraction failed: {e}")
            return None
    
    def extract_with_pypdf2(self, pdf_path: str):
        """Method 3: PyPDF2 extraction"""
        try:
            logger.info("Method 3: Trying PyPDF2...")
            text = ""
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    page_text = page.extract_text()
                    if page_text.strip():
                        text += f"\n--- Page {page_num + 1} ---\n{page_text}"
            text = self.clean_text(text)
            if len(text.strip()) > 100:
                logger.info(f"✅ PyPDF2 extracted {len(text)} characters")
                return text
            return None
        except Exception as e:
            logger.error(f"PyPDF2 extraction failed: {e}")
            return None
    
    def extract_with_ocr(self, pdf_path: str, max_pages: int = 10):
        """Method 4: OCR extraction"""
        try:
            logger.info("Method 4: Trying OCR...")
            images = convert_from_path(pdf_path, dpi=300, first_page=1, last_page=max_pages)
            text = ""
            for i, image in enumerate(images):
                logger.info(f"OCR processing page {i + 1}/{len(images)}...")
                processed_image = self.preprocess_image(image)
                page_text = pytesseract.image_to_string(processed_image, config='--psm 1 --oem 3')
                if page_text.strip():
                    text += f"\n--- Page {i + 1} (OCR) ---\n{page_text}\n"
            text = self.clean_text(text)
            if len(text.strip()) > 50:
                logger.info(f"✅ OCR extracted {len(text)} characters")
                return text
            return None
        except Exception as e:
            logger.error(f"OCR extraction failed: {e}")
            return None
    
    def extract_text(self, pdf_path: str):
        """Main extraction method - tries all methods"""
        if not os.path.exists(pdf_path):
            return {"text": None, "method": "error", "error": "File not found", "success": False}
        
        methods = [
            ("pymupdf", self.extract_with_pymupdf),
            ("pdfplumber", self.extract_with_pdfplumber),
            ("pypdf2", self.extract_with_pypdf2),
            ("ocr", self.extract_with_ocr),
        ]
        
        for method_name, method_func in methods:
            try:
                result = method_func(pdf_path)
                if result and len(result.strip()) > 100:
                    return {
                        "text": result,
                        "method": method_name,
                        "length": len(result),
                        "success": True
                    }
            except Exception as e:
                logger.error(f"Method {method_name} failed: {e}")
                continue
        
        return {"text": None, "method": "all_failed", "success": False, "error": "All methods failed"}

# ============================================
# DATABASE FUNCTIONS
# ============================================

def init_db():
    """Initialize SQLite database"""
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
        print("✓ Database initialized")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        print(f"❌ Database error: {e}")

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

# ============================================
# RATE LIMITING
# ============================================

def check_rate_limit(user_id: int) -> bool:
    """Check if user has exceeded rate limit"""
    now = datetime.now()
    user_requests[user_id] = [req_time for req_time in user_requests[user_id] if now - req_time < timedelta(hours=1)]
    if len(user_requests[user_id]) >= MAX_REQUESTS_PER_HOUR:
        return False
    user_requests[user_id].append(now)
    return True

def get_rate_limit_info(user_id: int) -> str:
    """Get rate limit information"""
    now = datetime.now()
    user_requests[user_id] = [req_time for req_time in user_requests[user_id] if now - req_time < timedelta(hours=1)]
    remaining = MAX_REQUESTS_PER_HOUR - len(user_requests[user_id])
    return f"Requests this hour: {len(user_requests[user_id])}/{MAX_REQUESTS_PER_HOUR} ({remaining} remaining)"

# ============================================
# PDF EXTRACTION
# ============================================

def extract_text_from_pdf(pdf_path):
    """Extract text using AdvancedPDFExtractor"""
    extractor = AdvancedPDFExtractor()
    result = extractor.extract_text(pdf_path)
    
    if result["success"]:
        return {"text": result["text"], "method": result["method"]}
    else:
        logger.error(f"All extraction methods failed: {result.get('error', 'Unknown error')}")
        return None

# ============================================
# MISTRAL AI ANALYSIS
# ============================================

def analyze_load_with_mistral(text):
    """Send PDF text to Mistral AI"""
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
- Equipment Type

📍 **PICKUP INFORMATION:**
- Date & Time
- Full Address
- Company Name
- Contact Person & Phone

🎯 **DELIVERY INFORMATION:**
- Date & Time
- Full Address
- Company Name
- Contact Person & Phone

💰 **ADDITIONAL RATES:**
- Detention rate
- Layover rate
- Lumper fee
- TONU rate

⚠️ **SPECIAL INSTRUCTIONS:**
- Temperature requirements
- Appointment requirements
- Other notes

Rate Confirmation Document:
{text[:15000]}"""

        headers = {
            "Authorization": f"Bearer {MISTRAL_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "mistral-large-latest",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 2000
        }
        
        logger.info("Sending to Mistral AI...")
        response = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            return result['choices'][0]['message']['content']
        else:
            logger.error(f"Mistral API error {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Error with Mistral AI: {e}")
        return None

# ============================================
# BOT COMMANDS
# ============================================

async def check_maintenance(update: Update) -> bool:
    """Check maintenance mode"""
    if MAINTENANCE_MODE:
        await update.message.reply_text("🔧 Bot is under maintenance. Try again later.")
        return False
    return True

async def check_access(update: Update) -> bool:
    """Check user access"""
    user_id = update.effective_user.id
    if ALLOWED_DISPATCHERS and user_id not in ALLOWED_DISPATCHERS:
        await update.message.reply_text(
            f"⛔ Access Denied\n\nYour ID: `{user_id}`\nContact admin for access.",
            parse_mode='Markdown'
        )
        return False
    return True

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    if not await check_maintenance(update) or not await check_access(update):
        return
    
    user_name = update.effective_user.first_name
    welcome = f"""🚛 **Enhanced Dispatch Bot**

Welcome, {user_name}! 👋

**Features:**
✅ Text-based PDFs
✅ Scanned/Image PDFs (OCR)
✅ AI-powered extraction
✅ Rate limiting: {MAX_REQUESTS_PER_HOUR} requests/hour

**Commands:**
/start - This message
/myid - Your Telegram ID
/help - Help
/stats - Your statistics

**Send me a rate confirmation PDF!**"""
    
    await update.message.reply_text(welcome, parse_mode='Markdown')

async def myid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user ID"""
    if not await check_maintenance(update):
        return
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    rate_info = get_rate_limit_info(user_id)
    await update.message.reply_text(
        f"👤 **Your Information**\nName: {user_name}\nID: `{user_id}`\n\n📊 {rate_info}",
        parse_mode='Markdown'
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help"""
    if not await check_maintenance(update):
        return
    help_text = f"""❓ **Help**

**Supported:**
✅ Text PDFs
✅ Scanned documents
✅ Image-based PDFs

**Limits:**
• Max size: 20 MB
• {MAX_REQUESTS_PER_HOUR} requests/hour

**Tips:**
• Clear, readable files
• Not password protected"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user stats"""
    if not await check_maintenance(update):
        return
    user_id = update.effective_user.id
    try:
        conn = sqlite3.connect('dispatches.db')
        c = conn.cursor()
        c.execute('''SELECT COUNT(*) as total, 
                            SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful
                     FROM processed_docs WHERE user_id = ?''', (user_id,))
        result = c.fetchone()
        conn.close()
        total = result[0] or 0
        successful = result[1] or 0
        rate_info = get_rate_limit_info(user_id)
        await update.message.reply_text(
            f"📊 **Statistics**\n\nTotal: {total}\nSuccessful: {successful}\n\n{rate_info}",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Stats error: {e}")
        await update.message.reply_text("❌ Could not retrieve statistics.")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process PDF documents"""
    if not await check_maintenance(update) or not await check_access(update):
        return
    
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    
    if not check_rate_limit(user_id):
        await update.message.reply_text(f"⏰ Rate limit exceeded\n\n{get_rate_limit_info(user_id)}", parse_mode='Markdown')
        return
    
    document = update.message.document
    
    if not document.file_name.lower().endswith('.pdf'):
        await update.message.reply_text("⚠️ Please send a PDF file.")
        return
    
    file_size_mb = document.file_size / (1024 * 1024)
    if document.file_size > 20 * 1024 * 1024:
        await update.message.reply_text(f"⚠️ File too large: {file_size_mb:.1f} MB (max: 20 MB)")
        return
    
    file_path = None
    status_msg = None
    
    try:
        status_msg = await update.message.reply_text(
            f"📥 **Downloading**\nFile: `{document.file_name}`\nSize: {file_size_mb:.1f} MB",
            parse_mode='Markdown'
        )
        
        file = await context.bot.get_file(document.file_id)
        file_path = f"temp_{user_id}_{document.file_id}.pdf"
        await file.download_to_drive(file_path)
        
        await status_msg.edit_text("📄 **Extracting text...**\n⏳ Please wait...")
        
        extraction_result = extract_text_from_pdf(file_path)
        
        if not extraction_result:
            await status_msg.edit_text("❌ **Extraction Failed**\n\nCould not extract text from PDF.")
            log_processing_result(user_id, user_name, document.file_name, file_size_mb, "failed", 0, False)
            return
        
        extraction_method = extraction_result["method"]
        pdf_text = extraction_result["text"]
        text_length = len(pdf_text)
        
        await status_msg.edit_text(
            f"🤖 **Analyzing with AI...**\nExtracted {text_length} chars using {extraction_method.upper()}"
        )
        
        load_info = analyze_load_with_mistral(pdf_text)
        
        if not load_info:
            await status_msg.edit_text("❌ **AI Analysis Failed**")
            log_processing_result(user_id, user_name, document.file_name, file_size_mb, extraction_method, text_length, False)
            return
        
        await status_msg.delete()
        
        result_header = (
            f"✅ **LOAD INFORMATION**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📄 {document.file_name}\n"
            f"👤 {user_name}\n"
            f"📊 Method: {extraction_method.upper()}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
        )
        
        full_response = result_header + load_info
        
        if len(full_response) <= 4000:
            await update.message.reply_text(full_response, parse_mode='Markdown')
        else:
            await update.message.reply_text(result_header, parse_mode='Markdown')
            chunks = [load_info[i:i+3800] for i in range(0, len(load_info), 3800)]
            for chunk in chunks:
                await update.message.reply_text(chunk)
        
        log_processing_result(user_id, user_name, document.file_name, file_size_mb, extraction_method, text_length, True)
        logger.info(f"✅ Success for {user_name} using {extraction_method}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        traceback.print_exc()
        error_msg = f"❌ **Error**\n\n{str(e)[:300]}"
        if status_msg:
            await status_msg.edit_text(error_msg)
        else:
            await update.message.reply_text(error_msg)
        log_processing_result(user_id, user_name, document.file_name, file_size_mb, "error", 0, False)
        
    finally:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Global error handler"""
    logger.error(f"Update error: {context.error}", exc_info=context.error)
    traceback.print_exc()

def main():
    """Start the bot"""
    try:
        if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN_HERE":
            print("❌ ERROR: TELEGRAM_BOT_TOKEN not set!")
            return
        
        if not MISTRAL_API_KEY or MISTRAL_API_KEY == "YOUR_MISTRAL_API_KEY_HERE":
            print("❌ ERROR: MISTRAL_API_KEY not set!")
            return
        
        init_db()
        
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("myid", myid_command))
        application.add_handler(CommandHandler("stats", stats_command))
        application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
        application.add_error_handler(error_handler)
        
        print("=" * 60)
        print("✅ BOT STARTED SUCCESSFULLY!")
        print("=" * 60)
        print("📱 Bot is ready to process PDFs!")
        print("⏹️  Press Ctrl+C to stop")
        print("=" * 60)
        
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
        
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        traceback.print_exc()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ STARTUP FAILED: {e}")
        traceback.print_exc()
        input("\nPress Enter to exit...")