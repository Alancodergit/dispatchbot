import os
import logging
import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import PyPDF2

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION - EDIT THESE VALUES
# ============================================
TELEGRAM_BOT_TOKEN = "8302927162:AAGRl82zel-zIVrmae4DKLmXU85ZUygyqac"
MISTRAL_API_KEY = "HZgQnLaip9nIhrL36Uo8Wvls6CbbprvL"

# Add your dispatch team's Telegram IDs here
# To get ID: send /myid command to the bot
ALLOWED_DISPATCHERS = [
    # 123456789,  # Add your Telegram ID
    # 987654321,  # Add other dispatcher IDs
]
# If empty list [], anyone can use the bot (not recommended)
# ============================================


def extract_text_from_pdf(pdf_path):
    """Extract all text from PDF file"""
    try:
        text = ""
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            total_pages = len(pdf_reader.pages)
            logger.info(f"PDF has {total_pages} pages")
            
            for page_num in range(total_pages):
                page = pdf_reader.pages[page_num]
                page_text = page.extract_text()
                text += page_text + "\n"
                
        logger.info(f"Extracted {len(text)} characters from PDF")
        return text
    except Exception as e:
        logger.error(f"Error extracting text from PDF: {e}")
        return None


def analyze_load_with_mistral(text):
    """Send PDF text to Mistral AI for load information extraction"""
    try:
        # Prepare the prompt for Mistral
        prompt = f"""Analyze this trucking rate confirmation and extract load information. Format the response clearly and professionally.

Extract these details:

📋 LOAD DETAILS:
- Load Number / Reference #
- Broker Company Name
- Broker Contact (Phone & Email)
- Total Rate/Payment ($)
- Commodity / Freight Type
- Weight (lbs)
- Equipment Type (Dry Van/Reefer/Flatbed/etc)

📍 PICKUP INFORMATION:
- Date & Time (appointment or window)
- Full Address (Street, City, State, ZIP)
- Company Name
- Contact Person & Phone Number
- Special pickup instructions

🎯 DELIVERY INFORMATION:
- Date & Time (appointment or window)
- Full Address (Street, City, State, ZIP)
- Company Name
- Contact Person & Phone Number
- Special delivery instructions

💰 ADDITIONAL RATES:
- Detention rate (if any)
- Layover rate (if any)
- Lumper fee coverage
- TONU (Truck Ordered Not Used) rate

⚠️ SPECIAL INSTRUCTIONS:
- Any temperature requirements
- Appointment needed?
- Driver check-in procedures
- Load securement requirements
- Any other important notes

If any information is not found in the document, write "Not specified" for that field.

Rate Confirmation Document:
{text[:18000]}"""

        # Mistral API request
        headers = {
            "Authorization": f"Bearer {MISTRAL_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "mistral-large-latest",
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.2,  # Low temperature for factual extraction
            "max_tokens": 2500
        }
        
        logger.info("Sending request to Mistral AI...")
        response = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=45
        )
        
        if response.status_code == 200:
            result = response.json()
            extracted_info = result['choices'][0]['message']['content']
            logger.info("Successfully received response from Mistral AI")
            return extracted_info
        else:
            logger.error(f"Mistral API error {response.status_code}: {response.text}")
            return None
            
    except requests.exceptions.Timeout:
        logger.error("Mistral API request timed out")
        return None
    except Exception as e:
        logger.error(f"Error communicating with Mistral AI: {e}")
        return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    
    # Check authorization
    if ALLOWED_DISPATCHERS and user_id not in ALLOWED_DISPATCHERS:
        await update.message.reply_text(
            "⛔ **Access Denied**\n\n"
            "This bot is restricted to authorized dispatchers only.\n\n"
            f"Your Telegram ID: `{user_id}`\n\n"
            "Please contact your admin to get access.",
            parse_mode='Markdown'
        )
        logger.warning(f"Unauthorized access attempt: {user_name} (ID: {user_id})")
        return
    
    welcome_message = f"""🚛 **Dispatch Load Extractor Bot**
*Powered by Mistral AI*

Welcome, {user_name}! 👋

This bot automatically extracts load information from rate confirmation PDFs sent by brokers.

**How to use:**
1️⃣ Send me a rate confirmation PDF
2️⃣ I'll extract all load details
3️⃣ Copy the formatted info and send to your driver

**What I extract:**
✅ Load number & payment rate
✅ Pickup location, date & time
✅ Delivery location, date & time
✅ Broker contact information
✅ Commodity & weight
✅ Special instructions
✅ Detention/layover rates

**Commands:**
/start - Show this message
/myid - Get your Telegram ID
/help - Get help

Ready to process your first load! 📄"""
    
    await update.message.reply_text(welcome_message, parse_mode='Markdown')
    logger.info(f"User {user_name} (ID: {user_id}) started the bot")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    help_text = """❓ **Help & Instructions**

**Supported Files:**
• PDF format only
• Max size: 20 MB
• Text-based PDFs (not scanned images)

**Tips:**
• Make sure PDF is readable (not image/scan)
• Check that all pages are included
• One rate confirmation per message

**Common Issues:**
❌ "Failed to extract" → PDF may be image-based
❌ "Missing information" → Info may not be in PDF
❌ "API error" → Check internet connection

**Need Access?**
Use /myid to get your Telegram ID, then ask admin to add you.

**Support:**
Contact your dispatch manager for help."""
    
    await update.message.reply_text(help_text, parse_mode='Markdown')


async def myid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user their Telegram ID"""
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    username = update.effective_user.username or "No username"
    
    id_message = f"""👤 **Your Telegram Information**

**Name:** {user_name}
**Username:** @{username}
**Telegram ID:** `{user_id}`

📋 Give this ID to your admin to get bot access."""
    
    await update.message.reply_text(id_message, parse_mode='Markdown')
    logger.info(f"ID request from {user_name} (ID: {user_id})")


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process PDF documents sent by users"""
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    
    # Authorization check
    if ALLOWED_DISPATCHERS and user_id not in ALLOWED_DISPATCHERS:
        await update.message.reply_text(
            "⛔ You don't have permission to use this bot.\n"
            f"Your ID: `{user_id}`\n\n"
            "Contact your admin for access.",
            parse_mode='Markdown'
        )
        logger.warning(f"Unauthorized PDF upload: {user_name} (ID: {user_id})")
        return
    
    document = update.message.document
    
    # Validate file type
    if not document.file_name.lower().endswith('.pdf'):
        await update.message.reply_text(
            "⚠️ **Wrong File Type**\n\n"
            "Please send a PDF file.\n"
            f"You sent: {document.file_name}"
        )
        return
    
    # Validate file size (20MB limit)
    file_size_mb = document.file_size / (1024 * 1024)
    if document.file_size > 20 * 1024 * 1024:
        await update.message.reply_text(
            f"⚠️ **File Too Large**\n\n"
            f"File size: {file_size_mb:.1f} MB\n"
            f"Maximum: 20 MB\n\n"
            "Please send a smaller PDF."
        )
        return
    
    file_path = None
    
    try:
        # Step 1: Notify user
        status_msg = await update.message.reply_text(
            "📥 Downloading PDF...\n"
            f"File: {document.file_name}\n"
            f"Size: {file_size_mb:.1f} MB"
        )
        
        # Step 2: Download file
        file = await context.bot.get_file(document.file_id)
        file_path = f"temp_{document.file_id}.pdf"
        await file.download_to_drive(file_path)
        logger.info(f"Downloaded PDF: {document.file_name}")
        
        # Step 3: Extract text
        await status_msg.edit_text("📄 Extracting text from PDF...")
        pdf_text = extract_text_from_pdf(file_path)
        
        if not pdf_text:
            await status_msg.edit_text(
                "❌ **Extraction Failed**\n\n"
                "Could not extract text from PDF.\n"
                "The file might be:\n"
                "• A scanned image (not text-based)\n"
                "• Corrupted or password-protected\n"
                "• An unsupported PDF format"
            )
            return
        
        if len(pdf_text.strip()) < 100:
            await status_msg.edit_text(
                "⚠️ **Very Little Text Found**\n\n"
                "The PDF contains very little text.\n"
                "It might be a scanned image or empty document."
            )
            return
        
        # Step 4: Analyze with Mistral AI
        await status_msg.edit_text(
            "🤖 Analyzing rate confirmation with AI...\n"
            "This may take 10-20 seconds..."
        )
        
        load_info = analyze_load_with_mistral(pdf_text)
        
        if not load_info:
            await status_msg.edit_text(
                "❌ **AI Analysis Failed**\n\n"
                "Could not analyze the document.\n"
                "Possible causes:\n"
                "• Mistral API is down\n"
                "• API key is invalid\n"
                "• Network connectivity issue\n\n"
                "Please try again in a moment."
            )
            return
        
        # Step 5: Send results
        await status_msg.delete()
        
        # Create header
        result_header = (
            f"✅ **LOAD EXTRACTED SUCCESSFULLY**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📄 File: {document.file_name}\n"
            f"👤 Processed by: {user_name}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        )
        
        full_response = result_header + load_info
        
        # Handle Telegram's 4096 character limit
        if len(full_response) <= 4000:
            await update.message.reply_text(full_response, parse_mode='Markdown')
        else:
            # Send header first
            await update.message.reply_text(result_header, parse_mode='Markdown')
            
            # Split and send load info in chunks
            chunk_size = 3800
            for i in range(0, len(load_info), chunk_size):
                chunk = load_info[i:i + chunk_size]
                await update.message.reply_text(chunk)
        
        logger.info(f"Successfully processed PDF for {user_name} (ID: {user_id})")
        
    except Exception as e:
        logger.error(f"Unexpected error processing PDF: {e}")
        await update.message.reply_text(
            "❌ **Unexpected Error**\n\n"
            "An error occurred while processing your PDF.\n"
            f"Error: {str(e)[:100]}\n\n"
            "Please try again or contact support."
        )
        
    finally:
        # Cleanup: Delete temporary file
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Cleaned up temp file: {file_path}")
            except Exception as e:
                logger.error(f"Could not delete temp file: {e}")


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Global error handler"""
    logger.error(f"Update {update} caused error: {context.error}")


def main():
    """Main function to start the bot"""
    
    # Validate configuration
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN_HERE":
        print("❌ ERROR: Please set your TELEGRAM_BOT_TOKEN in the code!")
        return
    
    if MISTRAL_API_KEY == "YOUR_MISTRAL_API_KEY_HERE":
        print("❌ ERROR: Please set your MISTRAL_API_KEY in the code!")
        return
    
    if not ALLOWED_DISPATCHERS:
        print("⚠️  WARNING: ALLOWED_DISPATCHERS is empty - anyone can use the bot!")
        print("   Add Telegram IDs to restrict access.")
    
    # Create application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Register command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("myid", myid_command))
    
    # Register document handler
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    # Register error handler
    application.add_error_handler(error_handler)
    
    # Start bot
    print("=" * 50)
    print("🚛 DISPATCH LOAD EXTRACTOR BOT")
    print("=" * 50)
    print("✅ Bot is running...")
    print("📱 Ready to process rate confirmations!")
    print("⏹️  Press Ctrl+C to stop")
    print("=" * 50)
    
    logger.info("Bot started successfully")
    
    # Run bot
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()