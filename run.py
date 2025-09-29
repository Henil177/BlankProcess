import pandas as pd
import requests
import time
import os
import csv
from typing import Tuple
import openpyxl
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get the directory where the script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Create output folder if it doesn't exist
OUTPUT_FOLDER = os.path.join(SCRIPT_DIR, 'output')
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Input CSV path (in 'input' folder)
INPUT_FOLDER = os.path.join(SCRIPT_DIR, 'input')
INPUT_CSV = os.path.join(INPUT_FOLDER, 'leadlist.csv')

# Check if input file exists
if not os.path.exists(INPUT_CSV):
    logging.error(f"'leadlist.csv' not found in {INPUT_FOLDER}")
    logging.error("Please make sure 'leadlist.csv' is in the 'input' folder")
    exit(1)

# Read CSV
logging.info(f"Reading CSV from: {INPUT_CSV}")
try:
    df = pd.read_csv(INPUT_CSV)
except pd.errors.ParserError as e:
    logging.error(f"Error reading input CSV: {e}")
    exit(1)

# Gemini API endpoint
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent"
API_KEY = "AIzaSyBbbTtRW1KL5FeXbZ_JralnErLvgfsj73M"  # Replace with your actual API key

def get_messages_from_gemini(row) -> pd.Series:
    """Generate personalized messages using Gemini API with retry logic."""
    
    prompt_text = f"""Generate a personalized outreach message for this lead. Format your response EXACTLY as shown:

WhatsApp: [Your personalized WhatsApp message here]
Subject: [Email subject line here]
Body: [Email body content here]

Lead Details:
Name: {row['Name']}
Email: {row['E-mail']}
Phone: {row['PhoneNumber']}

Make the messages professional, personalized, and engaging."""

    headers = {
        "Content-Type": "application/json"
    }
    
    payload = {
        "contents": [{
            "parts": [{"text": prompt_text}]
        }]
    }
    
    # Add API key to URL
    url = f"{GEMINI_API_URL}?key={API_KEY}"
    
    # Retry logic - try up to 3 times
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Correct path to extract generated text
            generated_text = data["candidates"][0]["content"]["parts"][0]["text"]
            
            # Parse the generated text
            whatsapp_msg, email_subject, email_body = parse_generated_text(generated_text)
            
            logging.info(f"Processed: {row['Name']}")
            return pd.Series([whatsapp_msg, email_subject, email_body])
            
        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 503:
                if attempt < max_retries:
                    wait_time = attempt * 2  # Exponential backoff: 2s, 4s, 6s
                    logging.warning(f"Service unavailable for {row['Name']} (Attempt {attempt}/{max_retries}). Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.error(f"HTTP error for {row['Name']} after {max_retries} attempts: {http_err}")
                    if hasattr(response, 'text'):
                        logging.error(f"  Response: {response.text[:200]}")
            else:
                logging.error(f"HTTP error for {row['Name']}: {http_err}")
                if hasattr(response, 'text'):
                    logging.error(f"  Response: {response.text[:200]}")
                break
                
        except requests.exceptions.RequestException as req_err:
            if attempt < max_retries:
                wait_time = attempt * 2
                logging.warning(f"Request error for {row['Name']} (Attempt {attempt}/{max_retries}). Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                logging.error(f"Request error for {row['Name']} after {max_retries} attempts: {req_err}")
                break
                
        except KeyError as key_err:
            logging.error(f"Parsing error for {row['Name']}: {key_err}")
            logging.error(f"  Response structure: {data.keys() if 'data' in locals() else 'No data'}")
            break
            
        except Exception as e:
            logging.error(f"Unexpected error for {row['Name']}: {e}")
            break
    
    return pd.Series(['', '', ''])

def parse_generated_text(text: str) -> Tuple[str, str, str]:
    """Parse the generated text to extract WhatsApp message, email subject, and body."""
    
    whatsapp_msg = ""
    email_subject = ""
    email_body = ""
    
    lines = text.strip().split("\n")
    current_section = None
    body_lines = []
    
    for line in lines:
        line_lower = line.lower().strip()
        
        if line_lower.startswith("whatsapp:"):
            whatsapp_msg = line.split(":", 1)[1].strip() if ":" in line else ""
            current_section = "whatsapp"
        elif line_lower.startswith("subject:"):
            email_subject = line.split(":", 1)[1].strip() if ":" in line else ""
            current_section = "subject"
        elif line_lower.startswith("body:"):
            email_body = line.split(":", 1)[1].strip() if ":" in line else ""
            current_section = "body"
            if email_body:
                body_lines.append(email_body)
        elif current_section == "body" and line.strip():
            body_lines.append(line.strip())
    
    # Join body lines with a space
    if body_lines:
        email_body = " ".join(body_lines)
    
    # Enhanced cleaning for CSV compatibility
    for char in [',', '\n', '\r', '"', '\t']:
        whatsapp_msg = whatsapp_msg.replace(char, ' ')
        email_subject = email_subject.replace(char, ' ')
        email_body = email_body.replace(char, ' ')
    
    # Replace multiple spaces with a single space
    whatsapp_msg = ' '.join(whatsapp_msg.split())
    email_subject = ' '.join(email_subject.split())
    email_body = ' '.join(email_body.split())
    
    # Log the cleaned content for debugging
    logging.debug(f"Parsed for row: WhatsApp: {whatsapp_msg[:50]}... | Subject: {email_subject[:50]}... | Body: {email_body[:50]}...")
    
    return whatsapp_msg, email_subject, email_body

# Main execution
if __name__ == "__main__":
    logging.info(f"Processing {len(df)} leads...")
    
    # Process each row with a small delay to avoid rate limiting
    results = []
    for idx, row in df.iterrows():
        result = get_messages_from_gemini(row)
        results.append(result)
        
        # Add delay between requests (adjust as needed)
        if idx < len(df) - 1:
            time.sleep(1)
    
    # Assign results to dataframe
    df[['whatsapp-message', 'Email_Subject', 'Email_Body']] = pd.DataFrame(results, index=df.index)
    
    # Save updated CSV and Excel to output folder
    output_csv_path = os.path.join(OUTPUT_FOLDER, 'leadlist_updated.csv')
    output_xlsx_path = os.path.join(OUTPUT_FOLDER, 'leadlist_updated.xlsx')
    try:
        # Save CSV with quoting all fields to handle commas
        df.to_csv(output_csv_path, index=False, quoting=csv.QUOTE_ALL, escapechar='\\')
        # Save Excel
        df.to_excel(output_xlsx_path, index=False, engine='openpyxl')
        logging.info("CSV and Excel files updated successfully!")
        logging.info(f"Saved CSV to: {output_csv_path}")
        logging.info(f"Saved Excel to: {output_xlsx_path}")
        logging.info(f"Total leads processed: {len(df)}")
    except Exception as e:
        logging.error(f"Error saving files: {e}")
        exit(1)