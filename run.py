import pandas as pd
import requests
import time
import os
from typing import Tuple

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
    print(f"✗ Error: 'leadlist.csv' not found in {INPUT_FOLDER}")
    print(f"✗ Please make sure 'leadlist.csv' is in the 'input' folder")
    exit(1)

# Read CSV
print(f"Reading CSV from: {INPUT_CSV}")
df = pd.read_csv(INPUT_CSV)

# Gemini API endpoint
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent"
API_KEY = "AIzaSyBbbTtRW1KL5FeXbZ_JralnErLvgfsj73M"  # Replace with your actual API key

def get_messages_from_gemini(row) -> pd.Series:
    """Generate personalized messages using Gemini API."""
    
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
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Correct path to extract generated text
        generated_text = data["candidates"][0]["content"]["parts"][0]["text"]
        
        # Parse the generated text
        whatsapp_msg, email_subject, email_body = parse_generated_text(generated_text)
        
        print(f"✓ Processed: {row['Name']}")
        return pd.Series([whatsapp_msg, email_subject, email_body])
        
    except requests.exceptions.HTTPError as http_err:
        print(f"✗ HTTP error for {row['Name']}: {http_err}")
        if hasattr(response, 'text'):
            print(f"  Response: {response.text[:200]}")
    except requests.exceptions.RequestException as req_err:
        print(f"✗ Request error for {row['Name']}: {req_err}")
    except KeyError as key_err:
        print(f"✗ Parsing error for {row['Name']}: {key_err}")
        print(f"  Response structure: {data.keys() if 'data' in locals() else 'No data'}")
    except Exception as e:
        print(f"✗ Unexpected error for {row['Name']}: {e}")
    
    return pd.Series(['', '', ''])

def parse_generated_text(text: str) -> Tuple[str, str, str]:
    """Parse the generated text to extract WhatsApp message, email subject, and body."""
    
    whatsapp_msg = ""
    email_subject = ""
    email_body = ""
    
    lines = text.strip().split("\n")
    current_section = None
    
    for line in lines:
        line_lower = line.lower().strip()
        
        if line_lower.startswith("whatsapp:"):
            whatsapp_msg = line.split(":", 1)[1].strip()
            current_section = "whatsapp"
        elif line_lower.startswith("subject:"):
            email_subject = line.split(":", 1)[1].strip()
            current_section = "subject"
        elif line_lower.startswith("body:"):
            email_body = line.split(":", 1)[1].strip()
            current_section = "body"
        elif current_section == "body" and line.strip():
            # Continue appending to body for multi-line content
            email_body += "\n" + line.strip()
    
    return whatsapp_msg, email_subject, email_body

# Main execution
if __name__ == "__main__":
    print(f"Processing {len(df)} leads...\n")
    
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
    
    # Save updated CSV to output folder
    output_path = os.path.join(OUTPUT_FOLDER, 'leadlist_updated.csv')
    try:
        df.to_csv(output_path, index=False)
        print(f"\n✓ CSV updated successfully!")
        print(f"✓ Saved to: {output_path}")
        print(f"✓ Total leads processed: {len(df)}")
    except Exception as e:
        print(f"\n✗ Error saving CSV: {e}")