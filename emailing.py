import os
import smtplib
import imaplib
import email
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from openai import OpenAI
from database import log_event

try:
    SENDER_EMAIL = os.environ["GMAIL_EMAIL"]
    SENDER_PASSWORD = os.environ["GMAIL_PASSWORD"]
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
except KeyError as e:
    print(f"FATAL ERROR: A required email/AI environment variable is missing: {e}.")
    exit(1)

SMTP_SERVER, SMTP_PORT = "smtp.gmail.com", 587
IMAP_SERVER, IMAP_PORT = "imap.gmail.com", 993
CALENDLY_LINK = "https://calendly.com/thridorbit03/30min"
OTHER_SERVICES_LINK = "https://www.morphius.in/services"

def generate_email_body(contact):
    """Generates email content, using OpenAI with a fallback template."""
    name, domain = contact.get('name'), contact.get('domain', 'their industry')
    greeting = f"Hi {name}," if pd.notna(name) and name.strip() else "Dear Sir/Madam,"
    try:
        prompt = f"""Write a professional, concise outreach email from Aasrith at Morphius AI (morphius.in) to {name or 'a professional'} in the {domain} sector. Start with "{greeting}", briefly state Morphius AI's relevance to their industry, and express interest in connecting. Keep it under 150 words. End with: "Best regards,\nAasrith\nEmployee, Morphius AI\nhttps://www.morphius.in/" """
        response = client.chat.completions.create(model="gpt-3.5-turbo", messages=[{"role": "user", "content": prompt}], max_tokens=300, temperature=0.75)
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"⚠️ OpenAI API failed ({e}). Using fallback template.")
        body = f"I came across your profile and was interested in your work. At Morphius AI, we build AI solutions to tackle challenges across various industries, and I'm always keen to connect with professionals like yourself to learn more about your experience."
        signature = "\n\nBest regards,\nAasrith\nEmployee, Morphius AI\nhttps://www.morphius.in/"
        return f"{greeting}\n\n{body}{signature}"

def send_email(conn, to_email, subject, body, event_type="sent"):
    """Sends an email via SMTP and logs the event."""
    try:
        msg = MIMEMultipart()
        msg["From"], msg["To"], msg["Subject"] = SENDER_EMAIL, to_email, subject
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(); server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, to_email, msg.as_string())
        log_event(conn, event_type, to_email, subject, body, "success")
        print(f"✅ Email ({event_type}) sent successfully to {to_email}")
        return True
    except Exception as e:
        print(f"❌ Failed to send email to {to_email}: {e}")
        log_event(conn, event_type, to_email, subject, body, "failed")
        return False

def check_interest(body):
    """Classifies email sentiment using OpenAI with a keyword fallback."""
    try:
        prompt = f"Analyze this email reply. Respond with one word: 'positive', 'negative', or 'neutral'.\n\nEmail: \"{body}\""
        response = client.chat.completions.create(model="gpt-3.5-turbo", messages=[{"role": "user", "content": prompt}], max_tokens=5, temperature=0)
        interest = response.choices[0].message.content.strip().lower().replace(".", "")
        return interest if interest in ["positive", "negative", "neutral"] else "neutral"
    except Exception:
        body_lower = body.lower()
        if any(k in body_lower for k in ["not interested", "unsubscribe", "remove me"]): return "negative"
        if any(k in body_lower for k in ["interested", "schedule", "connect"]): return "positive"
        return "neutral"

def process_replies(conn):
    """Fetches unread emails, classifies them, sends a reply, and logs interactions."""
    print("\n--- Checking for new replies ---")
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT); mail.login(SENDER_EMAIL, SENDER_PASSWORD); mail.select("inbox")
        _, data = mail.search(None, '(UNSEEN)'); unread_ids = data[0].split()
        if not unread_ids:
            print("No new replies to process."); mail.logout(); return
        
        print(f"Found {len(unread_ids)} new email(s).")
        for e_id in unread_ids:
            _, msg_data = mail.fetch(e_id, '(RFC822)'); msg = email.message_from_bytes(msg_data[0][1])
            from_addr = email.utils.parseaddr(msg["From"])[1]; subject = msg["Subject"]
            body = next((part.get_payload(decode=True).decode(errors='ignore') for part in msg.walk() if part.get_content_type() == 'text/plain'), msg.get_payload(decode=True).decode(errors='ignore'))
            
            print(f"Processing reply from: {from_addr}")
            log_event(conn, "received", from_addr, subject, body, "success", mail_id=e_id.decode())
            interest = check_interest(body); print(f"-> Interest level: {interest}")
            
            if interest == "positive":
                reply_body = f"Hi,\n\nThank you for your positive response! I'm glad to hear you're interested.\n\nYou can book a meeting with me directly here: {CALENDLY_LINK}\n\nI look forward to speaking with you.\n\nBest regards,\nAasrith"
            else:
                reply_body = f"Hi,\n\nThank you for getting back to me. I understand.\n\nIn case you're interested, we also offer other services which you can explore here: {OTHER_SERVICES_LINK}\n\nBest regards,\nAasrith"
            
            send_email(conn, from_addr, f"Re: {subject}", reply_body, event_type=f"replied_{interest}")
            mail.store(e_id, '+FLAGS', '\\Seen')
        mail.logout(); print("✅ Finished processing replies.")
    except Exception as e:
        print(f"❌ Failed to process emails: {e}")