import os
import requests
import psycopg2

try:
    CONTACTOUT_API_TOKEN = os.environ["CONTACTOUT_API_TOKEN"]
except KeyError:
    print("FATAL ERROR: The 'CONTACTOUT_API_TOKEN' environment variable is not set.")
    exit(1)

API_BASE = "https://api.contactout.com/v1/people/enrich"

def enrich_and_save_contact(conn, payload):
    """Enriches a contact using the API and saves it to the database."""
    headers = { "Content-Type": "application/json", "Accept": "application/json", "token": CONTACTOUT_API_TOKEN }
    print(f"🔄 Calling ContactOut API for: {payload}")
    try:
        resp = requests.post(API_BASE, headers=headers, json=payload)
        
        if resp.status_code == 200:
            profile = resp.json().get("profile", resp.json())
            linkedin_url = profile.get("linkedin_url", payload.get("linkedin_url", "")).rstrip('/')
            
            enriched_data = {
                "name": profile.get("full_name"), "linkedin_url": linkedin_url,
                "work_emails": ", ".join(profile.get("work_email", [])),
                "personal_emails": ", ".join(profile.get("personal_email", [])),
                "phones": ", ".join(profile.get("phone", [])),
                "domain": profile.get("company", {}).get("domain") if profile.get("company") else None
            }
            
            save_to_raw_contacts(conn, enriched_data)
            save_to_cleaned_contacts(conn, enriched_data)
            conn.commit()
            print(f"✅ Successfully enriched and saved: {enriched_data.get('name')}")
        elif resp.status_code == 404:
            print("🟡 Contact Not Found.")
        else:
            print(f"❌ ContactOut API Error (Code: {resp.status_code}): {resp.text}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error contacting ContactOut API: {e}")
        conn.rollback()
    except Exception as e:
        print(f"❌ Unexpected error during enrichment: {e}")
        conn.rollback()

def save_to_raw_contacts(conn, data):
    sql = "INSERT INTO contacts (name, linkedin_url, work_emails, personal_emails, phones, domain) VALUES (%s, %s, %s, %s, %s, %s);"
    data_tuple = (data.get("name"), data.get("linkedin_url"), data.get("work_emails"), data.get("personal_emails"), data.get("phones"), data.get("domain"))
    with conn.cursor() as cur: cur.execute(sql, data_tuple)

def save_to_cleaned_contacts(conn, data):
    if not data.get("linkedin_url"):
        print("⚠️ Skipped saving to cleaned: LinkedIn URL is missing.")
        return
    sql = "INSERT INTO cleaned_contacts (name, linkedin_url, work_emails, personal_emails, phones, domain) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (linkedin_url) DO NOTHING;"
    data_tuple = (data.get("name"), data.get("linkedin_url"), data.get("work_emails"), data.get("personal_emails"), data.get("phones"), data.get("domain"))
    with conn.cursor() as cur:
        cur.execute(sql, data_tuple)
        if cur.rowcount > 0: print(f"ℹ️ Added new unique contact '{data.get('name')}'")