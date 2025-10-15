import pandas as pd
import datetime
from emailing import send_email, OTHER_SERVICES_LINK

def send_follow_ups(conn):
    """Sends a follow-up to contacts who haven't replied after a set time."""
    print("\n--- Running Follow-Up Task ---")
    two_days_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2)
    
    query = """
    SELECT el1.recipient_email
    FROM email_logs el1
    WHERE el1.event_type = 'sent'
      AND el1.timestamp < %s
      AND NOT EXISTS (
          SELECT 1 FROM email_logs el2
          WHERE el2.recipient_email = el1.recipient_email
            AND (el2.event_type LIKE 'replied%%' OR el2.event_type = 'follow_up_sent')
      )
      AND el1.recipient_email NOT IN (SELECT email FROM unsubscribe_list);
    """
    
    candidates = pd.read_sql(query, conn, params=(two_days_ago,))
    if candidates.empty:
        print("No contacts need a follow-up."); return
        
    print(f"Found {len(candidates)} contacts for follow-up.")
    subject = "Quick Follow-Up"
    body = f"Hi,\n\nJust wanted to quickly follow up on my previous email. If it's not the right time, no worries.\n\nWe also have other services you might find interesting: {OTHER_SERVICES_LINK}\n\nBest regards,\nAasrith"
    
    for email_addr in candidates['recipient_email']:
        send_email(conn, email_addr, subject, body, event_type="follow_up_sent")

def process_unsubscribes(conn):
    """Auto-unsubscribes contacts who received multiple emails with no reply."""
    print("\n--- Running Unsubscribe Task ---")
    query = """
    SELECT recipient_email
    FROM email_logs
    WHERE event_type IN ('sent', 'follow_up_sent')
      AND recipient_email NOT IN (SELECT DISTINCT recipient_email FROM email_logs WHERE event_type LIKE 'replied%%')
      AND recipient_email NOT IN (SELECT email FROM unsubscribe_list)
    GROUP BY recipient_email
    HAVING COUNT(*) >= 3;
    """
    candidates = pd.read_sql(query, conn)
    if candidates.empty:
        print("No contacts to unsubscribe."); return

    print(f"Found {len(candidates)} contacts to unsubscribe.")
    with conn.cursor() as cur:
        for email_addr in candidates['recipient_email']:
            print(f"🚫 Unsubscribing {email_addr}")
            cur.execute("INSERT INTO unsubscribe_list (email, reason) VALUES (%s, %s) ON CONFLICT (email) DO NOTHING;", (email_addr, "No reply after 3 emails"))
    conn.commit()
