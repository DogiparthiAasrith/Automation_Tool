import argparse
import time
import pandas as pd
from database import get_db_connection, setup_database_tables, fetch_new_contacts_for_outreach
from enrichment import enrich_and_save_contact
from emailing import generate_email_body, send_email, process_replies
from automation import send_follow_ups, process_unsubscribes

def handle_enrich(args):
    """Enriches a single contact."""
    conn = get_db_connection();
    if not conn: return
    payload = {"include": ["work_email", "personal_email", "phone"]}
    if args.linkedin_url: payload["linkedin_url"] = args.linkedin_url
    elif args.email: payload["email"] = args.email
    else: print("Error: Provide either --linkedin_url or --email."); return
    enrich_and_save_contact(conn, payload)
    conn.close()

def handle_send_outreach(args):
    """Sends initial emails to all new contacts."""
    print("--- Starting Initial Outreach Task ---")
    conn = get_db_connection();
    if not conn: return
    contacts = fetch_new_contacts_for_outreach(conn)
    if contacts.empty:
        print("No new contacts to email."); conn.close(); return

    print(f"Found {len(contacts)} new contacts to email.")
    for _, contact in contacts.iterrows():
        to_email = contact.get('work_emails') or contact.get('personal_emails')
        if not to_email or pd.isna(to_email):
            print(f"Skipping {contact.get('name')}: missing email."); continue
        
        print(f"\nProcessing: {contact.get('name')} ({to_email})")
        body = generate_email_body(contact.to_dict())
        send_email(conn, to_email, "Connecting from Morphius AI", body)
        time.sleep(2)  # Rate limiting
    conn.close(); print("\n--- Initial Outreach Task Complete ---")

def handle_process_replies(args):
    """Checks inbox and processes replies, running once or continuously."""
    conn = get_db_connection();
    if not conn: return
    if args.daemon:
        print("--- Starting Reply Processor in Daemon Mode (runs every 60s) ---")
        while True:
            process_replies(conn)
            print("--- Sleeping for 60 seconds ---"); time.sleep(60)
    else:
        process_replies(conn)
    conn.close()

def handle_run_automations(args):
    """Runs the follow-up and unsubscribe automations."""
    conn = get_db_connection();
    if not conn: return
    send_follow_ups(conn)
    process_unsubscribes(conn)
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Morphius AI Backend CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Database setup command
    subparsers.add_parser("setup-db", help="Initialize the database tables.")
    
    # Enrich command
    p_enrich = subparsers.add_parser("enrich", help="Enrich a single contact.")
    p_enrich.add_argument("--linkedin_url", type=str)
    p_enrich.add_argument("--email", type=str)

    # Send outreach command
    subparsers.add_parser("send-outreach", help="Send initial emails to new contacts.")
    
    # Process replies command
    p_replies = subparsers.add_parser("process-replies", help="Check inbox and process replies.")
    p_replies.add_argument("--daemon", action="store_true", help="Run continuously.")
    
    # Run automations command
    subparsers.add_parser("run-automations", help="Send follow-ups and process unsubscribes.")

    args = parser.parse_args()

    if args.command == "setup-db":
        setup_database_tables()
    elif args.command == "enrich":
        handle_enrich(args)
    elif args.command == "send-outreach":
        handle_send_outreach(args)
    elif args.command == "process-replies":
        handle_process_replies(args)
    elif args.command == "run-automations":
        handle_run_automations(args)

if __name__ == "__main__":
    main()


