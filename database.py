import os
import psycopg2
import pandas as pd

try:
    POSTGRES_URL = os.environ["POSTGRES_URL"]
except KeyError:
    print("FATAL ERROR: The 'POSTGRES_URL' environment variable is not set.")
    exit(1)

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        return psycopg2.connect(POSTGRES_URL)
    except psycopg2.OperationalError as e:
        print(f"❌ Database Connection Error: {e}")
        return None

def setup_database_tables():
    """Ensures all required tables and columns exist in the database."""
    conn = get_db_connection()
    if not conn:
        print("❌ Could not connect to database to run setup.")
        return

    print("Checking database schema...")
    try:
        with conn.cursor() as cur:
            # Contacts tables
            cur.execute("""
                CREATE TABLE IF NOT EXISTS contacts (
                    id SERIAL PRIMARY KEY, name TEXT, linkedin_url TEXT,
                    work_emails TEXT, personal_emails TEXT, phones TEXT,
                    domain TEXT, created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cleaned_contacts (
                    id SERIAL PRIMARY KEY, name TEXT, linkedin_url TEXT UNIQUE NOT NULL,
                    work_emails TEXT, personal_emails TEXT, phones TEXT,
                    domain TEXT, created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            # Email logs table with all columns
            cur.execute("""
                CREATE TABLE IF NOT EXISTS email_logs (
                    id SERIAL PRIMARY KEY, timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    event_type VARCHAR(50), recipient_email TEXT, subject TEXT,
                    body TEXT, status VARCHAR(50), interest_level VARCHAR(50), mail_id TEXT
                );
            """)
            # Unsubscribe list table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS unsubscribe_list (
                    id SERIAL PRIMARY KEY, email TEXT UNIQUE NOT NULL, reason TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
        conn.commit()
        print("✅ Database schema is up to date.")
    except Exception as e:
        print(f"❌ Failed to set up database tables: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()

def fetch_new_contacts_for_outreach(conn):
    """Fetches cleaned contacts who have not yet been sent an initial email."""
    query = """
    SELECT cc.*
    FROM cleaned_contacts cc
    LEFT JOIN email_logs el ON cc.work_emails = el.recipient_email OR cc.personal_emails = el.recipient_email
    WHERE el.id IS NULL;
    """
    try:
        df = pd.read_sql(query, conn)
        return df
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"⚠️ Could not fetch contacts for outreach. Error: {error}")
        return pd.DataFrame()

def log_event(conn, event_type, email_addr, subject, body, status, interest_level=None, mail_id=None):
    """Generic function to log any email event to the database."""
    try:
        sql = "INSERT INTO email_logs (event_type, recipient_email, subject, body, status, interest_level, mail_id) VALUES (%s, %s, %s, %s, %s, %s, %s);"
        with conn.cursor() as cur:
            cur.execute(sql, (event_type, email_addr, subject, body, status, interest_level, mail_id))
        conn.commit()
    except Exception as e:
        print(f"❌ Failed to log event to database: {e}")
        conn.rollback()