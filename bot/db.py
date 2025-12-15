import psycopg2
from decouple import config

DB_NAME = config('DB_NAME')
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_PORT = config('DB_PORT')


def get_connection():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, host=DB_HOST, port=DB_PORT,
        sslmode='require'
    )


def save_call_schedule(text_lines: list[str]):
    full_text = "\n".join(text_lines).strip()

    if not full_text:
        return

    conn = get_connection()

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO call_schedule (content) VALUES (%s);",
                (full_text,)
            )


def get_call_schedule() -> str:
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT content FROM call_schedule ORDER BY created_at DESC LIMIT 1;"
        )
        row = cur.fetchone()

    return row[0] if row else "⚠️ This information is missing from the database!"
