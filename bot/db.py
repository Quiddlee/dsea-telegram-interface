import psycopg2
from psycopg2.extras import Json
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
        sslmode='prefer'
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


def save_document_record(
    *,
    source_type: str,
    source_id: str,
    url: str | None,
    title: str | None,
    mime_type: str | None,
    checksum: str,
    status: str,
    raw_path: str,
    last_error: str | None,
    parsed_at,
) -> str | None:
    conn = get_connection()

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO documents (
                    source_type,
                    source_id,
                    url,
                    title,
                    mime_type,
                    checksum,
                    status,
                    raw_path,
                    last_error,
                    parsed_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_type, source_id) DO UPDATE SET
                    url = EXCLUDED.url,
                    title = EXCLUDED.title,
                    mime_type = EXCLUDED.mime_type,
                    checksum = EXCLUDED.checksum,
                    status = EXCLUDED.status,
                    raw_path = EXCLUDED.raw_path,
                    last_error = EXCLUDED.last_error,
                    parsed_at = EXCLUDED.parsed_at,
                    updated_at = NOW()
                WHERE documents.checksum IS DISTINCT FROM EXCLUDED.checksum
                RETURNING id;
                """,
                (
                    source_type,
                    source_id,
                    url,
                    title,
                    mime_type,
                    checksum,
                    status,
                    raw_path,
                    last_error,
                    parsed_at,
                ),
            )
            row = cur.fetchone()

    return row[0] if row else None


def get_document_checksum(*, source_type: str, source_id: str) -> str | None:
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT checksum
            FROM documents
            WHERE source_type = %s AND source_id = %s
            LIMIT 1;
            """,
            (source_type, source_id),
        )
        row = cur.fetchone()

    return row[0] if row else None


def enqueue_chunk_document_job(document_id: str) -> None:
    conn = get_connection()

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pgboss.job (name, data) VALUES (%s, %s);",
                ("JOB_CHUNK_DOCUMENT", Json({"documentId": document_id})),
            )
