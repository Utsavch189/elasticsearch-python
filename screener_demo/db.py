import psycopg2

INSIGHTS_DB_USER="utsav"
INSIGHTS_DB_PASSWORD='nA9#[DgSJ0u0Y5'
INSIGHTS_DB_NAME="accord"
INSIGHTS_DB_HOST="127.0.0.1"
INSIGHTS_DB_PORT=7463

def get_connection():
    try:
        con = psycopg2.connect(
            database=INSIGHTS_DB_NAME,
            user=INSIGHTS_DB_USER,
            password=INSIGHTS_DB_PASSWORD,
            host=INSIGHTS_DB_HOST,
            port=INSIGHTS_DB_PORT
        )
        cursor = con.cursor()
        return con, cursor
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise
