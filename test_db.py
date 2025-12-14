import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_database_connection():
    """Test database connection and show database statistics"""
    db_type = os.getenv('DB_CONNECTION', 'questdb').lower()
    print(f"DB_CONNECTION: {db_type}")

    try:
        if db_type == 'postgresql':
            # Test PostgreSQL connection
            database_url = os.getenv("DATABASE_URL")
            print(f"DATABASE_URL: {database_url}")
            if database_url:
                print("Attempting connection with DATABASE_URL...")
                conn = psycopg2.connect(database_url)
            else:
                print("DATABASE_URL not found, using individual env vars...")
                conn = psycopg2.connect(
                    host=os.getenv('POSTGRES_HOST', 'localhost'),
                    user=os.getenv('POSTGRES_USER', 'postgres'),
                    password=os.getenv('POSTGRES_PASSWORD', ''),
                    database=os.getenv('POSTGRES_DATABASE', 'joai_db'),
                    port=int(os.getenv('POSTGRES_PORT', 5432))
                )

            # Add connection success logging
            print("Connection established successfully!")

            cursor = conn.cursor()

            # Get total count
            cursor.execute('SELECT COUNT(*) FROM crypto_candles')
            total_count = cursor.fetchone()[0]
            print(f'Total candles in database: {total_count}')

            # Get symbols and their counts
            cursor.execute('SELECT symbol, COUNT(*) as count FROM crypto_candles GROUP BY symbol ORDER BY symbol')
            symbols = cursor.fetchall()
            print('Symbols in database:')
            for symbol, count in symbols:
                print(f'  {symbol}: {count} candles')

            # Get latest entries
            cursor.execute('SELECT symbol, timestamp, open, high, low, close, volume FROM crypto_candles ORDER BY timestamp DESC LIMIT 5')
            latest = cursor.fetchall()
            print('\nLatest 5 entries:')
            for row in latest:
                print(f'  {row[0]} | {row[1]} | O:{row[2]} H:{row[3]} L:{row[4]} C:{row[5]} V:{row[6]}')

            # Check api_logs table
            cursor.execute('SELECT COUNT(*) FROM api_logs')
            api_logs_count = cursor.fetchone()[0]
            print(f'\nAPI logs in database: {api_logs_count}')

            cursor.close()
            conn.close()
            print("PostgreSQL connection test completed successfully!")

        elif db_type == 'mysql':
            print("MySQL testing commented out - focusing on PostgreSQL")
            print(f"Unsupported database type: {db_type}")
        else:
            print(f"Unsupported database type: {db_type}")

    except Exception as e:
        print(f"Connection failed with error: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_database_connection()