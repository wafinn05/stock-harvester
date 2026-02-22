import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv


# LOAD 

try:
    load_dotenv()
    print(".env file loaded successfully")
except Exception as e:
    print(f"Warning: Could not load .env file: {e}")
    print("   Using default/empty environment variables")

DB_USER = os.getenv("DB_USER", "app_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stock_prediction_db")

print("\n" + "=" * 50)
print("DATABASE CONFIGURATION")
print("=" * 50)
print(f"   Host: {DB_HOST}:{DB_PORT}")
print(f"   Database: {DB_NAME}")
print(f"   User: {DB_USER}")
print(f"   Password: {'*' * len(DB_PASSWORD) if DB_PASSWORD else 'NOT SET'}")
print("=" * 50)


#  URL
DATABASE_URL = (
    f"postgresql+pg8000://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


#CREATE ENGINE 
engine = None
SessionLocal = None

try:
    print("Creating database engine...")
    engine = create_engine(
        DATABASE_URL,
        echo=False,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={
            "timeout": 10,
        },
    )

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

except Exception as e:
    print(f"\nCRITICAL: Database engine creation failed: {e}")
    engine = None
    SessionLocal = None

def verify_db_connection():
    """Verify connection without crashing the app."""
    if engine is None:
        return False
    try:
        print("Testing database connection...")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Database connection successful!")
        return True
    except Exception as e:
        print(f"Database connection test failed: {e}")
        return False


# 5. PUBLIC API
def get_db():
    if SessionLocal is None:
        raise ConnectionError("Database not connected.")
    db = SessionLocal()
    try:
        return db
    finally:
        db.close()


#TEST
def test_connection():
    if engine is None:
        print("Engine not initialized")
        return False
    try:
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version()")).fetchone()[0]
            print("Connected:", version.split(",")[0])
        return True
    except Exception as e:
        print("Connection test failed:", e)
        return False

def get_db_connection():
    """Returns a raw DBAPI connection (pg8000) for direct cursor usage."""
    if engine is None:
        raise ConnectionError("Database engine not initialized.")
    return engine.raw_connection()


if __name__ == "__main__":
    test_connection()
