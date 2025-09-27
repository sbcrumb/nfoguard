#!/usr/bin/env python3
"""
Database management for NFOGuard
Handles SQLite database operations for tracking media dates and processing history
"""
import sqlite3
import json
import threading
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Any
from contextlib import contextmanager

class NFOGuardDatabase:
    """Manages NFOGuard SQLite database operations"""
    
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_database()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection"""
        if not hasattr(self._local, 'connection'):
            self._local.connection = sqlite3.connect(
                self.db_path, 
                check_same_thread=False,
                timeout=30.0
            )
            self._local.connection.row_factory = sqlite3.Row
        return self._local.connection
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    
    def _init_database(self):
        """Initialize database tables with migration support"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Series table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS series (
                    imdb_id TEXT PRIMARY KEY,
                    path TEXT NOT NULL,
                    last_updated TEXT NOT NULL,
                    metadata TEXT
                )
            """)
            
            # Episodes table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS episodes (
                    imdb_id TEXT NOT NULL,
                    season INTEGER NOT NULL,
                    episode INTEGER NOT NULL,
                    aired TEXT,
                    dateadded TEXT,
                    source TEXT,
                    last_updated TEXT NOT NULL,
                    PRIMARY KEY (imdb_id, season, episode),
                    FOREIGN KEY (imdb_id) REFERENCES series(imdb_id)
                )
            """)
            
            # Movies table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS movies (
                    imdb_id TEXT PRIMARY KEY,
                    path TEXT NOT NULL,
                    released TEXT,
                    dateadded TEXT,
                    source TEXT,
                    last_updated TEXT NOT NULL
                )
            """)
            
            # Processing history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processing_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    imdb_id TEXT NOT NULL,
                    media_type TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    details TEXT
                )
            """)
            
            # Add missing columns if they don't exist (migration)
            # Check current schema and add missing columns
            cursor.execute("PRAGMA table_info(movies)")
            movie_columns = [row[1] for row in cursor.fetchall()]
            
            cursor.execute("PRAGMA table_info(episodes)")
            episode_columns = [row[1] for row in cursor.fetchall()]
            
            # Add missing columns to movies table
            if 'path' not in movie_columns:
                cursor.execute("ALTER TABLE movies ADD COLUMN path TEXT")
                cursor.execute("UPDATE movies SET path = '/unknown/path/' || imdb_id WHERE path IS NULL")
            
            if 'has_video_file' not in movie_columns:
                cursor.execute("ALTER TABLE movies ADD COLUMN has_video_file BOOLEAN DEFAULT FALSE")
            
            if 'last_updated' not in movie_columns:
                cursor.execute("ALTER TABLE movies ADD COLUMN last_updated TEXT")
                cursor.execute("UPDATE movies SET last_updated = datetime('now') WHERE last_updated IS NULL")
            
            # Add missing columns to episodes table  
            if 'has_video_file' not in episode_columns:
                cursor.execute("ALTER TABLE episodes ADD COLUMN has_video_file BOOLEAN DEFAULT FALSE")
            
            if 'last_updated' not in episode_columns:
                cursor.execute("ALTER TABLE episodes ADD COLUMN last_updated TEXT")
                cursor.execute("UPDATE episodes SET last_updated = datetime('now') WHERE last_updated IS NULL")
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_episodes_imdb ON episodes(imdb_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_episodes_video ON episodes(has_video_file)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_movies_video ON movies(has_video_file)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_imdb ON processing_history(imdb_id)")
    
    def upsert_series(self, imdb_id: str, path: str, metadata: Optional[Dict] = None):
        """Insert or update series record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO series (imdb_id, path, last_updated, metadata)
                VALUES (?, ?, ?, ?)
            """, (imdb_id, path, datetime.utcnow().isoformat(), json.dumps(metadata) if metadata else None))
    
    def upsert_episode_date(self, imdb_id: str, season: int, episode: int, 
                           aired: Optional[str], dateadded: Optional[str], 
                           source: str, has_video_file: bool = False):
        """Insert or update episode date record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO episodes 
                (imdb_id, season, episode, aired, dateadded, source, has_video_file, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (imdb_id, season, episode, aired, dateadded, source, has_video_file, datetime.utcnow().isoformat()))
    
    def upsert_movie(self, imdb_id: str, path: str):
        """Insert or update movie record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO movies (imdb_id, path, last_updated)
                    VALUES (?, ?, ?)
                """, (imdb_id, path, datetime.utcnow().isoformat()))
            except sqlite3.OperationalError as e:
                if "no column named path" in str(e):
                    # Fallback for databases without path column - just insert imdb_id
                    cursor.execute("""
                        INSERT OR REPLACE INTO movies (imdb_id, last_updated)
                        VALUES (?, ?)
                    """, (imdb_id, datetime.utcnow().isoformat()))
                else:
                    raise
    
    def upsert_movie_dates(self, imdb_id: str, released: Optional[str], 
                          dateadded: Optional[str], source: str, has_video_file: bool = False):
        """Insert or update movie date record"""
        print(f"🔍 DATABASE UPSERT: imdb_id={imdb_id}, dateadded={dateadded}, source={source}")
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # Use INSERT OR REPLACE to ensure we always update the dates properly
            cursor.execute("""
                INSERT OR REPLACE INTO movies (imdb_id, path, released, dateadded, source, has_video_file, last_updated)
                VALUES (
                    ?, 
                    COALESCE((SELECT path FROM movies WHERE imdb_id = ?), 'unknown'),
                    ?, ?, ?, ?, ?
                )
            """, (imdb_id, imdb_id, released, dateadded, source, has_video_file, datetime.utcnow().isoformat()))
            
            # Debug: Check what was actually saved
            cursor.execute("SELECT dateadded, source FROM movies WHERE imdb_id = ?", (imdb_id,))
            result = cursor.fetchone()
            print(f"🔍 DATABASE VERIFY: After upsert, found dateadded={result[0] if result else 'NOT_FOUND'}, source={result[1] if result else 'NOT_FOUND'}")
    
    def get_series_episodes(self, imdb_id: str, has_video_file_only: bool = False) -> List[Dict]:
        """Get all episodes for a series"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM episodes WHERE imdb_id = ?"
            params = [imdb_id]
            
            if has_video_file_only:
                query += " AND has_video_file = TRUE"
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_episode_date(self, imdb_id: str, season: int, episode: int) -> Optional[Dict]:
        """Get episode date record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM episodes 
                WHERE imdb_id = ? AND season = ? AND episode = ?
            """, (imdb_id, season, episode))
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_movie_dates(self, imdb_id: str) -> Optional[Dict]:
        """Get movie date record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM movies WHERE imdb_id = ?", (imdb_id,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def add_processing_history(self, imdb_id: str, media_type: str, event_type: str, details: Optional[Dict] = None):
        """Add processing history entry"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO processing_history (imdb_id, media_type, event_type, processed_at, details)
                VALUES (?, ?, ?, ?, ?)
            """, (imdb_id, media_type, event_type, datetime.utcnow().isoformat(), 
                  json.dumps(details) if details else None))
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Series stats
            cursor.execute("SELECT COUNT(*) FROM series")
            series_count = cursor.fetchone()[0]
            
            # Episode stats
            cursor.execute("SELECT COUNT(*) FROM episodes")
            episodes_total = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM episodes WHERE has_video_file = TRUE")
            episodes_with_video = cursor.fetchone()[0]
            
            # Movie stats
            cursor.execute("SELECT COUNT(*) FROM movies")
            movies_total = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM movies WHERE has_video_file = TRUE")
            movies_with_video = cursor.fetchone()[0]
            
            # Processing history
            cursor.execute("SELECT COUNT(*) FROM processing_history")
            history_count = cursor.fetchone()[0]
            
            return {
                "series_count": series_count,
                "episodes_total": episodes_total,
                "episodes_with_video": episodes_with_video,
                "movies_total": movies_total,
                "movies_with_video": movies_with_video,
                "processing_history_count": history_count,
                "database_size_mb": round(self.db_path.stat().st_size / 1024 / 1024, 2)
            }