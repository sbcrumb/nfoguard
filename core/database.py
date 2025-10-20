#!/usr/bin/env python3
"""
PostgreSQL database management for NFOGuard
Handles database operations for tracking media dates and processing history
"""
import json
import threading
from datetime import datetime
from typing import Optional, Dict, List, Any
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

class NFOGuardDatabase:
    """PostgreSQL database manager for NFOGuard media tracking and processing history"""
    
    def __init__(self, config):
        """
        Initialize PostgreSQL database connection
        
        Args:
            config: Configuration object with database settings
        """
        if not config:
            raise ValueError("PostgreSQL configuration is required")
        self.db_host = config.db_host
        self.db_port = config.db_port
        self.db_name = config.db_name
        self.db_user = config.db_user
        self.db_password = config.db_password
        self.db_type = "postgresql"  # NFOGuard uses PostgreSQL
        
        self._local = threading.local()
        self._init_database()
    
    
    def _get_connection(self) -> 'psycopg2.extensions.connection':
        """Get thread-local PostgreSQL database connection"""
        if not hasattr(self._local, 'connection'):
            self._local.connection = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            self._local.connection.autocommit = True
        return self._local.connection
    
    def _get_first_value(self, row):
        """Get first value from row from PostgreSQL RealDictCursor"""
        # RealDictCursor returns dict-like objects
        return list(row.values())[0] if row else None
    
    @contextmanager
    def get_connection(self):
        """Context manager for PostgreSQL database connections"""
        conn = self._get_connection()
        try:
            yield conn
            # PostgreSQL uses autocommit - no manual commit needed
        except Exception:
            # PostgreSQL uses autocommit - no manual rollback needed
            raise
    
    def _init_database(self):
        """Initialize PostgreSQL database tables"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            self._init_postgresql_tables(cursor)
            
            # Test the connection works and verify autocommit
            cursor.execute("SELECT 1")
            print(f"✅ PostgreSQL database initialized and connection verified")
            print(f"🔍 Autocommit status: {conn.autocommit}")
    
    def _init_postgresql_tables(self, cursor):
        """Initialize database tables"""
        # Series table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS series (
                imdb_id VARCHAR(20) PRIMARY KEY,
                path TEXT NOT NULL,
                last_updated TIMESTAMP NOT NULL,
                metadata JSONB
            )
        """)
        
        # Episodes table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS episodes (
                imdb_id VARCHAR(20) NOT NULL,
                season INTEGER NOT NULL,
                episode INTEGER NOT NULL,
                aired DATE,
                dateadded TIMESTAMP,
                source VARCHAR(100),
                last_updated TIMESTAMP NOT NULL,
                has_video_file BOOLEAN DEFAULT FALSE,
                PRIMARY KEY (imdb_id, season, episode),
                FOREIGN KEY (imdb_id) REFERENCES series(imdb_id)
            )
        """)
        
        # Movies table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                imdb_id VARCHAR(20) PRIMARY KEY,
                path TEXT NOT NULL,
                released DATE,
                dateadded TIMESTAMP,
                source VARCHAR(100),
                last_updated TIMESTAMP NOT NULL,
                has_video_file BOOLEAN DEFAULT FALSE
            )
        """)
        
        # Processing history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processing_history (
                id SERIAL PRIMARY KEY,
                imdb_id VARCHAR(20) NOT NULL,
                media_type VARCHAR(20) NOT NULL,
                event_type VARCHAR(50) NOT NULL,
                processed_at TIMESTAMP NOT NULL,
                details TEXT
            )
        """)
        
        # Create indexes for PostgreSQL
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_episodes_imdb ON episodes(imdb_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_episodes_video ON episodes(has_video_file)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_movies_video ON movies(has_video_file)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_imdb ON processing_history(imdb_id)")
    def upsert_series(self, imdb_id: str, path: str, metadata: Optional[Dict] = None):
        """Insert or update series record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            timestamp = datetime.utcnow()
            
            cursor.execute("""
                INSERT INTO series (imdb_id, path, last_updated, metadata)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (imdb_id) DO UPDATE SET
                    path = EXCLUDED.path,
                    last_updated = EXCLUDED.last_updated,
                    metadata = EXCLUDED.metadata
            """, (imdb_id, path, timestamp, json.dumps(metadata) if metadata else None))
    
    def upsert_episode_date(self, imdb_id: str, season: int, episode: int, 
                           aired: Optional[str], dateadded: Optional[str], 
                           source: str, has_video_file: bool = False):
        """Insert or update episode date record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            timestamp = datetime.utcnow()
            
            cursor.execute("""
                INSERT INTO episodes 
                (imdb_id, season, episode, aired, dateadded, source, has_video_file, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (imdb_id, season, episode) DO UPDATE SET
                    aired = EXCLUDED.aired,
                    dateadded = EXCLUDED.dateadded,
                    source = EXCLUDED.source,
                    has_video_file = EXCLUDED.has_video_file,
                    last_updated = EXCLUDED.last_updated
            """, (imdb_id, season, episode, aired, dateadded, source, has_video_file, timestamp))
            import os
            if os.environ.get("DEBUG", "false").lower() == "true":
                print(f"🔍 DEBUG: PostgreSQL upsert executed for {imdb_id} S{season:02d}E{episode:02d}, rows affected: {cursor.rowcount}")
    
    def upsert_movie(self, imdb_id: str, path: str):
        """Insert or update movie record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            timestamp = datetime.utcnow()
            
            cursor.execute("""
                INSERT INTO movies (imdb_id, path, last_updated)
                VALUES (%s, %s, %s)
                ON CONFLICT (imdb_id) DO UPDATE SET
                    path = EXCLUDED.path,
                    last_updated = EXCLUDED.last_updated
            """, (imdb_id, path, timestamp))
    
    def upsert_movie_dates(self, imdb_id: str, released: Optional[str], 
                          dateadded: Optional[str], source: str, has_video_file: bool = False):
        """Insert or update movie date record"""
        import os
        if os.environ.get("DEBUG", "false").lower() == "true":
            print(f"🔍 DATABASE UPSERT: imdb_id={imdb_id}, dateadded={dateadded}, source={source}")
        with self.get_connection() as conn:
            cursor = conn.cursor()
            timestamp = datetime.utcnow()
            
            cursor.execute("""
                INSERT INTO movies (imdb_id, path, released, dateadded, source, has_video_file, last_updated)
                VALUES (%s, COALESCE((SELECT path FROM movies WHERE imdb_id = %s), 'unknown'), %s, %s, %s, %s, %s)
                ON CONFLICT (imdb_id) DO UPDATE SET
                    released = EXCLUDED.released,
                    dateadded = EXCLUDED.dateadded,
                    source = EXCLUDED.source,
                    has_video_file = EXCLUDED.has_video_file,
                    last_updated = EXCLUDED.last_updated
            """, (imdb_id, imdb_id, released, dateadded, source, has_video_file, timestamp))
            
            # Debug: Check what was actually saved
            cursor.execute("SELECT dateadded, source FROM movies WHERE imdb_id = %s", (imdb_id,))
            result = cursor.fetchone()
            import os
            if os.environ.get("DEBUG", "false").lower() == "true":
                print(f"🔍 DATABASE VERIFY: After upsert, found dateadded={result['dateadded'] if result else 'NOT_FOUND'}, source={result['source'] if result else 'NOT_FOUND'}")
    
    def get_series_episodes(self, imdb_id: str, has_video_file_only: bool = False) -> List[Dict]:
        """Get all episodes for a series"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM episodes WHERE imdb_id = %s"
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
                WHERE imdb_id = %s AND season = %s AND episode = %s
            """, (imdb_id, season, episode))
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_movie_dates(self, imdb_id: str) -> Optional[Dict]:
        """Get movie date record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM movies WHERE imdb_id = %s", (imdb_id,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def add_processing_history(self, imdb_id: str, media_type: str, event_type: str, details: Optional[Dict] = None):
        """Add processing history entry"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO processing_history (imdb_id, media_type, event_type, processed_at, details)
                VALUES (%s, %s, %s, %s, %s)
            """, (imdb_id, media_type, event_type, datetime.utcnow().isoformat(), 
                  json.dumps(details) if details else None))
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()  # Regular cursor for PostgreSQL
            
            # Series stats
            cursor.execute("SELECT COUNT(*) FROM series")
            series_count = self._get_first_value(cursor.fetchone())
            
            # Episode stats
            cursor.execute("SELECT COUNT(*) FROM episodes")
            episodes_total = self._get_first_value(cursor.fetchone())
            
            cursor.execute("SELECT COUNT(*) FROM episodes WHERE has_video_file = TRUE")
            episodes_with_video = self._get_first_value(cursor.fetchone())
            
            # Movie stats
            cursor.execute("SELECT COUNT(*) FROM movies")
            movies_total = self._get_first_value(cursor.fetchone())
            
            cursor.execute("SELECT COUNT(*) FROM movies WHERE has_video_file = TRUE")
            movies_with_video = self._get_first_value(cursor.fetchone())
            
            # Processing history
            cursor.execute("SELECT COUNT(*) FROM processing_history")
            history_count = self._get_first_value(cursor.fetchone())
            
            # Database size calculation for PostgreSQL
            cursor.execute("SELECT pg_database_size(%s)", (self.db_name,))
            db_size_bytes = self._get_first_value(cursor.fetchone())
            db_size_mb = round(db_size_bytes / 1024 / 1024, 2) if db_size_bytes else 0
            
            return {
                "series_count": series_count,
                "episodes_total": episodes_total,
                "episodes_with_video": episodes_with_video,
                "movies_total": movies_total,
                "movies_with_video": movies_with_video,
                "processing_history_count": history_count,
                "database_size_mb": db_size_mb,
                "database_type": "postgresql"
            }
    
    def delete_episode(self, imdb_id: str, season: int, episode: int) -> bool:
        """
        Delete a specific episode from the database
        
        Args:
            imdb_id: Series IMDb ID
            season: Season number
            episode: Episode number
            
        Returns:
            True if episode was deleted, False if not found
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                DELETE FROM episodes 
                WHERE imdb_id = %s AND season = %s AND episode = %s
            """, (imdb_id, season, episode))
            
            deleted_count = cursor.rowcount
            conn.commit()
            
            return deleted_count > 0
    
    def delete_series_episodes(self, imdb_id: str) -> int:
        """
        Delete all episodes for a series from the database
        
        Args:
            imdb_id: Series IMDb ID
            
        Returns:
            Number of episodes deleted
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                DELETE FROM episodes 
                WHERE imdb_id = %s
            """, (imdb_id,))
            
            deleted_count = cursor.rowcount
            conn.commit()
            
            return deleted_count
    
    def delete_orphaned_episodes(self) -> List[Dict]:
        """
        Find and delete episodes that don't have corresponding video files on disk
        This requires checking filesystem for each episode, so use carefully
        
        Returns:
            List of deleted episodes with their details
        """
        from utils.file_utils import find_episodes_on_disk
        from pathlib import Path
        
        deleted_episodes = []
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get all series with their paths
            cursor.execute("""
                SELECT DISTINCT imdb_id, path FROM series
            """)
            
            series_list = cursor.fetchall()
            
            for series in series_list:
                imdb_id = series['imdb_id']
                series_path = Path(series['path'])
                
                if not series_path.exists():
                    continue
                
                # Get episodes on disk
                disk_episodes = find_episodes_on_disk(series_path)
                disk_episode_keys = set(disk_episodes.keys())
                
                # Get episodes in database
                cursor.execute("""
                    SELECT season, episode, dateadded, source 
                    FROM episodes 
                    WHERE imdb_id = %s
                """, (imdb_id,))
                
                db_episodes = cursor.fetchall()
                
                # Find orphaned episodes (in DB but not on disk)
                for db_episode in db_episodes:
                    season = db_episode['season']
                    episode = db_episode['episode']
                    episode_key = (season, episode)
                    
                    if episode_key not in disk_episode_keys:
                        # Episode is orphaned - delete it
                        cursor.execute("""
                            DELETE FROM episodes 
                            WHERE imdb_id = %s AND season = %s AND episode = %s
                        """, (imdb_id, season, episode))
                        
                        deleted_episodes.append({
                            'imdb_id': imdb_id,
                            'season': season,
                            'episode': episode,
                            'dateadded': db_episode['dateadded'],
                            'source': db_episode['source'],
                            'series_path': str(series_path)
                        })
            
            conn.commit()
            
        return deleted_episodes
    
    def delete_movie(self, imdb_id: str) -> bool:
        """
        Delete a specific movie from the database
        
        Args:
            imdb_id: Movie IMDb ID
            
        Returns:
            True if movie was deleted, False if not found
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                DELETE FROM movies 
                WHERE imdb_id = %s
            """, (imdb_id,))
            
            deleted_count = cursor.rowcount
            conn.commit()
            
            return deleted_count > 0
    
    def delete_orphaned_movies(self) -> List[Dict]:
        """
        Find and delete movies that don't have corresponding video files on disk
        This requires checking filesystem for each movie, so use carefully
        
        Returns:
            List of deleted movies with their details
        """
        from pathlib import Path
        
        deleted_movies = []
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get all movies with their paths
            cursor.execute("""
                SELECT imdb_id, path, dateadded, source 
                FROM movies
            """)
            
            movies_list = cursor.fetchall()
            
            for movie in movies_list:
                imdb_id = movie['imdb_id']
                movie_path = Path(movie['path'])
                
                if not movie_path.exists():
                    # Movie directory doesn't exist - delete it
                    cursor.execute("""
                        DELETE FROM movies 
                        WHERE imdb_id = %s
                    """, (imdb_id,))
                    
                    deleted_movies.append({
                        'imdb_id': imdb_id,
                        'reason': 'directory_not_found',
                        'path': str(movie_path),
                        'dateadded': movie['dateadded'],
                        'source': movie['source']
                    })
                    continue
                
                # Check for video files
                video_exts = (".mkv", ".mp4", ".avi", ".mov", ".m4v")
                has_video = any(f.is_file() and f.suffix.lower() in video_exts 
                              for f in movie_path.iterdir() if f.is_file())
                
                if not has_video:
                    # No video files found - delete this movie
                    cursor.execute("""
                        DELETE FROM movies 
                        WHERE imdb_id = %s
                    """, (imdb_id,))
                    
                    deleted_movies.append({
                        'imdb_id': imdb_id,
                        'reason': 'no_video_files',
                        'path': str(movie_path),
                        'dateadded': movie['dateadded'],
                        'source': movie['source']
                    })
            
            conn.commit()
            
        return deleted_movies
    
    def delete_orphaned_series(self) -> List[Dict]:
        """
        Find and delete TV series that don't have corresponding directories on disk
        This requires checking filesystem for each series, so use carefully
        
        Returns:
            List of deleted series with their details
        """
        from pathlib import Path
        
        deleted_series = []
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get all series with their paths
            cursor.execute("""
                SELECT imdb_id, path, last_updated, metadata 
                FROM series
            """)
            
            series_list = cursor.fetchall()
            
            for series in series_list:
                imdb_id = series['imdb_id']
                series_path = Path(series['path'])
                
                if not series_path.exists():
                    # Series directory doesn't exist - delete the series and all its episodes
                    cursor.execute("""
                        DELETE FROM episodes 
                        WHERE imdb_id = %s
                    """, (imdb_id,))
                    episodes_deleted = cursor.rowcount
                    
                    cursor.execute("""
                        DELETE FROM series 
                        WHERE imdb_id = %s
                    """, (imdb_id,))
                    
                    deleted_series.append({
                        'imdb_id': imdb_id,
                        'reason': 'directory_not_found',
                        'path': str(series_path),
                        'last_updated': series['last_updated'],
                        'episodes_deleted': episodes_deleted
                    })
            
            conn.commit()
            
        return deleted_series
    
    def close(self):
        """Close all database connections"""
        if hasattr(self._local, 'connection'):
            try:
                self._local.connection.close()
                delattr(self._local, 'connection')
            except Exception:
                pass  # Connection may already be closed