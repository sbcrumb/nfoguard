#!/usr/bin/env python3
"""
NFOGuard - Automated NFO file management for Radarr and Sonarr
Modular architecture with webhook processing and intelligent date handling
"""
import os
import json
import asyncio
import glob
import re
import logging
import logging.handlers
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from pydantic import BaseModel
from typing import Optional, Dict, Any, List, Set, Tuple
import threading
from concurrent.futures import ThreadPoolExecutor
import uvicorn
import signal
import sys

# Import our new modular components
from core.database import NFOGuardDatabase
from core.nfo_manager import NFOManager
from core.path_mapper import PathMapper
from clients.radarr_client import RadarrClient
from clients.sonarr_client import SonarrClient
from clients.external_clients import ExternalClientManager

# ---------------------------
# Configuration & Logging
# ---------------------------

class TimezoneAwareFormatter(logging.Formatter):
    """Formatter that respects the container timezone"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timezone = self._get_local_timezone()
    
    def _get_local_timezone(self):
        """Get the local timezone, respecting TZ environment variable"""
        tz_name = os.environ.get('TZ', 'UTC')
        
        try:
            # Try zoneinfo first (Python 3.9+)
            return ZoneInfo(tz_name)
        except ImportError:
            # Fallback for older Python versions
            try:
                import pytz
                return pytz.timezone(tz_name)
            except:
                # Final fallback to UTC
                return timezone.utc
        except:
            # If zone name is invalid, fallback to UTC
            return timezone.utc
    
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=self.timezone)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat(timespec='seconds')

def _setup_file_logging():
    """Setup file logging for NFOGuard"""
    log_dir = Path(os.environ.get("LOG_DIR", "/app/data/logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger("NFOGuard")
    logger.setLevel(logging.DEBUG)
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / "nfoguard.log", maxBytes=50*1024*1024, backupCount=3
    )
    
    formatter = TimezoneAwareFormatter(
        '[%(asctime)s] %(levelname)s: %(message)s'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

def _mask_sensitive_data(msg: str) -> str:
    """Mask API keys and other sensitive data in log messages"""
    import re
    
    # List of patterns to mask
    sensitive_patterns = [
        (r'api_key=([a-zA-Z0-9_\-]+)', r'api_key=***masked***'),
        (r'password=([^\s&]+)', r'password=***masked***'),
        (r'token=([a-zA-Z0-9_\-]+)', r'token=***masked***'),
        (r'key=([a-zA-Z0-9_\-]{8,})', r'key=***masked***'),  # Keys longer than 8 chars
        (r'([a-zA-Z0-9]{32,})', lambda m: m.group(1)[:8] + '***masked***' if len(m.group(1)) > 16 else m.group(1))  # Long strings likely to be keys
    ]
    
    masked_msg = msg
    for pattern, replacement in sensitive_patterns:
        if isinstance(replacement, str):
            masked_msg = re.sub(pattern, replacement, masked_msg, flags=re.IGNORECASE)
        else:
            masked_msg = re.sub(pattern, replacement, masked_msg, flags=re.IGNORECASE)
    
    return masked_msg

def _get_local_timezone():
    """Get the local timezone, respecting TZ environment variable"""
    tz_name = os.environ.get('TZ', 'UTC')
    
    try:
        # Try zoneinfo first (Python 3.9+)
        return ZoneInfo(tz_name)
    except ImportError:
        # Fallback for older Python versions
        try:
            import pytz
            return pytz.timezone(tz_name)
        except:
            # Final fallback to UTC
            return timezone.utc
    except:
        # If zone name is invalid, fallback to UTC
        return timezone.utc

def _log(level: str, msg: str):
    """Enhanced logging that writes to both console and file with sensitive data masking"""
    masked_msg = _mask_sensitive_data(msg)
    tz = _get_local_timezone()
    print(f"[{datetime.now(tz).isoformat(timespec='seconds')}] {level}: {masked_msg}")
    
    try:
        file_logger = _setup_file_logging()
        getattr(file_logger, level.lower(), file_logger.info)(masked_msg)
    except Exception as e:
        print(f"File logging error: {e}")

def convert_utc_to_local(utc_iso_string: str) -> str:
    """Convert UTC ISO timestamp to local timezone timestamp"""
    if not utc_iso_string:
        return utc_iso_string
    
    try:
        # Parse UTC timestamp
        if utc_iso_string.endswith('Z'):
            dt_utc = datetime.fromisoformat(utc_iso_string.replace('Z', '+00:00'))
        elif '+00:00' in utc_iso_string:
            dt_utc = datetime.fromisoformat(utc_iso_string)
        else:
            # Assume UTC if no timezone info
            dt_utc = datetime.fromisoformat(utc_iso_string).replace(tzinfo=timezone.utc)
        
        # Convert to local timezone
        local_tz = _get_local_timezone()
        dt_local = dt_utc.astimezone(local_tz)
        
        return dt_local.isoformat(timespec='seconds')
    except Exception:
        # If conversion fails, return original
        return utc_iso_string

# Initialize logging
_setup_file_logging()

def _load_environment_files():
    """Load environment variables from .env and optionally .env.secrets"""
    from pathlib import Path
    
    # Try to load from python-dotenv if available
    try:
        from dotenv import load_dotenv
        
        # Load main .env file
        env_file = Path(".env")
        if env_file.exists():
            load_dotenv(env_file)
            _log("INFO", f"Loaded environment from {env_file}")
        
        # Load secrets file if it exists
        secrets_file = Path(".env.secrets")
        if secrets_file.exists():
            load_dotenv(secrets_file)
            _log("INFO", f"Loaded secrets from {secrets_file}")
            
    except ImportError:
        _log("WARNING", "python-dotenv not available - environment files not loaded")
        
# Load environment files at startup
_load_environment_files()

# Add debug logging near where configuration is loaded
print(f"DEBUG: Environment check - SONARR_ROOT_FOLDERS: {os.getenv('SONARR_ROOT_FOLDERS')}")
print(f"DEBUG: Environment check - TV_PATHS: {os.getenv('TV_PATHS')}")

def _bool_env(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "y", "on")

# ---------------------------
# Configuration
# ---------------------------

class NFOGuardConfig:
    def __init__(self):
        # Paths - No hardcoded defaults, must be configured via environment
        tv_paths_env = os.environ.get("TV_PATHS", "")
        movie_paths_env = os.environ.get("MOVIE_PATHS", "")
        
        if not tv_paths_env:
            raise ValueError("TV_PATHS environment variable is required but not set")
        if not movie_paths_env:
            raise ValueError("MOVIE_PATHS environment variable is required but not set")
            
        self.tv_paths = [Path(p.strip()) for p in tv_paths_env.split(",") if p.strip()]
        self.movie_paths = [Path(p.strip()) for p in movie_paths_env.split(",") if p.strip()]
        
        # Core settings
        self.manage_nfo = _bool_env("MANAGE_NFO", True)
        self.fix_dir_mtimes = _bool_env("FIX_DIR_MTIMES", True)
        self.lock_metadata = _bool_env("LOCK_METADATA", True)
        self.debug = _bool_env("DEBUG", False)
        self.manager_brand = os.environ.get("MANAGER_BRAND", "NFOGuard")
        
        # Batching
        self.batch_delay = float(os.environ.get("BATCH_DELAY", "5.0"))
        self.max_concurrent = int(os.environ.get("MAX_CONCURRENT_SERIES", "3"))
        
        # Database
        self.db_path = Path(os.environ.get("DB_PATH", "/app/data/media_dates.db"))
        
        # Movie processing
        self.movie_priority = os.environ.get("MOVIE_PRIORITY", "import_then_digital").lower()
        self.prefer_release_dates_over_file_dates = _bool_env("PREFER_RELEASE_DATES_OVER_FILE_DATES", True)
        self.allow_file_date_fallback = _bool_env("ALLOW_FILE_DATE_FALLBACK", False)
        self.release_date_priority = [p.strip() for p in os.environ.get("RELEASE_DATE_PRIORITY", "digital,physical,theatrical").split(",")]
        self.enable_smart_date_validation = _bool_env("ENABLE_SMART_DATE_VALIDATION", True)
        self.max_release_date_gap_years = int(os.environ.get("MAX_RELEASE_DATE_GAP_YEARS", "10"))
        self.movie_poll_mode = os.environ.get("MOVIE_POLL_MODE", "always").lower()
        self.movie_update_mode = os.environ.get("MOVIE_DATE_UPDATE_MODE", "backfill_only").lower()
        
        # TV processing
        self.tv_season_dir_format = os.environ.get("TV_SEASON_DIR_FORMAT", "Season {season:02d}")
        self.tv_season_dir_pattern = os.environ.get("TV_SEASON_DIR_PATTERN", "season ").lower()
        self.tv_webhook_processing_mode = os.environ.get("TV_WEBHOOK_PROCESSING_MODE", "targeted").lower()

config = NFOGuardConfig()

# ---------------------------
# Models
# ---------------------------

class SonarrWebhook(BaseModel):
    eventType: str
    series: Optional[Dict[str, Any]] = None
    episodes: Optional[list] = []
    episodeFile: Optional[Dict[str, Any]] = None
    isUpgrade: Optional[bool] = False

    class Config:
        extra = "allow"

class RadarrWebhook(BaseModel):
    eventType: str
    movie: Optional[Dict[str, Any]] = None
    movieFile: Optional[Dict[str, Any]] = None
    isUpgrade: Optional[bool] = False
    deletedFiles: Optional[list] = []
    remoteMovie: Optional[Dict[str, Any]] = None
    renamedMovieFiles: Optional[List[Dict[str, Any]]] = None

    class Config:
        extra = "allow"

class HealthResponse(BaseModel):
    status: str
    version: str
    uptime: str
    database_status: str
    radarr_database: Optional[Dict[str, Any]] = None

class TVSeasonRequest(BaseModel):
    series_path: str
    season_name: str

class TVEpisodeRequest(BaseModel):
    series_path: str
    season_name: str
    episode_name: str

# ---------------------------
# Core Processing
# ---------------------------

class TVProcessor:
    """Handles TV series processing"""
    
    def __init__(self, db: NFOGuardDatabase, nfo_manager: NFOManager, path_mapper: PathMapper):
        self.db = db
        self.nfo_manager = nfo_manager
        self.path_mapper = path_mapper
        self.sonarr = SonarrClient(
            os.environ.get("SONARR_URL", ""),
            os.environ.get("SONARR_API_KEY", "")
        )
        self.external_clients = ExternalClientManager()
    
    def find_series_path(self, series_title: str, imdb_id: str, sonarr_path: str = None) -> Optional[Path]:
        """Find series directory path"""
        # Try webhook path first
        if sonarr_path:
            container_path = self.path_mapper.sonarr_path_to_container_path(sonarr_path)
            path_obj = Path(container_path)
            if path_obj.exists():
                return path_obj
        
        # Search by IMDb ID or title
        for media_path in config.tv_paths:
            if not media_path.exists():
                continue
            
            # Search by IMDb ID
            if imdb_id:
                pattern = str(media_path / f"*[imdb-{imdb_id}]*")
                matches = glob.glob(pattern)
                if matches:
                    return Path(matches[0])
            
            # Search by title
            if series_title:
                title_clean = series_title.lower().replace(" ", "").replace("-", "")
                for item in media_path.iterdir():
                    if item.is_dir() and "[imdb-" in item.name.lower():
                        item_clean = item.name.lower().replace(" ", "").replace("-", "")
                        if title_clean in item_clean:
                            return item
        
        return None
    
    def process_series(self, series_path: Path) -> None:
        """Process a TV series directory"""
        imdb_id = self.nfo_manager.parse_imdb_from_path(series_path)
        if not imdb_id:
            _log("ERROR", f"No IMDb ID found in series path: {series_path}")
            return
        
        _log("INFO", f"Processing TV series: {series_path.name}")
        
        # Update database
        self.db.upsert_series(imdb_id, str(series_path))
        
        # Find video files
        disk_episodes = self._find_disk_episodes(series_path)
        _log("INFO", f"Found {len(disk_episodes)} episodes on disk")
        
        # Get episode dates
        episode_dates = self._gather_episode_dates(series_path, imdb_id, disk_episodes)
        
        # Process episodes
        for (season, episode), (aired, dateadded, source) in episode_dates.items():
            if (season, episode) in disk_episodes:
                # Create NFO
                if config.manage_nfo:
                    season_dir = series_path / config.tv_season_dir_format.format(season=season)
                    self.nfo_manager.create_episode_nfo(
                        season_dir,
                        season, episode, aired, dateadded, source, config.lock_metadata
                    )
                
                # Update file mtimes
                if config.fix_dir_mtimes and dateadded:
                    video_files = disk_episodes[(season, episode)]
                    for video_file in video_files:
                        self.nfo_manager.set_file_mtime(video_file, dateadded)
                
                # Save to database
                self.db.upsert_episode_date(imdb_id, season, episode, aired, dateadded, source, True)
        
        # Create season/tvshow NFOs
        if config.manage_nfo:
            seasons_processed = set()
            for (season, episode) in disk_episodes.keys():
                if season not in seasons_processed:
                    season_dir = series_path / config.tv_season_dir_format.format(season=season)
                    self.nfo_manager.create_season_nfo(season_dir, season)
                    seasons_processed.add(season)
            
            # Get TVDB ID for better Emby compatibility
            tvdb_id = self.external_clients.get_tvdb_series_id(imdb_id)
            self.nfo_manager.create_tvshow_nfo(series_path, imdb_id, tvdb_id)
        
        _log("INFO", f"Completed processing TV series: {series_path.name}")
    
    def _find_disk_episodes(self, series_path: Path) -> Dict[Tuple[int, int], List[Path]]:
        """Find all episode video files on disk"""
        disk_episodes = {}
        video_exts = (".mkv", ".mp4", ".avi", ".mov", ".m4v")
        
        for season_dir in series_path.iterdir():
            if not (season_dir.is_dir() and season_dir.name.lower().startswith(config.tv_season_dir_pattern)):
                continue
            
            try:
                # Extract season number from directory name
                # Handle formats like "Season 01", "S01", "Season01", etc.
                dir_name = season_dir.name.lower()
                if config.tv_season_dir_pattern in dir_name:
                    # Extract everything after the pattern
                    season_part = dir_name[len(config.tv_season_dir_pattern):].strip()
                else:
                    continue
                season_num = int(season_part)
            except (ValueError, IndexError):
                continue
            
            for video_file in season_dir.iterdir():
                if video_file.is_file() and video_file.suffix.lower() in video_exts:
                    match = re.search(r"S(\d{2})E(\d{2})", video_file.name, re.IGNORECASE)
                    if match:
                        file_season, file_episode = int(match.group(1)), int(match.group(2))
                        key = (season_num, file_episode)  # Use directory season number
                        if key not in disk_episodes:
                            disk_episodes[key] = []
                        disk_episodes[key].append(video_file)
        
        return disk_episodes
    
    def _gather_episode_dates(self, series_path: Path, imdb_id: str, disk_episodes: Dict) -> Dict:
        """Gather episode dates from various sources"""
        episode_dates = {}
        
        # Check cache first
        cached_episodes = self.db.get_series_episodes(imdb_id, has_video_file_only=True)
        for ep in cached_episodes:
            key = (ep["season"], ep["episode"])
            if key in disk_episodes:  # Only use cached data for episodes we have
                episode_dates[key] = (ep["aired"], ep["dateadded"], ep["source"])
        
        # Check for existing NFO files (including long-named ones) for migration
        nfo_episodes_found = 0
        for (season_num, episode_num) in disk_episodes.keys():
            if (season_num, episode_num) not in episode_dates:
                # Check if this episode has an existing NFO file that needs migration
                season_dir = series_path / config.tv_season_dir_format.format(season=season_num)
                existing_nfo = self.nfo_manager.find_existing_episode_nfo(season_dir, season_num, episode_num)
                
                if existing_nfo:
                    # Force processing of this episode for NFO migration
                    episode_dates[(season_num, episode_num)] = (None, None, "nfo_migration_required")
                    nfo_episodes_found += 1
                    
        if nfo_episodes_found > 0:
            _log("INFO", f"Found {nfo_episodes_found} episodes with existing NFO files requiring migration")
        
        # Find missing episodes (not in cache and no existing NFO)
        cached_keys = set(episode_dates.keys())
        missing_keys = set(disk_episodes.keys()) - cached_keys
        
        if not missing_keys:
            if nfo_episodes_found == 0:
                _log("INFO", "All episodes found in cache")
            return episode_dates
        
        _log("INFO", f"Querying APIs for {len(missing_keys)} missing episodes")
        
        # Query Sonarr for missing episodes
        if self.sonarr.enabled:
            series = self.sonarr.series_by_imdb(imdb_id)
            if series:
                series_id = series.get("id")
                if series_id:
                    episodes = self.sonarr.episodes_for_series(series_id)
                    for ep in episodes:
                        season_num = ep.get("seasonNumber")
                        episode_num = ep.get("episodeNumber")
                        
                        if not isinstance(season_num, int) or not isinstance(episode_num, int):
                            continue
                        
                        key = (season_num, episode_num)
                        if key not in missing_keys:
                            continue
                        
                        # Get dates
                        aired = self._parse_date_to_iso(ep.get("airDateUtc"))
                        dateadded = None
                        source = "sonarr:episode.airDateUtc"
                        
                        # Try to get import history
                        episode_id = ep.get("id")
                        if episode_id:
                            import_date = self.sonarr.get_episode_import_history(episode_id)
                            if import_date:
                                dateadded = self._parse_date_to_iso(import_date)
                                source = "sonarr:history.import"
                        
                        if not dateadded:
                            dateadded = aired
                        
                        if aired or dateadded:
                            episode_dates[key] = (aired, dateadded, source)
        
        # Fill remaining gaps with external APIs
        remaining_keys = missing_keys - set(episode_dates.keys())
        if remaining_keys and self.external_clients.tmdb.enabled:
            tmdb_movie = self.external_clients.tmdb.find_by_imdb(imdb_id)
            if tmdb_movie:
                tv_id = tmdb_movie.get("id")
                if tv_id:
                    seasons_needed = set(season for season, episode in remaining_keys)
                    for season_num in seasons_needed:
                        tmdb_episodes = self.external_clients.tmdb.get_tv_season_episodes(tv_id, season_num)
                        for ep_num, air_date in tmdb_episodes.items():
                            key = (season_num, ep_num)
                            if key in remaining_keys:
                                aired = self._parse_date_to_iso(air_date)
                                episode_dates[key] = (aired, aired, "tmdb:air_date")
        
        return episode_dates
    
    def _parse_date_to_iso(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO format"""
        if not date_str:
            return None
        try:
            if len(date_str) == 10 and date_str[4] == "-":
                dt = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
            else:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(timezone.utc)
            return dt.isoformat(timespec="seconds")
        except Exception:
            return None

    def process_season(self, series_path: Path, season_path: Path) -> None:
        """Process a specific TV season directory"""
        imdb_id = self.nfo_manager.parse_imdb_from_path(series_path)
        if not imdb_id:
            _log("ERROR", f"No IMDb ID found in series path: {series_path}")
            return
        
        # Extract season number from path
        season_match = re.search(r'season\s*(\d+)', season_path.name, re.IGNORECASE)
        if not season_match:
            _log("ERROR", f"Could not extract season number from: {season_path.name}")
            return
        
        season_num = int(season_match.group(1))
        _log("INFO", f"Processing TV season {season_num}: {season_path}")
        
        # Update database
        self.db.upsert_series(imdb_id, str(series_path))
        
        # Find video files in this season only
        disk_episodes = self._find_season_episodes(season_path, season_num)
        _log("INFO", f"Found {len(disk_episodes)} episodes in season {season_num}")
        
        # Get enhanced metadata from Sonarr
        series_metadata = self._get_sonarr_series_metadata(imdb_id)
        
        # Get episode dates for this season
        episode_dates = self._gather_season_episode_dates(series_path, imdb_id, season_num, disk_episodes, series_metadata)
        
        # Process episodes
        for (season, episode), (aired, dateadded, source) in episode_dates.items():
            if (season, episode) in disk_episodes:
                # Get enhanced episode metadata
                enhanced_metadata = self._get_episode_metadata(series_metadata, season, episode, season_path) if series_metadata else self._get_episode_metadata(None, season, episode, season_path)
                
                # Create NFO
                if config.manage_nfo:
                    self.nfo_manager.create_episode_nfo(
                        season_path,
                        season, episode, aired, dateadded, source, config.lock_metadata,
                        enhanced_metadata
                    )
                
                # Update file mtimes
                if config.fix_dir_mtimes and dateadded:
                    video_files = disk_episodes[(season, episode)]
                    for video_file in video_files:
                        self.nfo_manager.set_file_mtime(video_file, dateadded)
                
                # Save to database
                self.db.upsert_episode_date(imdb_id, season, episode, aired, dateadded, source, True)
        
        # Create season NFO
        if config.manage_nfo:
            self.nfo_manager.create_season_nfo(season_path, season_num)
            # Get TVDB ID for better Emby compatibility
            tvdb_id = self.external_clients.get_tvdb_series_id(imdb_id)
            self.nfo_manager.create_tvshow_nfo(series_path, imdb_id, tvdb_id)
        
        _log("INFO", f"Completed processing season {season_num}")

    def process_episode_file(self, series_path: Path, season_path: Path, episode_file: Path) -> None:
        """Process a single TV episode file"""
        imdb_id = self.nfo_manager.parse_imdb_from_path(series_path)
        if not imdb_id:
            _log("ERROR", f"No IMDb ID found in series path: {series_path}")
            return
        
        # Parse episode info from filename
        episode_info = self._parse_episode_from_filename(episode_file.name)
        if not episode_info:
            _log("ERROR", f"Could not parse episode info from: {episode_file.name}")
            return
        
        season_num, episode_num = episode_info
        _log("INFO", f"Processing single episode S{season_num:02d}E{episode_num:02d}: {episode_file.name}")
        
        # Update database
        self.db.upsert_series(imdb_id, str(series_path))
        
        # TIER 1: Check if episode NFO already has NFOGuard data (fastest - no DB or API calls)
        nfo_data = self.nfo_manager.extract_nfoguard_dates_from_episode_nfo(season_path, season_num, episode_num)
        if nfo_data:
            _log("INFO", f"🚀 Using existing NFOGuard data from episode NFO S{season_num:02d}E{episode_num:02d}: {nfo_data['dateadded']} (source: {nfo_data['source']})")
            dateadded = nfo_data["dateadded"]
            source = nfo_data["source"] 
            aired = nfo_data.get("aired")
            
            # Check if NFO is missing title and try to enhance it
            if not nfo_data.get("has_title", False):
                _log("INFO", f"NFO S{season_num:02d}E{episode_num:02d} missing title, attempting to extract from filename")
                filename_title = self._extract_title_from_filename(season_path, season_num, episode_num)
                if filename_title:
                    enhanced = self.nfo_manager.enhance_existing_episode_nfo_with_title(
                        season_path, season_num, episode_num, filename_title
                    )
                    if enhanced:
                        _log("INFO", f"✅ Enhanced NFO S{season_num:02d}E{episode_num:02d} with title: '{filename_title}'")
                    else:
                        _log("WARNING", f"Failed to enhance NFO S{season_num:02d}E{episode_num:02d} with title")
                else:
                    _log("WARNING", f"Could not extract title from filename for S{season_num:02d}E{episode_num:02d}")
            
            # Update file mtime if enabled (NFO is already correct)
            if config.fix_dir_mtimes and dateadded:
                self.nfo_manager.set_file_mtime(episode_file, dateadded)
            
            _log("INFO", f"Completed processing episode S{season_num:02d}E{episode_num:02d} (source: {source}) [episode-nfo-only]")
            return
            
        # TIER 2: Check database for existing episode data
        existing_episode = self.db.get_episode_date(imdb_id, season_num, episode_num)
        if existing_episode and existing_episode.get("dateadded") and existing_episode.get("source") != "no_valid_date_source":
            _log("INFO", f"✅ Using complete database data for episode S{season_num:02d}E{episode_num:02d}: {existing_episode['dateadded']} (source: {existing_episode['source']})")
            # Still create NFO and update files but skip API queries
            dateadded = existing_episode["dateadded"]
            source = existing_episode["source"]
            aired = existing_episode.get("aired")
            
            # Create NFO with existing data, but try to add title from filename if missing
            if config.manage_nfo and dateadded:
                # Try to extract title from filename as fallback for Tier 2 processing
                enhanced_metadata = self._get_episode_metadata(None, season_num, episode_num, season_path)
                self.nfo_manager.create_episode_nfo(
                    season_path,
                    season_num, episode_num, aired, dateadded, source, config.lock_metadata,
                    enhanced_metadata  # Include filename-extracted title if available
                )
            
            # Update file mtime if enabled
            if config.fix_dir_mtimes and dateadded:
                self.nfo_manager.set_file_mtime(episode_file, dateadded)
            
            _log("INFO", f"Completed processing episode S{season_num:02d}E{episode_num:02d} (source: {source}) [episode-database-only]")
            return
        
        # TIER 3: Full processing with Sonarr API calls (slowest)
        _log("DEBUG", f"Episode S{season_num:02d}E{episode_num:02d} requires full processing - querying Sonarr APIs")
        
        # Get enhanced metadata from Sonarr
        series_metadata = self._get_sonarr_series_metadata(imdb_id)
        enhanced_metadata = self._get_episode_metadata(series_metadata, season_num, episode_num, season_path) if series_metadata else self._get_episode_metadata(None, season_num, episode_num, season_path)
        
        # Get episode date
        aired, dateadded, source = self._get_single_episode_date(imdb_id, season_num, episode_num, series_metadata)
        
        # Create NFO
        if config.manage_nfo and dateadded:
            self.nfo_manager.create_episode_nfo(
                season_path,
                season_num, episode_num, aired, dateadded, source, config.lock_metadata,
                enhanced_metadata
            )
        
        # Update file mtime
        if config.fix_dir_mtimes and dateadded:
            self.nfo_manager.set_file_mtime(episode_file, dateadded)
        
        # Save to database
        self.db.upsert_episode_date(imdb_id, season_num, episode_num, aired, dateadded, source, True)
        
        _log("INFO", f"Completed processing episode S{season_num:02d}E{episode_num:02d}")

    def _find_season_episodes(self, season_path: Path, season_num: int) -> Dict[Tuple[int, int], List[Path]]:
        """Find all episode video files in a single season"""
        disk_episodes = {}
        video_exts = (".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts", ".m2ts")
        
        for file_path in season_path.iterdir():
            if not file_path.is_file() or file_path.suffix.lower() not in video_exts:
                continue
            
            episode_info = self._parse_episode_from_filename(file_path.name)
            if episode_info and episode_info[0] == season_num:
                season, episode = episode_info
                key = (season, episode)
                if key not in disk_episodes:
                    disk_episodes[key] = []
                disk_episodes[key].append(file_path)
        
        return disk_episodes

    def _parse_episode_from_filename(self, filename: str) -> Optional[Tuple[int, int]]:
        """Parse season and episode numbers from filename"""
        # Try SxxExx format
        match = re.search(r'S(\d{1,2})E(\d{1,2})', filename, re.IGNORECASE)
        if match:
            return int(match.group(1)), int(match.group(2))
        
        # Try season.episode format
        match = re.search(r'(\d{1,2})\.(\d{1,2})', filename)
        if match:
            return int(match.group(1)), int(match.group(2))
        
        return None


    def _get_sonarr_series_metadata(self, imdb_id: str) -> Optional[Dict[str, Any]]:
        """Get enhanced series metadata from Sonarr API"""
        try:
            if not self.sonarr.enabled:
                return None
            
            series = self.sonarr.series_by_imdb(imdb_id)
            if not series:
                _log("DEBUG", f"No Sonarr series found for IMDb {imdb_id}")
                return None
            
            # Get episodes for the series
            series_id = series.get("id")
            if series_id:
                episodes = self.sonarr.episodes_for_series(series_id)
                series["episodes"] = episodes
                _log("DEBUG", f"Got {len(episodes)} episodes from Sonarr for series {series.get('title')}")
            
            return series
            
        except Exception as e:
            _log("ERROR", f"Error getting Sonarr metadata for {imdb_id}: {e}")
            return None

    def _get_episode_metadata(self, series_metadata: Dict[str, Any], season_num: int, episode_num: int, season_dir: Optional[Path] = None) -> Optional[Dict[str, Any]]:
        """Extract specific episode metadata from series data with filename fallback for titles"""
        _log("INFO", f"🔍 _get_episode_metadata called for S{season_num:02d}E{episode_num:02d}, season_dir: {season_dir}")
        metadata = {
            "title": None,
            "overview": None,
            "runtime": None,
            "ratings": {}
        }
        
        # Try to get metadata from Sonarr first
        if series_metadata and "episodes" in series_metadata:
            for episode in series_metadata["episodes"]:
                if episode.get("seasonNumber") == season_num and episode.get("episodeNumber") == episode_num:
                    metadata.update({
                        "title": episode.get("title"),
                        "overview": episode.get("overview"),
                        "runtime": episode.get("runtime"),
                        "ratings": episode.get("ratings", {})
                    })
                    break
        
        # If no title from Sonarr, try to extract from filename
        if not metadata["title"] and season_dir:
            _log("INFO", f"📁 No title from Sonarr for S{season_num:02d}E{episode_num:02d}, trying filename extraction")
            filename_title = self._extract_title_from_filename(season_dir, season_num, episode_num)
            if filename_title:
                metadata["title"] = filename_title
                _log("INFO", f"✅ Using filename-extracted title for S{season_num:02d}E{episode_num:02d}: '{filename_title}'")
            else:
                _log("DEBUG", f"⚠️ No filename title extracted for S{season_num:02d}E{episode_num:02d}")
        elif metadata["title"]:
            _log("DEBUG", f"✅ Using Sonarr title for S{season_num:02d}E{episode_num:02d}: '{metadata['title']}'")
        
        # Return metadata if we have at least some information
        if any(metadata.values()):
            return metadata
            
        return None

    def _extract_title_from_filename(self, season_dir: Path, season_num: int, episode_num: int) -> Optional[str]:
        """Extract episode title from video filename as fallback when Sonarr doesn't provide it"""
        try:
            import re
            # Look for video files matching this episode
            season_pattern = f"S{season_num:02d}E{episode_num:02d}"
            _log("INFO", f"🔍 Searching for title in files for {season_pattern} in directory: {season_dir}")
            
            for video_file in season_dir.glob("*.mkv"):
                filename = video_file.name
                _log("DEBUG", f"🔍 Checking file: {filename}")
                if season_pattern in filename:
                    _log("DEBUG", f"✅ Found matching file: {filename}")
                    # Pattern: SeriesName-S01E01-Episode Title[WEBDL-1080p][AAC2.0][h264].mkv
                    # Extract the part between season/episode and the first bracket
                    pattern = rf'{season_pattern}-(.*?)\['
                    _log("DEBUG", f"🔍 Using regex pattern: {pattern}")
                    match = re.search(pattern, filename)
                    if match:
                        title = match.group(1).strip()
                        _log("DEBUG", f"🔍 Raw extracted title: '{title}'")
                        # Clean up common encoding artifacts and separators
                        title = title.replace('-', ' ').strip()
                        if title:
                            _log("INFO", f"✅ Extracted title from filename: '{title}' for {season_pattern}")
                            return title
                        else:
                            _log("DEBUG", f"⚠️ Title was empty after cleanup")
                    else:
                        _log("DEBUG", f"⚠️ Regex pattern didn't match filename")
            
            # Also check .mp4 files
            for video_file in season_dir.glob("*.mp4"):
                filename = video_file.name
                _log("DEBUG", f"🔍 Checking .mp4 file: {filename}")
                if season_pattern in filename:
                    _log("DEBUG", f"✅ Found matching .mp4 file: {filename}")
                    pattern = rf'{season_pattern}-(.*?)\['
                    match = re.search(pattern, filename)
                    if match:
                        title = match.group(1).strip()
                        _log("DEBUG", f"🔍 Raw extracted title from .mp4: '{title}'")
                        title = title.replace('-', ' ').strip()
                        if title:
                            _log("INFO", f"✅ Extracted title from .mp4 filename: '{title}' for {season_pattern}")
                            return title
                            
        except Exception as e:
            _log("ERROR", f"Error extracting title from filename for S{season_num:02d}E{episode_num:02d}: {e}")
        
        _log("DEBUG", f"⚠️ No title found in filenames for {season_pattern}")
        return None

    def _gather_season_episode_dates(self, series_path: Path, imdb_id: str, season_num: int, disk_episodes: Dict[Tuple[int, int], List[Path]], series_metadata: Optional[Dict[str, Any]] = None) -> Dict[Tuple[int, int], Tuple[Optional[str], Optional[str], str]]:
        """Get episode dates for a specific season"""
        episode_dates = {}
        
        for (season, episode) in disk_episodes.keys():
            if season != season_num:
                continue
            
            aired, dateadded, source = self._get_single_episode_date(imdb_id, season, episode, series_metadata)
            episode_dates[(season, episode)] = (aired, dateadded, source)
        
        return episode_dates

    def _get_single_episode_date(self, imdb_id: str, season_num: int, episode_num: int, series_metadata: Optional[Dict[str, Any]] = None) -> Tuple[Optional[str], Optional[str], str]:
        """
        Get date info for a single episode during backfill scans.
        Priority: 1) Database 2) Sonarr import history 3) Air dates
        """
        # Step 1: Try database first
        existing = self.db.get_episode_date(imdb_id, season_num, episode_num)
        if existing and existing.get("dateadded"):
            _log("DEBUG", f"Using existing database entry for S{season_num:02d}E{episode_num:02d}")
            return existing.get("aired"), existing["dateadded"], existing.get("source", "database:existing")
        
        # Step 2: Try Sonarr import history (the real import date)
        aired = None
        import_date = None
        
        if self.sonarr.enabled:
            try:
                series = self.sonarr.series_by_imdb(imdb_id)
                if series:
                    episodes = self.sonarr.episodes_for_series(series["id"])
                    for ep in episodes:
                        if ep.get("seasonNumber") == season_num and ep.get("episodeNumber") == episode_num:
                            aired = ep.get("airDateUtc")
                            import_date = self.sonarr.get_episode_import_history(ep["id"])
                            if import_date:
                                # Convert import date to local timezone for NFO files
                                local_import_date = convert_utc_to_local(import_date)
                                _log("INFO", f"Found Sonarr import history for S{season_num:02d}E{episode_num:02d}: {local_import_date}")
                                return aired, local_import_date, "sonarr:history.import"
                            break
            except Exception as e:
                _log("DEBUG", f"Sonarr API error for episode S{season_num:02d}E{episode_num:02d}: {e}")
        
        # Step 3: Try Sonarr metadata for air date
        if not aired and series_metadata and "episodes" in series_metadata:
            for episode in series_metadata["episodes"]:
                if episode.get("seasonNumber") == season_num and episode.get("episodeNumber") == episode_num:
                    aired = episode.get("airDateUtc")
                    break
        
        # Step 4: Only if we have an air date but no import history, use air date as fallback
        if aired:
            _log("WARNING", f"No import history found for S{season_num:02d}E{episode_num:02d}, using air date as fallback")
            return aired, aired, "sonarr:episode.airDateUtc"
        
        # Step 5: Last resort - current time in local timezone (shouldn't happen in normal operation)
        local_tz = _get_local_timezone()
        current_time = datetime.now(local_tz).isoformat(timespec="seconds")
        _log("WARNING", f"No data found for S{season_num:02d}E{episode_num:02d}, using current time")
        return None, current_time, "fallback:current_time"

    def process_webhook_episodes(self, series_path: Path, webhook_episodes: List[Dict[str, Any]]) -> None:
        """Process only the specific episodes mentioned in a webhook (targeted mode)"""
        imdb_id = self.nfo_manager.parse_imdb_from_path(series_path)
        if not imdb_id:
            _log("ERROR", f"No IMDb ID found in series path: {series_path}")
            return
        
        if not webhook_episodes:
            _log("WARNING", f"No episodes in webhook, falling back to series processing: {series_path}")
            self.process_series(series_path)
            return
        
        _log("INFO", f"Processing {len(webhook_episodes)} webhook episodes for: {series_path.name} (IMDb: {imdb_id})")
        
        # Update database
        self.db.upsert_series(imdb_id, str(series_path))
        
        # Get enhanced metadata from Sonarr
        series_metadata = self._get_sonarr_series_metadata(imdb_id)
        
        episodes_processed = 0
        for webhook_episode in webhook_episodes:
            season_num = webhook_episode.get("seasonNumber")
            episode_num = webhook_episode.get("episodeNumber")
            
            if not season_num or not episode_num:
                _log("WARNING", f"Invalid episode data in webhook: {webhook_episode}")
                continue
            
            # Check if episode file exists on disk
            season_dir = series_path / config.tv_season_dir_format.format(season=season_num)
            if not season_dir.exists():
                _log("WARNING", f"Season directory not found: {season_dir}")
                continue
                
            # Find matching episode files
            episode_files = []
            for file_path in season_dir.iterdir():
                if file_path.is_file() and file_path.suffix.lower() in ('.mkv', '.mp4', '.avi', '.mov', '.m4v'):
                    parsed = self._parse_episode_from_filename(file_path.name)
                    if parsed and parsed == (season_num, episode_num):
                        episode_files.append(file_path)
            
            if not episode_files:
                _log("WARNING", f"No video files found for S{season_num:02d}E{episode_num:02d}")
                continue
                
            # Get episode date information - webhook processing prioritizes existing DB entries
            _log("DEBUG", f"Processing webhook episode: IMDb={imdb_id}, S{season_num:02d}E{episode_num:02d}")
            aired, dateadded, source = self._get_webhook_episode_date(imdb_id, season_num, episode_num, series_metadata)
            enhanced_metadata = self._get_episode_metadata(series_metadata, season_num, episode_num, season_dir) if series_metadata else self._get_episode_metadata(None, season_num, episode_num, season_dir)
            
            # Create NFO
            if config.manage_nfo:
                self.nfo_manager.create_episode_nfo(
                    season_dir,
                    season_num, episode_num, aired, dateadded, source, config.lock_metadata,
                    enhanced_metadata
                )
            
            # Update file mtimes
            if config.fix_dir_mtimes and dateadded:
                for episode_file in episode_files:
                    self.nfo_manager.set_file_mtime(episode_file, dateadded)
            
            # Save to database
            self.db.upsert_episode_date(imdb_id, season_num, episode_num, aired, dateadded, source, True)
            
            # Verify database entry was saved (debug)
            verification = self.db.get_episode_date(imdb_id, season_num, episode_num)
            if verification:
                _log("DEBUG", f"Verified database entry saved: S{season_num:02d}E{episode_num:02d} -> {verification['dateadded']}")
            else:
                _log("ERROR", f"Failed to save episode to database: S{season_num:02d}E{episode_num:02d}")
            
            episodes_processed += 1
        
        # Create season/tvshow NFOs if any episodes were processed
        if episodes_processed > 0 and config.manage_nfo:
            seasons_processed = set()
            for webhook_episode in webhook_episodes:
                season_num = webhook_episode.get("seasonNumber")
                if season_num and season_num not in seasons_processed:
                    season_dir = series_path / config.tv_season_dir_format.format(season=season_num)
                    if season_dir.exists():
                        self.nfo_manager.create_season_nfo(season_dir, season_num)
                        seasons_processed.add(season_num)
            
            # Get TVDB ID for better Emby compatibility
            tvdb_id = self.external_clients.get_tvdb_series_id(imdb_id)
            self.nfo_manager.create_tvshow_nfo(series_path, imdb_id, tvdb_id)
        
        _log("INFO", f"Completed targeted processing: {episodes_processed}/{len(webhook_episodes)} episodes processed")

    def _get_webhook_episode_date(self, imdb_id: str, season_num: int, episode_num: int, series_metadata: Optional[Dict[str, Any]] = None) -> Tuple[Optional[str], Optional[str], str]:
        """
        Get episode date for webhook processing with correct priority:
        1. Check existing NFOGuard database entry (preserve previous import dates)
        2. If not found, use current time (webhook = new download)
        3. Get aired date from Sonarr/TMDB for reference
        """
        # Step 1: Check if we already have this episode in our database
        _log("DEBUG", f"Checking database for existing episode: IMDb={imdb_id}, S{season_num:02d}E{episode_num:02d}")
        existing = self.db.get_episode_date(imdb_id, season_num, episode_num)
        if existing:
            _log("DEBUG", f"Found database entry: {existing}")
            if existing.get("dateadded"):
                _log("INFO", f"Using existing database entry for S{season_num:02d}E{episode_num:02d}: {existing['dateadded']} (source: {existing.get('source', 'unknown')})")
                return existing.get("aired"), existing["dateadded"], existing.get("source", "database:existing")
            else:
                _log("DEBUG", f"Database entry found but no dateadded value: {existing}")
        else:
            _log("DEBUG", f"No database entry found for IMDb={imdb_id}, S{season_num:02d}E{episode_num:02d}")
        
        # Step 2: Webhook = Source of Truth - use current timestamp in local timezone
        local_tz = _get_local_timezone()
        current_time = datetime.now(local_tz).isoformat(timespec="seconds")
        _log("INFO", f"Webhook episode processing - using current timestamp as source of truth: {current_time}")
        
        # Step 3: Get aired date for reference (but don't use for dateadded)
        aired = None
        
        # Try Sonarr metadata first for air date
        if series_metadata and "episodes" in series_metadata:
            for episode in series_metadata["episodes"]:
                if episode.get("seasonNumber") == season_num and episode.get("episodeNumber") == episode_num:
                    aired = episode.get("airDateUtc")
                    if aired:
                        break
        
        # Fallback to Sonarr API for air date if not in metadata
        if not aired and self.sonarr.enabled:
            try:
                series = self.sonarr.series_by_imdb(imdb_id)
                if series:
                    episodes = self.sonarr.episodes_for_series(series["id"])
                    for ep in episodes:
                        if ep.get("seasonNumber") == season_num and ep.get("episodeNumber") == episode_num:
                            aired = ep.get("airDateUtc")
                            break
            except Exception as e:
                _log("DEBUG", f"Error getting air date from Sonarr: {e}")
        
        return aired, current_time, "webhook:first_seen"


class MovieProcessor:
    """Handles movie processing"""
    
    def __init__(self, db: NFOGuardDatabase, nfo_manager: NFOManager, path_mapper: PathMapper):
        self.db = db
        self.nfo_manager = nfo_manager
        self.path_mapper = path_mapper
        self.radarr = RadarrClient(
            os.environ.get("RADARR_URL", ""),
            os.environ.get("RADARR_API_KEY", "")
        )
        self.external_clients = ExternalClientManager()
    
    def find_movie_path(self, movie_title: str, imdb_id: str, radarr_path: str = None) -> Optional[Path]:
        """Find movie directory path"""
        # Try webhook path first
        if radarr_path:
            container_path = self.path_mapper.radarr_path_to_container_path(radarr_path)
            path_obj = Path(container_path)
            if path_obj.exists():
                return path_obj
        
        # Search by IMDb ID or title
        for media_path in config.movie_paths:
            if not media_path.exists():
                continue
            
            # Search by IMDb ID
            if imdb_id:
                pattern = str(media_path / f"*[imdb-{imdb_id}]*")
                matches = glob.glob(pattern)
                if matches:
                    return Path(matches[0])
            
            # Search by title
            if movie_title:
                title_clean = movie_title.lower().replace(" ", "").replace("-", "")
                for item in media_path.iterdir():
                    if item.is_dir() and "[imdb-" in item.name.lower():
                        item_clean = item.name.lower().replace(" ", "").replace("-", "")
                        if title_clean in item_clean:
                            return item
        
        return None
    
    def process_movie(self, movie_path: Path, webhook_mode: bool = False) -> None:
        """Process a movie directory"""
        imdb_id = self.nfo_manager.find_movie_imdb_id(movie_path)
        if not imdb_id:
            _log("ERROR", f"No IMDb ID found in movie directory, filenames, or NFO file: {movie_path}")
            return
        
        # Handle TMDB ID fallback case
        is_tmdb_fallback = imdb_id.startswith("tmdb-")
        if is_tmdb_fallback:
            _log("INFO", f"Processing movie: {movie_path.name} (TMDB: {imdb_id})")
        else:
            _log("INFO", f"Processing movie: {movie_path.name} (IMDb: {imdb_id})")
        
        # Update database
        self.db.upsert_movie(imdb_id, str(movie_path))
        
        # Check for video files
        video_exts = (".mkv", ".mp4", ".avi", ".mov", ".m4v")
        has_video = any(f.is_file() and f.suffix.lower() in video_exts for f in movie_path.iterdir())
        
        if not has_video:
            _log("WARNING", f"No video files found in: {movie_path}")
            self.db.upsert_movie_dates(imdb_id, None, None, None, False)
            return
        
        # TIER 1: Check if NFO file already has NFOGuard data (fastest - no DB or API calls)
        nfo_path = movie_path / "movie.nfo"
        nfo_data = self.nfo_manager.extract_nfoguard_dates_from_nfo(nfo_path)
        if nfo_data:
            _log("INFO", f"🚀 Using existing NFOGuard data from NFO file: {nfo_data['dateadded']} (source: {nfo_data['source']})")
            dateadded = nfo_data["dateadded"]
            source = nfo_data["source"] 
            released = nfo_data.get("released")
            
            # Update file mtimes if enabled (NFO is already correct)
            if config.fix_dir_mtimes and dateadded:
                self.nfo_manager.update_movie_files_mtime(movie_path, dateadded)
            
            _log("INFO", f"Completed processing movie: {movie_path.name} (source: {source}) [nfo-only]")
            return
            
        # TIER 1.5: Special handling for TMDB-only movies - extract dates from existing NFO
        if is_tmdb_fallback:
            tmdb_nfo_data = self._extract_dates_from_tmdb_nfo(nfo_path)
            if tmdb_nfo_data:
                _log("INFO", f"🎬 Using TMDB data from existing NFO file: {tmdb_nfo_data['dateadded']} (source: {tmdb_nfo_data['source']})")
                dateadded = tmdb_nfo_data["dateadded"]
                source = tmdb_nfo_data["source"]
                released = tmdb_nfo_data.get("released")
                
                # Create NFO with NFOGuard fields added
                if config.manage_nfo:
                    self.nfo_manager.create_movie_nfo(
                        movie_path, imdb_id, dateadded, released, source, config.lock_metadata
                    )
                
                # Update file mtimes if enabled
                if config.fix_dir_mtimes and dateadded:
                    self.nfo_manager.update_movie_files_mtime(movie_path, dateadded)
                
                # Save to database
                self.db.upsert_movie_dates(imdb_id, released, dateadded, source, True)
                
                _log("INFO", f"Completed processing movie: {movie_path.name} (source: {source}) [tmdb-nfo]")
                return
            
        # TIER 2: Check database for existing data
        existing = self.db.get_movie_dates(imdb_id)
        _log("DEBUG", f"Database lookup for {imdb_id}: {existing}")
        
        # If we have complete data in database, use it and skip expensive API calls
        if existing and existing.get("dateadded") and existing.get("source") != "no_valid_date_source":
            _log("INFO", f"✅ Using complete database data for {imdb_id}: {existing['dateadded']} (source: {existing['source']})")
            # Still create NFO and update files but skip API queries
            dateadded, source, released = existing["dateadded"], existing["source"], existing.get("released")
            
            # Create NFO with existing data
            if config.manage_nfo:
                self.nfo_manager.create_movie_nfo(
                    movie_path, imdb_id, dateadded, released, source, config.lock_metadata
                )
            
            # Update file mtimes if enabled
            if config.fix_dir_mtimes and dateadded:
                self.nfo_manager.update_movie_files_mtime(movie_path, dateadded)
            
            _log("INFO", f"Completed processing movie: {movie_path.name} (source: {source}) [database-only]")
            return
        
        # Handle webhook mode - prioritize database, then use proper date logic
        if webhook_mode:
            _log("DEBUG", f"Webhook mode: existing={bool(existing)}, has_dateadded={bool(existing and existing.get('dateadded')) if existing else 'N/A'}")
            if existing and existing.get("dateadded"):
                _log("INFO", f"Webhook processing - using existing database entry: {existing['dateadded']} (source: {existing.get('source', 'unknown')})")
                dateadded, source, released = existing["dateadded"], existing["source"], existing.get("released")
            else:
                if existing:
                    _log("INFO", f"Webhook processing - database entry exists but no dateadded field: {existing}")
                else:
                    _log("INFO", f"Webhook processing - no database entry found for {imdb_id}")
                _log("INFO", f"Using full date decision logic")
                # Use same logic as manual scan to check Radarr import dates, release dates, etc.
                should_query = True  # Always query for webhooks when no database entry exists
                dateadded, source, released = self._decide_movie_dates(imdb_id, movie_path, should_query, existing)
                
                # Only if ALL date sources fail, fall back to current timestamp
                if dateadded is None:
                    local_tz = _get_local_timezone()
                    current_time = datetime.now(local_tz).isoformat(timespec="seconds")
                    _log("INFO", f"Webhook processing - all date sources failed, using current timestamp as last resort: {current_time}")
                    dateadded, source = current_time, "webhook:fallback_timestamp"
        else:
            # Manual scan mode - determine if we should query APIs
            should_query = (
                config.movie_poll_mode == "always" or
                (config.movie_poll_mode == "if_missing" and not existing) or
                (config.movie_poll_mode == "if_missing" and existing and existing.get("source") == "file:mtime") or
                (config.movie_poll_mode == "if_missing" and existing and not existing.get("dateadded"))
            )
            
            _log("DEBUG", f"Movie {imdb_id}: should_query={should_query}, poll_mode={config.movie_poll_mode}, existing={bool(existing)}, has_dateadded={bool(existing and existing.get('dateadded')) if existing else False}")
            
            # Use existing movie date decision logic
            dateadded, source, released = self._decide_movie_dates(imdb_id, movie_path, should_query, existing)
        
        # If we don't have an import/download date but we have a release date, use it as dateadded
        # This ensures we save digital release dates, theatrical dates, etc. to the database
        final_dateadded = dateadded
        final_source = source
        
        if dateadded is None and released is not None:
            final_dateadded = released
            final_source = f"{source}_as_dateadded" if source else "release_date_fallback"
            _log("INFO", f"Using release date as dateadded: {final_dateadded} (source: {final_source})")
        
        # Create NFO regardless of date availability (preserves existing metadata)
        if config.manage_nfo:
            self.nfo_manager.create_movie_nfo(
                movie_path, imdb_id, final_dateadded, released, final_source, config.lock_metadata
            )
        
        # Skip remaining processing if no valid date found and file dates disabled
        if final_dateadded is None:
            _log("WARNING", f"Movie {movie_path.name} - no valid date source available, but NFO was still processed")
            self.db.upsert_movie_dates(imdb_id, released, None, source, True)
            return
            
        # Update dateadded and source for the rest of processing
        dateadded = final_dateadded
        source = final_source
        
        _log("DEBUG", f"Movie {movie_path.name} proceeding to save: dateadded={dateadded}, source={source}")
        
        # Update file mtimes (only if we have a valid date)
        if config.fix_dir_mtimes and dateadded and dateadded != "MANUAL_REVIEW_NEEDED":
            self.nfo_manager.update_movie_files_mtime(movie_path, dateadded)
        
        _log("DEBUG", f"Movie processing reached file mtime section: fix_dir_mtimes={config.fix_dir_mtimes}, dateadded={dateadded}")
        
        # Save to database
        _log("DEBUG", f"About to save to database: imdb_id={imdb_id}, dateadded={dateadded}")
        try:
            self.db.upsert_movie_dates(imdb_id, released, dateadded, source, True)
            _log("DEBUG", f"Database save completed for {imdb_id}")
        except Exception as e:
            _log("ERROR", f"Database save failed for {imdb_id}: {e}")
            raise
        
        _log("INFO", f"Completed processing movie: {movie_path.name} (source: {source})")
    
    def _extract_dates_from_tmdb_nfo(self, nfo_path: Path) -> Optional[Dict[str, str]]:
        """Extract date information from TMDB-based NFO file"""
        if not nfo_path.exists():
            return None
            
        try:
            root = self.nfo_manager._parse_nfo_with_tolerance(nfo_path)
            
            # Look for premiered date (from TMDB)
            premiered_elem = root.find('.//premiered')
            if premiered_elem is not None and premiered_elem.text:
                premiered_date = premiered_elem.text.strip()
                print(f"✅ Found TMDB premiered date: {premiered_date}")
                
                return {
                    "dateadded": premiered_date,
                    "source": "tmdb:premiered_from_nfo",
                    "released": premiered_date
                }
                
        except (ET.ParseError, Exception) as e:
            print(f"⚠️ Error parsing TMDB NFO for dates: {e}")
            
        return None
    
    def _decide_movie_dates(self, imdb_id: str, movie_path: Path, should_query: bool, existing: Optional[Dict]) -> Tuple[str, str, Optional[str]]:
        """Decide movie dates based on configuration and available data"""
        _log("DEBUG", f"_decide_movie_dates for {imdb_id}: should_query={should_query}, existing={existing}")
        
        if not should_query and existing:
            _log("DEBUG", f"Using existing data without querying: dateadded={existing.get('dateadded')}, source={existing.get('source')}")
            return existing["dateadded"], existing["source"], existing.get("released")
        
        # Query Radarr for movie info
        radarr_movie = None
        if should_query and self.radarr.api_key:
            radarr_movie = self.radarr.movie_by_imdb(imdb_id)
        
        released = None
        if radarr_movie:
            released = self._parse_date_to_iso(radarr_movie.get("inCinemas"))
        
        # Try import history first if configured
        if config.movie_priority == "import_then_digital":
            import_date, import_source = None, None
            if radarr_movie:
                movie_id = radarr_movie.get("id")
                if movie_id:
                    import_date, import_source = self.radarr.get_movie_import_date(movie_id, fallback_to_file_date=config.allow_file_date_fallback)
                    _log("INFO", f"Movie {imdb_id}: Radarr import result: date={import_date}, source={import_source}")
            
            # Check for special case: rename-first scenario (should prefer release dates)
            if import_source == "radarr:db.prefer_release_dates":
                _log("INFO", f"🎯 Movie {imdb_id} has rename-first history - skipping import, preferring release dates")
                # Fall through to release date logic below
            # Check if we got a real import date or just file date fallback
            elif import_date and import_source != "radarr:db.file.dateAdded":
                # Convert import date to local timezone for NFO files
                local_import_date = convert_utc_to_local(import_date)
                _log("INFO", f"✅ Movie {imdb_id}: Using import date {local_import_date} from {import_source}")
                return local_import_date, import_source, released
            
            # Get digital release date for comparison/fallback
            _log("INFO", f"🔍 Movie {imdb_id}: Trying digital release date fallback...")
            digital_date, digital_source = self._get_digital_release_date(imdb_id)
            _log("INFO", f"Movie {imdb_id}: Digital release result: date={digital_date}, source={digital_source}")
            
            # If we only have file date and release date exists, prefer it if reasonable and enabled
            if import_date and import_source == "radarr:db.file.dateAdded" and digital_date and config.prefer_release_dates_over_file_dates:
                # Compare dates - prefer release date if it's reasonable
                if self._should_prefer_release_over_file_date(digital_date, digital_source, released, imdb_id):
                    _log("INFO", f"✅ Movie {imdb_id}: Preferring digital release date {digital_date} over file date")
                    return digital_date, digital_source, released
                else:
                    # Convert file date to local timezone for NFO files
                    local_file_date = convert_utc_to_local(import_date)
                    _log("INFO", f"✅ Movie {imdb_id}: Keeping file date {local_file_date} - digital date not reasonable")
                    return local_file_date, import_source, released
            
            # Use whichever we have
            if import_date:
                # Convert import date to local timezone for NFO files
                local_import_date = convert_utc_to_local(import_date)
                _log("INFO", f"✅ Movie {imdb_id}: Using import date {local_import_date} from {import_source}")
                return local_import_date, import_source, released
            elif digital_date:
                _log("INFO", f"✅ Movie {imdb_id}: Using digital release date {digital_date} from {digital_source}")
                return digital_date, digital_source, released
            else:
                _log("WARNING", f"⚠️ Movie {imdb_id}: No import date OR digital release date found - trying additional fallbacks")
                
                # Try Radarr's own NFO premiered date as fallback
                radarr_premiered = self._get_radarr_nfo_premiered_date(movie_path)
                if radarr_premiered:
                    _log("INFO", f"✅ Movie {imdb_id}: Using Radarr NFO premiered date {radarr_premiered}")
                    return radarr_premiered, "radarr:nfo.premiered", released
        
        else:  # digital_then_import
            # Try digital release first
            digital_date, digital_source = self._get_digital_release_date(imdb_id)
            if digital_date:
                return digital_date, digital_source, released
            
            # Fall back to import history
            if radarr_movie:
                movie_id = radarr_movie.get("id")
                if movie_id:
                    import_date, import_source = self.radarr.get_movie_import_date(movie_id, fallback_to_file_date=config.allow_file_date_fallback)
                    if import_date:
                        # Convert import date to local timezone for NFO files
                        local_import_date = convert_utc_to_local(import_date)
                        return local_import_date, import_source, released
        
        # Last resort: file mtime (if allowed)
        if config.allow_file_date_fallback:
            return self._get_file_mtime_date(movie_path)
        else:
            _log("INFO", f"No valid dates found for {imdb_id} and file date fallback disabled - skipping NFO creation")
            
            # Log to failed movies debug file for troubleshooting
            self._log_failed_movie(movie_path, imdb_id, "No import date, no release date, file date fallback disabled")
            
            return None, "no_valid_date_source", None
    
    def _get_digital_release_date(self, imdb_id: str) -> Tuple[Optional[str], str]:
        """Get release date from external sources using configured priority"""
        _log("INFO", f"🔍 Calling external clients for {imdb_id}")
        _log("INFO", f"Release date priority: {config.release_date_priority}")
        _log("INFO", f"Smart validation enabled: {config.enable_smart_date_validation}")
        
        try:
            release_result = self.external_clients.get_release_date_by_priority(
                imdb_id, 
                config.release_date_priority,
                enable_smart_validation=config.enable_smart_date_validation
            )
            _log("INFO", f"External clients result for {imdb_id}: {release_result}")
            
            if release_result:
                _log("INFO", f"✅ Got release date: {release_result[0]} from {release_result[1]}")
                return release_result[0], release_result[1]
            else:
                _log("WARNING", f"❌ No release date found from external clients for {imdb_id}")
                return None, "release:none"
        except Exception as e:
            _log("ERROR", f"External clients error for {imdb_id}: {e}")
            return None, f"release:error:{str(e)}"
    
    def _get_radarr_nfo_premiered_date(self, movie_path: Path) -> Optional[str]:
        """Extract premiered date from Radarr's existing movie.nfo file"""
        try:
            nfo_path = movie_path / "movie.nfo"
            if not nfo_path.exists():
                _log("DEBUG", f"No existing NFO file found at {nfo_path}")
                return None
                
            nfo_content = nfo_path.read_text(encoding='utf-8')
            
            # Look for <premiered>YYYY-MM-DD</premiered>
            import re
            match = re.search(r'<premiered>(\d{4}-\d{2}-\d{2})</premiered>', nfo_content)
            if match:
                premiered_date = match.group(1)
                # Convert to ISO format with timezone
                iso_date = f"{premiered_date}T00:00:00+00:00"
                _log("INFO", f"✅ Found Radarr NFO premiered date: {premiered_date}")
                return iso_date
            else:
                _log("DEBUG", f"No <premiered> tag found in existing NFO")
                return None
                
        except Exception as e:
            _log("ERROR", f"Error reading Radarr NFO file: {e}")
            return None
    
    def _log_failed_movie(self, movie_path: Path, imdb_id: str, reason: str, available_countries: List[str] = None):
        """Log movies that failed to get valid dates to a debug file"""
        try:
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            
            failed_log_path = log_dir / "failed_movies.log"
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            log_entry = f"[{timestamp}] {movie_path.name} | IMDb: {imdb_id} | Reason: {reason}"
            if available_countries:
                log_entry += f" | Available Countries: {', '.join(available_countries)}"
            log_entry += "\n"
            
            with open(failed_log_path, "a", encoding="utf-8") as f:
                f.write(log_entry)
            
            _log("INFO", f"📝 Logged failed movie to {failed_log_path}: {movie_path.name}")
            
        except Exception as e:
            _log("ERROR", f"Failed to write to failed movies log: {e}")
    
    def _get_file_mtime_date(self, movie_path: Path) -> Tuple[str, str, Optional[str]]:
        """Get date from file modification time as last resort"""
        video_exts = (".mkv", ".mp4", ".avi", ".mov", ".m4v")
        newest_mtime = None
        
        for file_path in movie_path.iterdir():
            if file_path.is_file() and file_path.suffix.lower() in video_exts:
                try:
                    mtime = file_path.stat().st_mtime
                    if newest_mtime is None or mtime > newest_mtime:
                        newest_mtime = mtime
                except Exception:
                    continue
        
        if newest_mtime:
            try:
                # Use local timezone for file modification times
                local_tz = _get_local_timezone()
                iso_date = datetime.fromtimestamp(newest_mtime, tz=local_tz).isoformat(timespec="seconds")
                return iso_date, "file:mtime", None
            except Exception:
                pass
        
        return "MANUAL_REVIEW_NEEDED", "manual_review_required", None
    
    def _should_prefer_release_over_file_date(self, release_date: str, release_source: str, theatrical_release: Optional[str], imdb_id: str) -> bool:
        """
        Decide if release date should be preferred over file date
        
        Logic:
        - For theatrical dates: Always prefer over file dates (they're authoritative)
        - For physical dates: Usually prefer over file dates  
        - For digital dates: Prefer if reasonable (not decades before theatrical)
        """
        try:
            release_dt = datetime.fromisoformat(release_date.replace("Z", "+00:00"))
            
            # Always prefer theatrical and physical releases over file dates
            if any(release_type in release_source for release_type in ["theatrical", "physical"]):
                _log("INFO", f"Release date {release_date} ({release_source}) for {imdb_id}, preferring over file date")
                return True
            
            # If we have theatrical release date, compare digital against it
            if theatrical_release:
                theatrical_dt = datetime.fromisoformat(theatrical_release.replace("Z", "+00:00"))
                year_diff = release_dt.year - theatrical_dt.year
                
                # If digital is more than 10 years before theatrical, it's probably wrong
                if year_diff < -10:
                    _log("INFO", f"Release date {release_date} is {abs(year_diff)} years before theatrical {theatrical_release} for {imdb_id}, using file date instead")
                    return False
                    
                # If digital is within reasonable range (theatrical to +20 years), use it
                if -2 <= year_diff <= 20:
                    _log("INFO", f"Release date {release_date} is reasonable for {imdb_id} (theatrical: {theatrical_release}), preferring over file date")
                    return True
            
            # If no theatrical date, use digital if it's not absurdly old
            if release_dt.year >= 1990:  # Reasonable minimum for digital releases
                _log("INFO", f"Release date {release_date} seems reasonable for {imdb_id}, preferring over file date")
                return True
                
            _log("INFO", f"Release date {release_date} seems too old for {imdb_id}, using file date instead")
            return False
            
        except Exception as e:
            _log("WARNING", f"Error comparing dates for {imdb_id}: {e}")
            return False
    
    def _parse_date_to_iso(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO format"""
        if not date_str:
            return None
        try:
            if len(date_str) == 10 and date_str[4] == "-":
                dt = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
            else:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(timezone.utc)
            return dt.isoformat(timespec="seconds")
        except Exception:
            return None


# ---------------------------
# Batching System
# ---------------------------

class WebhookBatcher:
    """Batches webhook events to avoid processing storms"""
    
    def __init__(self, nfo_manager: NFOManager):
        self.pending: Dict[str, Dict] = {}
        self.timers: Dict[str, threading.Timer] = {}
        self.processing: Set[str] = set()
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=config.max_concurrent)
        self.nfo_manager = nfo_manager
    
    def add_webhook(self, key: str, webhook_data: Dict, media_type: str):
        """Add webhook to batch queue"""
        with self.lock:
            if key in self.timers:
                self.timers[key].cancel()
            
            webhook_data['media_type'] = media_type
            self.pending[key] = webhook_data
            _log("INFO", f"Batched {media_type} webhook for {key}")
            _log("DEBUG", f"Batch added - key: {key}, media_type: {media_type}, timer scheduled for {config.batch_delay}s")
            
            timer = threading.Timer(config.batch_delay, self._process_item, args=[key])
            self.timers[key] = timer
            timer.start()
    
    def _process_item(self, key: str):
        """Process a batched item"""
        with self.lock:
            if key in self.processing or key not in self.pending:
                return
            self.processing.add(key)
            webhook_data = self.pending.pop(key)
            self.timers.pop(key, None)
        
        try:
            self.executor.submit(self._process_sync, key, webhook_data)
        except Exception as e:
            _log("ERROR", f"Error submitting processing for {key}: {e}")
            with self.lock:
                self.processing.discard(key)
    
    def _process_sync(self, key: str, webhook_data: Dict):
        """Synchronous processing of webhook data with validation"""
        try:
            media_type = webhook_data.get('media_type')
            path_str = webhook_data.get('path')
            
            _log("DEBUG", f"Processing batch item: key={key}, media_type={media_type}, path={path_str}")
            
            if not path_str:
                _log("ERROR", f"No path found for {media_type} {key}")
                return
            
            path_obj = Path(path_str)
            if not path_obj.exists():
                _log("ERROR", f"BATCH PROCESSING FAILED: Path does not exist: {path_obj}")
                _log("ERROR", f"This indicates a path mapping issue - webhook rejected to prevent wrong processing")
                return
            
            # CRITICAL: Validate that the path contains the expected IMDb ID for movies
            if media_type == 'movie':
                expected_imdb = key.replace('movie:', '') if key.startswith('movie:') else key
                # Use comprehensive IMDb detection (directory, filenames, NFO content)
                detected_imdb = self.nfo_manager.find_movie_imdb_id(path_obj)
                
                # Check if detected IMDb matches expected (handle both full IMDb IDs and just numbers)
                imdb_match = False
                if detected_imdb:
                    if detected_imdb == expected_imdb:
                        imdb_match = True
                    elif detected_imdb.replace('tt', '') == expected_imdb.replace('tt', ''):
                        imdb_match = True
                        
                if not imdb_match:
                    _log("ERROR", f"BATCH VALIDATION FAILED: Expected IMDb {expected_imdb} not found in path {path_str}")
                    _log("ERROR", f"Detected IMDb: {detected_imdb}, Expected: {expected_imdb}")
                    _log("ERROR", f"This prevents processing wrong movies due to batch corruption")
                    return
                _log("DEBUG", f"Batch validation passed: Expected IMDb {expected_imdb} matches detected IMDb {detected_imdb}")
            
            # Process based on media type
            if media_type == 'tv':
                # Check processing mode for TV webhooks
                processing_mode = webhook_data.get('processing_mode', config.tv_webhook_processing_mode)
                episodes_data = webhook_data.get('episodes', [])
                
                if processing_mode == 'targeted' and episodes_data:
                    _log("INFO", f"Using targeted episode processing for {len(episodes_data)} episodes")
                    tv_processor.process_webhook_episodes(path_obj, episodes_data)
                else:
                    _log("INFO", f"Using series processing mode (fallback or configured)")
                    tv_processor.process_series(path_obj)
            elif media_type == 'movie':
                movie_processor.process_movie(path_obj, webhook_mode=True)
            else:
                _log("ERROR", f"Unknown media type: {media_type}")
        
        except Exception as e:
            _log("ERROR", f"Error processing {media_type} {key}: {e}")
        finally:
            with self.lock:
                self.processing.discard(key)
    
    def get_status(self) -> Dict:
        """Get batch queue status"""
        with self.lock:
            return {
                "pending_items": list(self.pending.keys()),
                "processing_items": list(self.processing),
                "pending_count": len(self.pending),
                "processing_count": len(self.processing)
            }


# ---------------------------
# FastAPI Application
# ---------------------------

# Get version
try:
    version = (Path(__file__).parent / "VERSION").read_text().strip()
except:
    version = "0.1.0"

# Check if running from dev branch (detect at runtime)
try:
    # Try to read git branch from .git/HEAD
    git_head_path = Path(__file__).parent / ".git" / "HEAD"
    if git_head_path.exists():
        head_content = git_head_path.read_text().strip()
        if "ref: refs/heads/dev" in head_content:
            version = f"{version}-dev"
        elif head_content.startswith("ref: refs/heads/"):
            # Extract branch name for other branches
            branch = head_content.split("refs/heads/")[-1]
            if branch != "main":
                version = f"{version}-{branch}"
except Exception:
    # If git detection fails, that's fine - use base version
    pass

# Check for build source (only add -gitea for local Gitea builds)
build_source = os.environ.get("BUILD_SOURCE", "")
if build_source == "gitea":
    if "gitea" not in version:  # Don't double-add gitea suffix
        version = f"{version}-gitea"

app = FastAPI(
    title="NFOGuard",
    description="Webhook server for preserving media import dates",
    version=version
)

start_time = datetime.now(timezone.utc)

# Initialize components
db = NFOGuardDatabase(config.db_path)
nfo_manager = NFOManager(config.manager_brand, config.debug)
path_mapper = PathMapper(config)  # FIXED: Pass config to PathMapper
tv_processor = TVProcessor(db, nfo_manager, path_mapper)
movie_processor = MovieProcessor(db, nfo_manager, path_mapper)
batcher = WebhookBatcher(nfo_manager)

# ---------------------------
# Webhook Handlers
# ---------------------------

async def _read_payload(request: Request) -> dict:
    """Read webhook payload from request"""
    content_type = (request.headers.get("content-type") or "").lower()
    try:
        if "application/json" in content_type:
            return await request.json()
        form = await request.form()
        if "payload" in form:
            return json.loads(form["payload"])
        return dict(form)
    except Exception as e:
        _log("ERROR", f"Failed to read webhook payload: {e}")
        return {}

@app.post("/webhook/sonarr")
async def sonarr_webhook(request: Request, background_tasks: BackgroundTasks):
    """Handle Sonarr webhooks"""
    try:
        payload = await _read_payload(request)
        if not payload:
            raise HTTPException(status_code=422, detail="Empty Sonarr payload")
        
        webhook = SonarrWebhook(**payload)
        _log("INFO", f"Received Sonarr webhook: {webhook.eventType}")
        
        if webhook.eventType not in ["Download", "Upgrade", "Rename"]:
            return {"status": "ignored", "reason": f"Event type {webhook.eventType} not processed"}
        
        if not webhook.series:
            return {"status": "ignored", "reason": "No series data"}
        
        series_info = webhook.series
        series_title = series_info.get("title", "")
        imdb_id = series_info.get("imdbId", "").replace("tt", "").strip()
        if imdb_id:
            imdb_id = f"tt{imdb_id}"
        sonarr_path = series_info.get("path", "")
        
        if not imdb_id:
            _log("ERROR", f"No IMDb ID for series: {series_title}")
            return {"status": "error", "reason": "No IMDb ID"}
        
        # Find series path
        series_path = tv_processor.find_series_path(series_title, imdb_id, sonarr_path)
        if not series_path:
            _log("ERROR", f"Could not find series directory: {series_title} ({imdb_id})")
            return {"status": "error", "reason": "Series directory not found"}
        
        # Add to batch queue with TV-prefixed key to avoid movie conflicts
        tv_batch_key = f"tv:{imdb_id}"
        webhook_dict = {
            'path': str(series_path),
            'series_info': series_info,
            'event_type': webhook.eventType,
            'episodes': webhook.episodes or [],  # Include episode data for targeted processing
            'processing_mode': config.tv_webhook_processing_mode
        }
        batcher.add_webhook(tv_batch_key, webhook_dict, 'tv')
        
        return {"status": "accepted", "message": f"Sonarr webhook queued for {tv_batch_key}"}
        
    except Exception as e:
        _log("ERROR", f"Sonarr webhook error: {e}")
        raise HTTPException(status_code=422, detail=f"Invalid webhook: {e}")

@app.post("/webhook/radarr") 
async def radarr_webhook(request: Request, background_tasks: BackgroundTasks):
    """Handle Radarr webhooks"""
    try:
        payload = await _read_payload(request)
        _log("INFO", f"Received Radarr webhook: {payload.get('eventType', 'Unknown')}")
        _log("DEBUG", f"Full Radarr webhook payload: {payload}")
        
        # Filter supported event types (same as Sonarr: Download, Upgrade, Rename)
        event_type = payload.get('eventType', '')
        if event_type not in ["Download", "Upgrade", "Rename"]:
            return {"status": "ignored", "reason": f"Event type {event_type} not processed"}
        
        # Extract movie info
        movie_data = payload.get("movie", {})
        if not movie_data:
            _log("WARNING", "No movie data in Radarr webhook")
            return {"status": "error", "message": "No movie data"}
        
        # Get IMDb ID for batching key
        imdb_id = movie_data.get("imdbId", "").lower()
        if not imdb_id:
            _log("WARNING", "No IMDb ID in Radarr webhook movie data")
            return {"status": "error", "message": "No IMDb ID"}
        
        # Get movie path and map it
        movie_path = movie_data.get("folderPath") or movie_data.get("path", "")
        if not movie_path:
            _log("ERROR", "No movie path in Radarr webhook")
            return {"status": "error", "message": "No movie path provided"}
        
        # Map the path to container path
        container_path = path_mapper.radarr_path_to_container_path(movie_path)
        _log("DEBUG", f"Mapped Radarr path {movie_path} -> {container_path}")
        
        # CRITICAL: Verify the mapped path actually exists
        from pathlib import Path
        if not Path(container_path).exists():
            _log("ERROR", f"RADARR WEBHOOK REJECTED: Mapped path does not exist: {container_path}")
            _log("ERROR", f"This prevents processing wrong movies due to path mapping issues")
            return {"status": "error", "message": f"Mapped movie path does not exist: {container_path}"}
        
        # Verify the path contains the expected IMDb ID using comprehensive detection
        detected_imdb = nfo_manager.find_movie_imdb_id(Path(container_path))
        imdb_match = False
        if detected_imdb:
            if detected_imdb == imdb_id or detected_imdb.replace('tt', '') == imdb_id.replace('tt', ''):
                imdb_match = True
        
        if not imdb_match:
            _log("WARNING", f"IMDb ID {imdb_id} not found via comprehensive detection in {container_path}")
            _log("DEBUG", f"Detected IMDb: {detected_imdb}, Expected: {imdb_id}")
        else:
            _log("DEBUG", f"IMDb ID validated: {imdb_id} matches detected {detected_imdb}")
        
        # Create movie-specific webhook data with proper path validation
        movie_webhook_data = {
            'path': container_path,  # Use verified container path
            'movie_info': movie_data,
            'event_type': payload.get('eventType'),
            'original_payload': payload
        }
        
        # Add to batch queue with movie-prefixed key to avoid TV conflicts
        movie_batch_key = f"movie:{imdb_id}"
        _log("DEBUG", f"Adding Radarr webhook to batch: key={movie_batch_key}, movie_title={movie_data.get('title', 'Unknown')}")
        batcher.add_webhook(movie_batch_key, movie_webhook_data, "movie")
        
        return {"status": "success", "message": f"Radarr webhook queued for {movie_batch_key}"}
        
    except Exception as e:
        _log("ERROR", f"Radarr webhook error: {e}")
        return {"status": "error", "message": str(e)}

# ---------------------------
# API Endpoints  
# ---------------------------

@app.get("/health")
async def health() -> HealthResponse:
    """Health check endpoint with Radarr database status"""
    uptime = datetime.now(timezone.utc) - start_time
    
    # Check NFOGuard database
    try:
        with db.get_connection() as conn:
            conn.execute("SELECT 1").fetchone()
        db_status = "healthy"
    except Exception as e:
        db_status = f"error: {e}"
    
    # Check Radarr database if available
    radarr_db_health = None
    overall_status = "healthy" if db_status == "healthy" else "degraded"
    
    # Get Radarr client with database access from movie processor
    try:
        if hasattr(movie_processor, 'radarr') and movie_processor.radarr:
            radarr_client = movie_processor.radarr
            if hasattr(radarr_client, 'db_client') and radarr_client.db_client:
                try:
                    radarr_db_health = radarr_client.db_client.health_check()
                    if radarr_db_health["status"] != "healthy":
                        overall_status = "degraded"
                except Exception as e:
                    radarr_db_health = {
                        "status": "error",
                        "error": str(e),
                        "tested_at": datetime.now(timezone.utc).isoformat(timespec="seconds")
                    }
                    overall_status = "degraded"
    except Exception as e:
        # If movie processor isn't available, skip database health check
        _log("DEBUG", f"Skipping Radarr database health check: {e}")
    
    return HealthResponse(
        status=overall_status,
        version=version,
        uptime=str(uptime),
        database_status=db_status,
        radarr_database=radarr_db_health
    )

@app.get("/stats")
async def get_stats():
    """Get database statistics"""
    try:
        return db.get_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/batch/status") 
async def batch_status():
    """Get batch queue status"""
    return batcher.get_status()

@app.get("/debug/movie/{imdb_id}")
async def debug_movie_import_date(imdb_id: str):
    """Debug endpoint to analyze movie import date detection"""
    try:
        if not imdb_id.startswith("tt"):
            imdb_id = f"tt{imdb_id}"
            
        _log("INFO", f"=== DEBUG MOVIE IMPORT DATE: {imdb_id} ===")
        
        if not (os.environ.get("RADARR_URL") and os.environ.get("RADARR_API_KEY")):
            return {
                "error": "Radarr not configured", 
                "imdb_id": imdb_id,
                "radarr_configured": False
            }
        
        # Create Radarr client
        from clients.radarr_client import RadarrClient
        radarr_client = RadarrClient(
            os.environ.get("RADARR_URL"),
            os.environ.get("RADARR_API_KEY")
        )
        
        # Look up movie
        movie_obj = radarr_client.movie_by_imdb(imdb_id)
        if not movie_obj:
            return {
                "error": f"Movie not found in Radarr for IMDb ID {imdb_id}",
                "imdb_id": imdb_id,
                "radarr_configured": True,
                "movie_found": False
            }
            
        movie_id = movie_obj.get("id")
        movie_title = movie_obj.get("title")
        
        _log("INFO", f"Found movie: {movie_title} (Radarr ID: {movie_id})")
        
        # Test the FULL movie processing pipeline (not just database lookup)
        _log("INFO", f"=== TESTING FULL MOVIE PROCESSING PIPELINE ===")
        
        # Create a dummy path for testing the decision logic
        from pathlib import Path
        dummy_path = Path("/tmp/test")
        
        try:
            # Use the global movie processor instance to test full decision logic
            global movie_processor
            if movie_processor:
                # First check external clients configuration
                _log("INFO", f"=== CHECKING EXTERNAL CLIENTS CONFIG ===")
                try:
                    tmdb_key = os.environ.get("TMDB_API_KEY", "")
                    _log("INFO", f"TMDB API Key configured: {'✅ YES' if tmdb_key else '❌ NO'}")
                    if tmdb_key:
                        _log("INFO", f"TMDB API Key length: {len(tmdb_key)} chars")
                    
                    # Check if external clients exist
                    external_clients_available = hasattr(movie_processor, 'external_clients') and movie_processor.external_clients
                    _log("INFO", f"External clients initialized: {'✅ YES' if external_clients_available else '❌ NO'}")
                    
                except Exception as e:
                    _log("ERROR", f"Error checking external clients config: {e}")
                
                # Test the full decision logic (including TMDB fallback)
                final_date, final_source, released = movie_processor._decide_movie_dates(
                    imdb_id, dummy_path, should_query=True, existing=None
                )
                
                _log("INFO", f"=== FULL PIPELINE RESULT ===")
                _log("INFO", f"Final date: {final_date}")
                _log("INFO", f"Final source: {final_source}")
                _log("INFO", f"Released (theater): {released}")
                
                return {
                    "imdb_id": imdb_id,
                    "radarr_configured": True,
                    "movie_found": True,
                    "movie_title": movie_title,
                    "movie_id": movie_id,
                    "full_pipeline_test": {
                        "final_date": final_date,
                        "final_source": final_source,
                        "theater_release": released,
                        "decision_logic": "✅ TESTED FULL PIPELINE INCLUDING TMDB FALLBACK"
                    },
                    "database_only_test": {
                        "radarr_db_result": radarr_client.get_movie_import_date(movie_id, fallback_to_file_date=True),
                        "note": "This is just the database part - fallback happens in full pipeline"
                    },
                    "debug_info": {
                        "radarr_url": os.environ.get("RADARR_URL"),
                        "movie_digital_release": movie_obj.get("digitalRelease"),
                        "movie_in_cinemas": movie_obj.get("inCinemas"),
                        "movie_physical_release": movie_obj.get("physicalRelease"),
                                                                                                                                                             "movie_folder_path": movie_obj.get("folderPath")
                    }
                }
            else:
                _log("ERROR", "Movie processor not available - testing database only")
                # Fallback to database-only testing
                import_date, source = radarr_client.get_movie_import_date(movie_id, fallback_to_file_date=True)
                return {
                    "error": "Movie processor not available - only database test performed",
                    "imdb_id": imdb_id,
                    "radarr_configured": True,
                    "movie_found": True,
                    "movie_title": movie_title,
                    "movie_id": movie_id,
                    "detected_import_date": import_date,
                    "import_source": source,
                    "debug_info": {
                        "note": "FULL PIPELINE TEST FAILED - movie processor not initialized"
                    }
                }
                
        except Exception as pipeline_error:
            _log("ERROR", f"Full pipeline test failed: {pipeline_error}")
            # Fallback to database-only testing
            import_date, source = radarr_client.get_movie_import_date(movie_id, fallback_to_file_date=True)
            return {
                "pipeline_error": str(pipeline_error),
                "imdb_id": imdb_id,
                "radarr_configured": True,
                "movie_found": True,
                "movie_title": movie_title,
                "movie_id": movie_id,
                "detected_import_date": import_date,
                "import_source": source,
                "debug_info": {
                    "note": "FULL PIPELINE TEST FAILED - showing database-only result"
                }
            }
        
    except Exception as e:
        _log("ERROR", f"Debug endpoint error for {imdb_id}: {e}")
        return {
            "error": str(e),
            "imdb_id": imdb_id,
            "success": False
        }

@app.get("/debug/movie/{imdb_id}/history")
async def debug_movie_history(imdb_id: str):
    """Detailed history analysis for a movie"""
    try:
        if not imdb_id.startswith("tt"):
            imdb_id = f"tt{imdb_id}"
            
        _log("INFO", f"=== DETAILED HISTORY ANALYSIS: {imdb_id} ===")
        
        # Use database-only mode for consistency
        if not movie_processor.radarr.db_client:
            return {"error": "Radarr database not configured"}
        
        radarr_client = movie_processor.radarr
        
        # Look up movie
        movie_obj = radarr_client.movie_by_imdb(imdb_id)
        if not movie_obj:
            return {"error": f"Movie not found for {imdb_id}"}
            
        movie_id = movie_obj.get("id")
        movie_title = movie_obj.get("title")
        
        # Get history from database instead of API
        if not radarr_client.db_client:
            return {"error": "Database-only mode required"}
        
        # TODO: Implement database-only history retrieval
        return {
            "error": "History endpoint temporarily disabled - use /debug/movie/{imdb_id}/priority for date analysis",
            "imdb_id": imdb_id,
            "movie_id": movie_id,
            "movie_title": movie_title,
            "note": "This endpoint needs database-only implementation to avoid showing wrong movie events"
        }
        
        # Analyze each event
        analyzed_events = []
        for event in all_history:
            event_type = event.get("eventType", "")
            date_str = event.get("date", "")
            event_data = event.get("data", {})
            
            # Get source path info
            source_path = (
                event_data.get('droppedPath', '') or
                event_data.get('sourcePath', '') or 
                event_data.get('path', '') or 
                event_data.get('sourceTitle', '')
            )
            
            # Analyze if this is a real import
            is_real, reason, date_iso = radarr_client._analyze_event_for_import(event)
            
            analyzed_events.append({
                "event_type": event_type,
                "date": date_str,
                "source_path": source_path,
                "is_real_import": is_real,
                "analysis_reason": reason,
                "parsed_date": date_iso,
                "full_data": event_data
            })
        
        # Find what our algorithm would pick - DATABASE ONLY
        picked_date, _ = radarr_client.get_movie_import_date(movie_id, fallback_to_file_date=True)
        
        return {
            "imdb_id": imdb_id,
            "movie_title": movie_title,
            "movie_id": movie_id,
            "total_history_events": len(all_history),
            "our_algorithm_picked": picked_date,
            "all_events": analyzed_events,
            "expected_july_date": "2025-07-07" in (picked_date or "")
        }
        
    except Exception as e:
        _log("ERROR", f"History debug error for {imdb_id}: {e}")
        return {"error": str(e)}

@app.post("/manual/scan")
async def manual_scan(background_tasks: BackgroundTasks, path: Optional[str] = None, scan_type: str = "both"):
    """Manual scan endpoint"""
    if scan_type not in ["both", "tv", "movies"]:
        raise HTTPException(status_code=400, detail="scan_type must be 'both', 'tv', or 'movies'")
    
    async def run_scan():
        paths_to_scan = []
        if path:
            paths_to_scan = [Path(path)]
        else:
            if scan_type in ["both", "tv"]:
                paths_to_scan.extend(config.tv_paths)
            if scan_type in ["both", "movies"]:  
                paths_to_scan.extend(config.movie_paths)
        
        for scan_path in paths_to_scan:
            if not scan_path.exists():
                continue
                
            if scan_type in ["both", "tv"] and (scan_path in config.tv_paths or path):
                # Handle specific season/episode path
                if path and scan_path.name.lower().startswith('season'):
                    # Single season processing
                    series_path = scan_path.parent
                    if nfo_manager.parse_imdb_from_path(series_path):
                        _log("INFO", f"Processing single season: {scan_path}")
                        try:
                            tv_processor.process_season(series_path, scan_path)
                        except Exception as e:
                            _log("ERROR", f"Failed processing season {scan_path}: {e}")
                elif path and scan_path.is_file() and scan_path.suffix.lower() in ('.mkv', '.mp4', '.avi'):
                    # Single episode processing
                    season_path = scan_path.parent
                    series_path = season_path.parent
                    if nfo_manager.parse_imdb_from_path(series_path):
                        _log("INFO", f"Processing single episode: {scan_path}")
                        try:
                            tv_processor.process_episode_file(series_path, season_path, scan_path)
                        except Exception as e:
                            _log("ERROR", f"Failed processing episode {scan_path}: {e}")
                else:
                    # Full series processing
                    for item in scan_path.iterdir():
                        if item.is_dir() and nfo_manager.parse_imdb_from_path(item):
                            try:
                                tv_processor.process_series(item)
                            except Exception as e:
                                _log("ERROR", f"Failed processing TV series {item}: {e}")
            
            if scan_type in ["both", "movies"] and scan_path in config.movie_paths:
                _log("INFO", f"Scanning movies in: {scan_path}")
                movie_count = 0
                for item in scan_path.iterdir():
                    if item.is_dir() and nfo_manager.find_movie_imdb_id(item):
                        movie_count += 1
                        _log("INFO", f"Processing movie: {item.name}")
                        try:
                            movie_processor.process_movie(item)  
                        except Exception as e:
                            _log("ERROR", f"Failed processing movie {item}: {e}")
                _log("INFO", f"Completed movie scan: {movie_count} movies processed in {scan_path}")
    
    background_tasks.add_task(run_scan)
    return {"status": "started", "message": f"Manual {scan_type} scan started"}

@app.post("/tv/scan-season")
async def scan_tv_season(background_tasks: BackgroundTasks, request: TVSeasonRequest):
    """Scan a specific TV season - URL-safe endpoint"""
    try:
        series_dir = Path(request.series_path)
        season_dir = series_dir / request.season_name
        
        if not series_dir.exists():
            raise HTTPException(status_code=404, detail=f"Series path not found: {request.series_path}")
        if not season_dir.exists():
            raise HTTPException(status_code=404, detail=f"Season path not found: {season_dir}")
        
        imdb_id = nfo_manager.parse_imdb_from_path(series_dir)
        if not imdb_id:
            raise HTTPException(status_code=400, detail="No IMDb ID found in series path")
        
        async def process_season():
            _log("INFO", f"Processing TV season: {season_dir}")
            try:
                tv_processor.process_season(series_dir, season_dir)
            except Exception as e:
                _log("ERROR", f"Failed processing season {season_dir}: {e}")
        
        background_tasks.add_task(process_season)
        return {"status": "started", "message": f"Season scan started for {request.season_name}"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tv/scan-episode")
async def scan_tv_episode(background_tasks: BackgroundTasks, request: TVEpisodeRequest):
    """Scan a specific TV episode - URL-safe endpoint"""
    try:
        series_dir = Path(request.series_path)
        season_dir = series_dir / request.season_name
        episode_file = season_dir / request.episode_name
        
        if not series_dir.exists():
            raise HTTPException(status_code=404, detail=f"Series path not found: {request.series_path}")
        if not episode_file.exists():
            raise HTTPException(status_code=404, detail=f"Episode file not found: {episode_file}")
        
        imdb_id = nfo_manager.parse_imdb_from_path(series_dir)
        if not imdb_id:
            raise HTTPException(status_code=400, detail="No IMDb ID found in series path")
        
        async def process_episode():
            _log("INFO", f"Processing TV episode: {episode_file}")
            try:
                tv_processor.process_episode_file(series_dir, season_dir, episode_file)
            except Exception as e:
                _log("ERROR", f"Failed processing episode {episode_file}: {e}")
        
        background_tasks.add_task(process_episode)
        return {"status": "started", "message": f"Episode scan started for {request.episode_name}"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/test/bulk-update")
async def test_bulk_update():
    """Test bulk update functionality without modifying data"""
    try:
        from clients.radarr_db_client import RadarrDbClient
        
        # Test Radarr database
        radarr_db = RadarrDbClient.from_env()
        if not radarr_db:
            return {"status": "error", "message": "Radarr database connection failed"}
        
        # Test query execution
        query = 'SELECT COUNT(*) FROM "Movies" m JOIN "MovieMetadata" mm ON m."MovieMetadataId" = mm."Id" WHERE mm."ImdbId" IS NOT NULL'
        with radarr_db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            movie_count = cursor.fetchone()[0]
        
        return {
            "status": "success", 
            "message": "Bulk update test passed",
            "movies_with_imdb": movie_count,
            "database_type": radarr_db.db_type
        }
    except Exception as e:
        return {"status": "error", "message": f"Bulk update test failed: {e}"}

@app.post("/test/movie-scan")
async def test_movie_scan():
    """Test movie directory scanning logic"""
    try:
        results = []
        for path in config.movie_paths:
            path_result = {
                "path": str(path),
                "exists": path.exists(),
                "movies_found": 0
            }
            
            if path.exists():
                for item in path.iterdir():
                    if item.is_dir() and nfo_manager.find_movie_imdb_id(item):
                        path_result["movies_found"] += 1
            
            results.append(path_result)
        
        total_movies = sum(r["movies_found"] for r in results)
        return {
            "status": "success",
            "message": f"Movie scan test found {total_movies} movies",
            "path_results": results
        }
    except Exception as e:
        return {"status": "error", "message": f"Movie scan test failed: {e}"}

@app.post("/bulk/update")
async def trigger_bulk_update(background_tasks: BackgroundTasks):
    """Trigger bulk update of all movies"""
    async def run_bulk_update():
        try:
            from bulk_update_movies import bulk_update_all_movies
            success = bulk_update_all_movies()
            _log("INFO", f"Bulk update completed: {'success' if success else 'failed'}")
        except Exception as e:
            _log("ERROR", f"Bulk update error: {e}")
    
    background_tasks.add_task(run_bulk_update)
    return {"status": "started", "message": "Bulk update started"}

@app.get("/debug/movie/{imdb_id}/priority")
async def debug_movie_priority_logic(imdb_id: str):
    """Debug endpoint showing how MOVIE_PRIORITY affects date selection"""
    try:
        if not imdb_id.startswith("tt"):
            imdb_id = f"tt{imdb_id}"
        
        result = {
            "imdb_id": imdb_id,
            "movie_priority": config.movie_priority,
            "release_date_priority": config.release_date_priority,
            "priority_explanation": "",
            "date_sources": {},
            "selected_date": None,
            "selected_source": None
        }
        
        # Get Radarr import date
        if movie_processor.radarr.api_key:
            radarr_movie = movie_processor.radarr.movie_by_imdb(imdb_id)
            if radarr_movie:
                movie_id = radarr_movie.get("id")
                if movie_id:
                    import_date, import_source = movie_processor.radarr.get_movie_import_date(movie_id)
                    if import_date:
                        result["date_sources"]["radarr_import"] = {
                            "date": import_date,
                            "source": import_source
                        }
        
        # Get digital release dates with detailed logging
        digital_date, digital_source = movie_processor._get_digital_release_date(imdb_id)
        if digital_date:
            result["date_sources"]["digital_release"] = {
                "date": digital_date,
                "source": digital_source
            }
        else:
            # Add debug info about why digital date wasn't found
            candidates = movie_processor.external_clients.get_digital_release_candidates(imdb_id)
            result["date_sources"]["digital_release_debug"] = {
                "candidates_found": len(candidates),
                "candidates": candidates[:3] if candidates else [],  # Show first 3
                "reason": digital_source if digital_source else "no_digital_dates_found"
            }
        
        # Show priority logic
        if config.movie_priority == "import_then_digital":
            priority_list = " → ".join(config.release_date_priority)
            result["priority_explanation"] = f"1st: Radarr import history, 2nd: Release dates ({priority_list}), 3rd: file mtime. Note: If import is only file date, prefer reasonable release dates."
            
            radarr_import = result["date_sources"].get("radarr_import")
            digital_release = result["date_sources"].get("digital_release")
            
            # Check for file date fallback logic
            if radarr_import and radarr_import["source"] == "radarr:db.file.dateAdded" and digital_release:
                # Test the smart logic
                would_prefer_digital = movie_processor._should_prefer_release_over_file_date(
                    digital_release["date"],
                    digital_release["source"], 
                    None,  # We don't have theatrical date in this debug context
                    imdb_id
                )
                result["file_date_detected"] = True
                result["would_prefer_digital"] = would_prefer_digital
                
                if would_prefer_digital:
                    result["selected_date"] = digital_release["date"]
                    result["selected_source"] = digital_release["source"] + " (preferred over file date)"
                else:
                    result["selected_date"] = radarr_import["date"]
                    result["selected_source"] = radarr_import["source"] + " (digital too old)"
            elif radarr_import and radarr_import["source"] != "radarr:db.file.dateAdded":
                result["selected_date"] = radarr_import["date"]
                result["selected_source"] = radarr_import["source"]
            elif digital_release:
                result["selected_date"] = digital_release["date"]
                result["selected_source"] = digital_release["source"]
        else:  # digital_then_import
            result["priority_explanation"] = "1st: TMDB/OMDb digital release, 2nd: Radarr import history, 3rd: file mtime"
            if result["date_sources"].get("digital_release"):
                result["selected_date"] = result["date_sources"]["digital_release"]["date"]
                result["selected_source"] = result["date_sources"]["digital_release"]["source"]
            elif result["date_sources"].get("radarr_import"):
                result["selected_date"] = result["date_sources"]["radarr_import"]["date"]
                result["selected_source"] = result["date_sources"]["radarr_import"]["source"]
        
        # Show external API status
        result["external_apis"] = {
            "tmdb_enabled": movie_processor.external_clients.tmdb.enabled,
            "omdb_enabled": movie_processor.external_clients.omdb.enabled,
            "jellyseerr_enabled": movie_processor.external_clients.jellyseerr.enabled
        }
        
        return result
        
    except Exception as e:
        return {"error": str(e), "imdb_id": imdb_id}

@app.get("/debug/tmdb/{imdb_id}")
async def debug_tmdb_lookup(imdb_id: str):
    """Debug TMDB API lookup for a specific movie"""
    try:
        if not imdb_id.startswith("tt"):
            imdb_id = f"tt{imdb_id}"
        
        result = {
            "imdb_id": imdb_id,
            "tmdb_api_enabled": movie_processor.external_clients.tmdb.enabled,
            "tmdb_api_key_configured": bool(movie_processor.external_clients.tmdb.api_key),
            "steps": {}
        }
        
        if not movie_processor.external_clients.tmdb.enabled:
            result["error"] = "TMDB API not enabled - check TMDB_API_KEY environment variable"
            return result
        
        # Step 1: Find movie by IMDb ID
        _log("INFO", f"TMDB Debug: Looking up {imdb_id}")
        tmdb_movie = movie_processor.external_clients.tmdb.find_by_imdb(imdb_id)
        result["steps"]["1_find_by_imdb"] = {
            "found": bool(tmdb_movie),
            "tmdb_movie": tmdb_movie if tmdb_movie else None
        }
        
        if not tmdb_movie:
            result["error"] = f"Movie {imdb_id} not found in TMDB"
            return result
        
        tmdb_id = tmdb_movie.get("id")
        result["tmdb_id"] = tmdb_id
        
        # Step 2: Get release dates
        if tmdb_id:
            _log("INFO", f"TMDB Debug: Getting release dates for TMDB ID {tmdb_id}")
            release_dates_result = movie_processor.external_clients.tmdb._get(f"/movie/{tmdb_id}/release_dates")
            result["steps"]["2_release_dates"] = {
                "raw_response": release_dates_result,
                "has_results": bool(release_dates_result and release_dates_result.get("results"))
            }
            
            # Step 3: Look for US digital releases
            if release_dates_result and release_dates_result.get("results"):
                us_releases = []
                for country_data in release_dates_result["results"]:
                    if country_data.get("iso_3166_1") == "US":
                        us_releases = country_data.get("release_dates", [])
                        break
                
                result["steps"]["3_us_releases"] = {
                    "found_us_data": bool(us_releases),
                    "us_releases": us_releases
                }
                
                # Step 4: Look for digital releases (type 4)
                digital_releases = [r for r in us_releases if r.get("type") == 4]
                result["steps"]["4_digital_releases"] = {
                    "digital_count": len(digital_releases),
                    "digital_releases": digital_releases
                }
        
        # Step 5: Test the full digital release function
        digital_date = movie_processor.external_clients.tmdb.get_digital_release_date(imdb_id)
        result["steps"]["5_final_result"] = {
            "digital_date": digital_date,
            "success": bool(digital_date)
        }
        
        return result
        
    except Exception as e:
        return {"error": str(e), "imdb_id": imdb_id, "traceback": str(e)}

# ---------------------------
# Main
# ---------------------------

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    _log("INFO", f"Received signal {signum}, shutting down gracefully...")
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    _log("INFO", "Starting NFOGuard")
    _log("INFO", f"Version: {version}")
    _log("INFO", f"TV paths: {[str(p) for p in config.tv_paths]}")
    _log("INFO", f"Movie paths: {[str(p) for p in config.movie_paths]}")
    _log("INFO", f"Database: {config.db_path}")
    _log("INFO", f"Config: manage_nfo={config.manage_nfo}, fix_mtimes={config.fix_dir_mtimes}")
    _log("INFO", f"Movie priority: {config.movie_priority}")
    
    try:
        uvicorn.run(
            app,
            host="0.0.0.0", 
            port=int(os.environ.get("PORT", "8080")),
            reload=False
        )
    except KeyboardInterrupt:
        _log("INFO", "NFOGuard stopped by user")
    except Exception as e:
        _log("ERROR", f"NFOGuard crashed: {e}")
        sys.exit(1)