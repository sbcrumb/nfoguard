"""
Movie Processor for NFOGuard
Handles movie processing and metadata management
"""
import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from core.database import NFOGuardDatabase
from core.nfo_manager import NFOManager
from core.path_mapper import PathMapper
from clients.radarr_client import RadarrClient
from clients.external_clients import ExternalClientManager
from config.settings import config
from utils.logging import _log
from utils.file_utils import find_media_path_by_imdb_and_title


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
        # Final fallback to UTC
        return timezone.utc


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
        """Find movie directory path using unified file utilities"""
        return find_media_path_by_imdb_and_title(
            title=movie_title,
            imdb_id=imdb_id,
            search_paths=config.movie_paths,
            webhook_path=radarr_path,
            path_mapper=self.path_mapper
        )
    
    def should_skip_movie(self, imdb_id: str, movie_name: str = "") -> Tuple[bool, str]:
        """
        Determine if we should skip processing this movie based on completion status
        
        Args:
            imdb_id: Movie IMDb ID  
            movie_name: Movie name for logging
            
        Returns:
            (should_skip: bool, reason: str)
        """
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                if self.db.db_type == "postgresql":
                    cursor.execute("""
                        SELECT dateadded, source, has_video_file
                        FROM movies 
                        WHERE imdb_id = %s
                    """, (imdb_id,))
                else:
                    cursor.execute("""
                        SELECT dateadded, source, has_video_file
                        FROM movies 
                        WHERE imdb_id = ?
                    """, (imdb_id,))
                
                result = cursor.fetchone()
                if not result:
                    return False, "No database record found"
                
                if self.db.db_type == "postgresql":
                    dateadded = result['dateadded']
                    source = result['source']
                    has_video_file = result['has_video_file']
                else:
                    dateadded = result[0] if result[0] else None
                    source = result[1] if result[1] else None  
                    has_video_file = result[2] if result[2] else False
                
                # Skip if:
                # 1. Movie has a valid dateadded timestamp
                # 2. Source is valid (not 'unknown' or 'no_valid_date_source')  
                # 3. Has video file on disk
                if (dateadded and 
                    source and 
                    source not in ['unknown', 'no_valid_date_source'] and
                    has_video_file):
                    return True, f"Complete: Has valid date '{dateadded}' from source '{source}'"
                elif not dateadded:
                    return False, "Missing dateadded"
                elif not source or source in ['unknown', 'no_valid_date_source']:
                    return False, f"Invalid source: '{source}'"
                elif not has_video_file:
                    return False, "No video file detected"
                else:
                    return False, "Incomplete movie data"
                    
        except Exception as e:
            _log("ERROR", f"Error checking movie completion for {imdb_id}: {e}")
            return False, f"Error checking completion: {e}"
    
    def process_movie(self, movie_path: Path, webhook_mode: bool = False, force_scan: bool = False, shutdown_event=None) -> str:
        """Process a movie directory"""
        imdb_id = self.nfo_manager.find_movie_imdb_id(movie_path)
        if not imdb_id:
            _log("ERROR", f"No IMDb ID found in movie directory, filenames, or NFO file: {movie_path}")
            return "error"
        
        # Handle TMDB ID fallback case
        is_tmdb_fallback = imdb_id.startswith("tmdb-")
        if is_tmdb_fallback:
            _log("INFO", f"Processing movie: {movie_path.name} (TMDB: {imdb_id})")
        else:
            _log("INFO", f"Processing movie: {movie_path.name} (IMDb: {imdb_id})")
        
        # Check if we should skip this movie (unless forced or webhook mode)
        if not force_scan and not webhook_mode:
            should_skip, reason = self.should_skip_movie(imdb_id, movie_path.name)
            if should_skip:
                _log("INFO", f"⏭️ SKIPPING MOVIE: {movie_path.name} [{imdb_id}] - {reason}")
                # Still update the movie record to track that we've seen it
                self.db.upsert_movie(imdb_id, str(movie_path))
                return "skipped"
            else:
                _log("INFO", f"🎬 PROCESSING MOVIE: {movie_path.name} [{imdb_id}] - {reason}")
        elif force_scan:
            _log("INFO", f"🔄 FORCE PROCESSING MOVIE: {movie_path.name} [{imdb_id}] - Force scan enabled")
        else:
            _log("INFO", f"📥 WEBHOOK PROCESSING MOVIE: {movie_path.name} [{imdb_id}] - Webhook mode")
        
        # Check for shutdown signal early in processing
        if shutdown_event and shutdown_event.is_set():
            _log("INFO", f"⚠️ SHUTDOWN SIGNAL RECEIVED - Stopping movie processing: {movie_path.name}")
            return "shutdown"
        
        # Update database
        self.db.upsert_movie(imdb_id, str(movie_path))
        
        # Check for video files
        video_exts = (".mkv", ".mp4", ".avi", ".mov", ".m4v")
        has_video = any(f.is_file() and f.suffix.lower() in video_exts for f in movie_path.iterdir())
        
        if not has_video:
            _log("WARNING", f"No video files found in: {movie_path} - skipping database entry")
            return "no_video_files"
        
        # TIER 1: Check database first (fastest - local lookup)
        existing = self.db.get_movie_dates(imdb_id)
        _log("DEBUG", f"Database lookup for {imdb_id}: {existing}")
        
        # Enhanced debug for database state
        if existing:
            has_dateadded = bool(existing.get("dateadded"))
            source_value = existing.get("source")
            _log("INFO", f"🔍 TIER 1 DEBUG - {imdb_id}: has_dateadded={has_dateadded}, source='{source_value}', dateadded='{existing.get('dateadded')}'")
        else:
            _log("INFO", f"🔍 TIER 1 DEBUG - {imdb_id}: No database record found")
        
        # If we have complete data in database, use it and skip all other checks
        if existing and existing.get("dateadded") and existing.get("source") != "no_valid_date_source":
            _log("INFO", f"✅ TIER 1 - Using complete database data for {imdb_id}: {existing['dateadded']} (source: {existing['source']})")
            dateadded, source, released = existing["dateadded"], existing["source"], existing.get("released")
            
            # Convert datetime objects to strings for NFO manager
            if hasattr(dateadded, 'isoformat'):
                dateadded = dateadded.isoformat()
            if released and hasattr(released, 'isoformat'):
                released = released.isoformat()
            
            # Create NFO with existing data and update files
            if config.manage_nfo:
                self.nfo_manager.create_movie_nfo(
                    movie_path, imdb_id, dateadded, released, source, config.lock_metadata
                )
            
            if config.fix_dir_mtimes and dateadded:
                self.nfo_manager.update_movie_files_mtime(movie_path, dateadded)
            
            _log("INFO", f"Completed processing movie: {movie_path.name} (source: {source}) [database-cached]")
            return "processed"
        else:
            _log("INFO", f"🔍 TIER 1 SKIP - {imdb_id}: Database incomplete, proceeding to Tier 2")
        
        # TIER 2: Check if NFO file has NFOGuard data and cache it in database
        nfo_path = movie_path / "movie.nfo"
        _log("INFO", f"🔍 TIER 2 - Checking NFO file: {nfo_path}")
        _log("INFO", f"🔍 TIER 2 - NFO exists: {nfo_path.exists()}")
        
        nfo_data = self.nfo_manager.extract_nfoguard_dates_from_nfo(nfo_path)
        _log("INFO", f"🔍 TIER 2 - NFOGuard data extracted: {nfo_data}")
        
        if nfo_data:
            _log("INFO", f"🚀 TIER 2 - Found NFOGuard data in NFO file: {nfo_data['dateadded']} (source: {nfo_data['source']})")
            dateadded = nfo_data["dateadded"]
            source = nfo_data["source"] 
            released = nfo_data.get("released")
            
            # Cache NFO data in database for future lookups  
            # Fixed parameter order: imdb_id, released, dateadded, source
            self.db.upsert_movie_dates(imdb_id, released, dateadded, source, True)
            _log("INFO", f"✅ Cached NFO data in database for {imdb_id}")
            
            # Update file mtimes if enabled (NFO is already correct)
            if config.fix_dir_mtimes and dateadded:
                self.nfo_manager.update_movie_files_mtime(movie_path, dateadded)
            
            _log("INFO", f"Completed processing movie: {movie_path.name} (source: {source}) [nfo-cached]")
            return "processed"
            
        # TIER 2.5: Check for any existing valid date data in NFO (even without lockdata marker)
        # Only use NFO dates if prioritize_nfo is enabled, otherwise check external APIs first
        existing_nfo_data = self._extract_any_valid_dates_from_nfo(nfo_path)
        if existing_nfo_data and config.manual_scan_prioritize_nfo:
            _log("INFO", f"🔍 TIER 2.5 - Found existing date data in NFO (no lockdata): {existing_nfo_data['dateadded']} (source: {existing_nfo_data['source']})")
            _log("INFO", f"⚡ MANUAL_SCAN_PRIORITIZE_NFO=True - Using NFO date for speed")
            dateadded = existing_nfo_data["dateadded"]
            source = existing_nfo_data["source"]
            released = existing_nfo_data.get("released")
            
            # Cache existing data in database and add proper NFOGuard formatting
            # Fixed parameter order: imdb_id, released, dateadded, source
            self.db.upsert_movie_dates(imdb_id, released, dateadded, source, True)
            _log("INFO", f"✅ Cached existing NFO data in database for {imdb_id}")
            
            # Update NFO file to add NFOGuard formatting (lockdata, comment)
            if config.manage_nfo:
                self.nfo_manager.create_movie_nfo(
                    movie_path, imdb_id, dateadded, released, source, config.lock_metadata
                )
                _log("INFO", f"✅ Added NFOGuard formatting to existing NFO for {imdb_id}")
            
            # Update file mtimes if enabled
            if config.fix_dir_mtimes and dateadded:
                self.nfo_manager.update_movie_files_mtime(movie_path, dateadded)
            
            _log("INFO", f"Completed processing movie: {movie_path.name} (source: {source}) [existing-nfo-enhanced]")
            return "processed"
        elif existing_nfo_data:
            _log("INFO", f"🔍 TIER 2.5 - Found existing date data in NFO (no lockdata): {existing_nfo_data['dateadded']} (source: {existing_nfo_data['source']})")
            _log("INFO", f"🎯 MANUAL_SCAN_PRIORITIZE_NFO=False - Will verify against external APIs first")
            # Store NFO data as fallback but continue to TIER 3 to check external APIs
            nfo_fallback_data = existing_nfo_data
            
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
                return "processed"
            
        # TIER 3: No cached data found - proceed with API lookups and verification
        
        # Check for shutdown signal before expensive API operations
        if shutdown_event and shutdown_event.is_set():
            _log("INFO", f"⚠️ SHUTDOWN SIGNAL RECEIVED - Stopping movie processing before API calls: {movie_path.name}")
            return "shutdown"
        
        # TIER 3: No cached data found - determine if we should query APIs
        if webhook_mode:
            _log("INFO", f"Webhook processing - no cached data found, using full date decision logic")
            should_query = True  # Always query for webhooks when no cached data exists
        else:
            # Manual scan mode - determine if we should query APIs
            should_query = config.movie_poll_mode == "always"
            _log("DEBUG", f"Movie {imdb_id}: should_query={should_query}, poll_mode={config.movie_poll_mode}")
        
        # Use existing movie date decision logic
        # Pass NFO fallback data if available for cases where external APIs don't have import history
        nfo_fallback = locals().get('nfo_fallback_data', None)
        dateadded, source, released = self._decide_movie_dates(imdb_id, movie_path, should_query, nfo_fallback)
        
        # Webhook fallback: if ALL date sources fail, use current timestamp
        if webhook_mode and dateadded is None:
            local_tz = _get_local_timezone()
            current_time = datetime.now(local_tz).isoformat(timespec="seconds")
            _log("INFO", f"Webhook processing - all date sources failed, using current timestamp as last resort: {current_time}")
            dateadded, source = current_time, "webhook:fallback_timestamp"
        
        # If we don't have an import/download date but we have a release date, use it as dateadded
        # This ensures we save digital release dates, theatrical dates, etc. to the database
        final_dateadded = dateadded
        final_source = source
        
        if dateadded is None and released is not None:
            final_dateadded = released
            final_source = f"{source}_as_dateadded" if source else "release_date_fallback"
            _log("INFO", f"Using release date as dateadded: {final_dateadded} (source: {final_source})")
        
        # Create NFO regardless of date availability (preserves existing metadata)
        if config.debug:
            print(f"🔍 TIER3 - config.manage_nfo: {config.manage_nfo}")
        if config.manage_nfo:
            if config.debug:
                print(f"🔍 TIER3 - Calling create_movie_nfo with final_dateadded: {final_dateadded}")
            self.nfo_manager.create_movie_nfo(
                movie_path, imdb_id, final_dateadded, released, final_source, config.lock_metadata
            )
        else:
            if config.debug:
                print(f"❌ TIER3 - manage_nfo is disabled, skipping NFO creation")
        
        # Skip remaining processing if no valid date found and file dates disabled
        if final_dateadded is None:
            _log("WARNING", f"Movie {movie_path.name} - no valid date source available, but NFO was still processed")
            self.db.upsert_movie_dates(imdb_id, released, None, source, True)
            return "processed"
            
        # Update dateadded and source for the rest of processing
        dateadded = final_dateadded
        source = final_source
        
        _log("DEBUG", f"Movie {movie_path.name} proceeding to save: dateadded={dateadded}, source={source}")
        
        # Update file mtimes (only if we have a valid date)
        if config.fix_dir_mtimes and dateadded and dateadded != "MANUAL_REVIEW_NEEDED":
            self.nfo_manager.update_movie_files_mtime(movie_path, dateadded)
        
        _log("DEBUG", f"Movie processing reached file mtime section: fix_dir_mtimes={config.fix_dir_mtimes}, dateadded={dateadded}")
        
        # Yield control briefly during movie processing to allow web interface requests
        import time
        time.sleep(0.005)  # 5ms yield per movie to improve responsiveness
        
        # Save to database
        _log("DEBUG", f"About to save to database: imdb_id={imdb_id}, dateadded={dateadded}")
        try:
            self.db.upsert_movie_dates(imdb_id, released, dateadded, source, True)
            _log("DEBUG", f"Database save completed for {imdb_id}")
        except Exception as e:
            _log("ERROR", f"Database save failed for {imdb_id}: {e}")
            raise
        
        _log("INFO", f"Completed processing movie: {movie_path.name} (source: {source})")
        return "processed"
    
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
    
    def _extract_any_valid_dates_from_nfo(self, nfo_path: Path) -> Optional[Dict[str, str]]:
        """Extract any valid date information from NFO file, even without NFOGuard markers"""
        if not nfo_path.exists():
            return None
            
        try:
            import xml.etree.ElementTree as ET
            tree = ET.parse(nfo_path)
            root = tree.getroot()
            
            # Look for dateadded element (indicates previously processed by NFOGuard or similar)
            dateadded_elem = root.find('.//dateadded')
            premiered_elem = root.find('.//premiered')
            
            if dateadded_elem is not None and dateadded_elem.text:
                dateadded = dateadded_elem.text.strip()
                
                # Try to determine source from NFOGuard comment if present
                source = "existing_nfo_data"
                try:
                    nfo_content = nfo_path.read_text(encoding='utf-8')
                    import re
                    # Look for NFOGuard comment pattern
                    source_match = re.search(r'<!--.*?NFOGuard.*?Source:\s*([^-\s]+).*?-->', nfo_content, re.DOTALL | re.IGNORECASE)
                    if source_match:
                        source = source_match.group(1).strip()
                        _log("DEBUG", f"Found source in NFOGuard comment: {source}")
                    else:
                        # Try to infer source from dateadded format/content
                        if "tmdb" in nfo_content.lower() or (premiered_elem and premiered_elem.text):
                            source = "tmdb:digital"
                        elif "radarr" in nfo_content.lower():
                            source = "radarr:db.history.import"
                        else:
                            source = "existing_nfo_data"
                        _log("DEBUG", f"Inferred source from NFO content: {source}")
                except Exception as e:
                    _log("DEBUG", f"Could not determine source from NFO content: {e}")
                
                result = {
                    "dateadded": dateadded,
                    "source": source
                }
                
                if premiered_elem is not None and premiered_elem.text:
                    result["released"] = premiered_elem.text.strip()
                
                _log("INFO", f"✅ Found existing date data in NFO: dateadded={dateadded}, source={source}")
                return result
                
        except (ET.ParseError, Exception) as e:
            _log("DEBUG", f"Error parsing NFO for existing date data: {e}")
            
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
        
        # Last resort: check if we have NFO fallback data (when external APIs don't have import history)
        if existing and existing.get('dateadded'):
            _log("INFO", f"✅ Movie {imdb_id}: External APIs don't have import history, using NFO fallback date: {existing['dateadded']} (source: {existing['source']})")
            return existing["dateadded"], f"nfo_fallback:{existing['source']}", existing.get("released")
        
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