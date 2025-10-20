"""
TV Series Processor for NFOGuard
Handles TV series processing and episode management with async I/O support
"""
import os
import re
import time
import asyncio
from pathlib import Path
from typing import Optional, Dict, List, Set, Tuple, Any
from datetime import datetime

from core.database import NFOGuardDatabase
from core.nfo_manager import NFOManager
from core.async_nfo_manager import AsyncNFOManager
from core.path_mapper import PathMapper
from clients.sonarr_client import SonarrClient
from clients.external_clients import ExternalClientManager
from config.settings import config
from utils.logging import _log
from utils.file_utils import (
    find_media_path_by_imdb_and_title,
    find_episodes_on_disk,
    extract_title_from_directory_name
)
from utils.async_file_utils import (
    async_find_episodes_on_disk,
    async_concurrent_episode_processing
)


class TVProcessor:
    """Handles TV series processing"""
    
    def __init__(self, db: NFOGuardDatabase, nfo_manager: NFOManager, path_mapper: PathMapper):
        self.db = db
        self.nfo_manager = nfo_manager
        self.async_nfo_manager = AsyncNFOManager(config.manager_brand, config.debug)
        self.path_mapper = path_mapper
        self.sonarr = SonarrClient(
            os.environ.get("SONARR_URL", ""),
            os.environ.get("SONARR_API_KEY", "")
        )
        self.external_clients = ExternalClientManager()
    
    def find_series_path(self, series_title: str, imdb_id: str, sonarr_path: str = None) -> Optional[Path]:
        """Find series directory path using unified file utilities"""
        return find_media_path_by_imdb_and_title(
            title=series_title,
            imdb_id=imdb_id,
            search_paths=config.tv_paths,
            webhook_path=sonarr_path,
            path_mapper=self.path_mapper
        )
    
    def should_skip_series_fast(self, imdb_id: str, series_name: str = "") -> Tuple[bool, str, int]:
        """
        Fast preliminary check to skip series without filesystem scan
        
        Args:
            imdb_id: Series IMDb ID
            series_name: Series name for logging
            
        Returns:
            (should_skip: bool, reason: str, episodes_in_db: int)
        """
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if we have complete episodes in database
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_in_db,
                        COUNT(CASE WHEN dateadded IS NOT NULL AND source IS NOT NULL AND source != 'unknown' AND source != 'no_valid_date_source' THEN 1 END) as complete_episodes
                    FROM episodes 
                    WHERE imdb_id = %s
                """, (imdb_id,))
                
                result = cursor.fetchone()
                if not result:
                    return False, "No database records found", 0
                
                total_in_db = result['total_in_db']
                complete_episodes = result['complete_episodes']
                
                # Skip if we have episodes and all are complete
                # We'll verify disk count later if needed
                if total_in_db > 0 and complete_episodes == total_in_db:
                    return True, f"Likely complete: {complete_episodes} episodes in DB all have valid dates", total_in_db
                else:
                    return False, f"Needs checking: {complete_episodes}/{total_in_db} episodes complete in DB", total_in_db
                    
        except Exception as e:
            _log("ERROR", f"Error in fast series check for {imdb_id}: {e}")
            return False, f"Error in fast check: {e}", 0

    def should_skip_series(self, imdb_id: str, episodes_on_disk: int, series_name: str = "") -> Tuple[bool, str]:
        """
        Determine if we should skip processing this series based on completion status
        
        Args:
            imdb_id: Series IMDb ID
            episodes_on_disk: Number of episodes found on disk
            series_name: Series name for logging
            
        Returns:
            (should_skip: bool, reason: str)
        """
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                # PostgreSQL-only query
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_in_db,
                        COUNT(CASE WHEN dateadded IS NOT NULL AND source IS NOT NULL AND source != 'unknown' AND source != 'no_valid_date_source' THEN 1 END) as complete_episodes
                    FROM episodes 
                    WHERE imdb_id = %s
                """, (imdb_id,))
                
                result = cursor.fetchone()
                if not result:
                    return False, "No database records found"
                
                # PostgreSQL RealDictCursor returns dict-like objects
                total_in_db = result['total_in_db']
                complete_episodes = result['complete_episodes']
                
                # Skip if:
                # 1. We have episodes in database
                # 2. Database count matches disk count (no missing episodes)
                # 3. All episodes have valid dates and sources
                if total_in_db > 0 and total_in_db == episodes_on_disk and complete_episodes == episodes_on_disk:
                    return True, f"Complete: {complete_episodes}/{episodes_on_disk} episodes have valid dates"
                elif total_in_db == 0:
                    return False, f"New series: No episodes in database"
                elif total_in_db != episodes_on_disk:
                    return False, f"Disk mismatch: {total_in_db} in DB vs {episodes_on_disk} on disk"
                else:
                    return False, f"Incomplete: {complete_episodes}/{episodes_on_disk} episodes have valid dates"
                    
        except Exception as e:
            _log("ERROR", f"Error checking series completion for {imdb_id}: {e}")
            return False, f"Error checking completion: {e}"
    
    def process_series(self, series_path: Path, force_scan: bool = False) -> str:
        """Process a TV series directory"""
        imdb_id = self.nfo_manager.parse_imdb_from_path(series_path)
        if not imdb_id:
            _log("ERROR", f"No IMDb ID found in series path: {series_path}")
            return "error"
        
        _log("INFO", f"Processing TV series: {series_path.name}")
        
        # Fast check first - avoid expensive filesystem scan if possible
        if not force_scan:
            should_skip_fast, reason_fast, episodes_in_db = self.should_skip_series_fast(imdb_id, series_path.name)
            if should_skip_fast:
                _log("INFO", f"⚡ FAST SKIP: {series_path.name} [{imdb_id}] - {reason_fast}")
                # Still update the series record to track that we've seen it
                self.db.upsert_series(imdb_id, str(series_path))
                return "skipped"
        
        # Need filesystem scan - either force_scan=True or series not complete in DB
        disk_episodes = find_episodes_on_disk(series_path)
        _log("INFO", f"Found {len(disk_episodes)} episodes on disk")
        
        # Final skip check with actual episode count (unless forced)
        if not force_scan:
            should_skip, reason = self.should_skip_series(imdb_id, len(disk_episodes), series_path.name)
            if should_skip:
                _log("INFO", f"⏭️ SKIPPING SERIES: {series_path.name} [{imdb_id}] - {reason}")
                # Still update the series record to track that we've seen it
                self.db.upsert_series(imdb_id, str(series_path))
                return "skipped"
            else:
                _log("INFO", f"📺 PROCESSING SERIES: {series_path.name} [{imdb_id}] - {reason}")
        else:
            _log("INFO", f"🔄 FORCE PROCESSING SERIES: {series_path.name} [{imdb_id}] - Force scan enabled")
        
        # Update database
        self.db.upsert_series(imdb_id, str(series_path))
        
        # Get episode dates
        episode_dates = self._gather_episode_dates(series_path, imdb_id, disk_episodes)
        
        # Process episodes with periodic yielding for non-blocking operation
        episode_count = 0
        for (season, episode), (aired, dateadded, source) in episode_dates.items():
            if (season, episode) in disk_episodes:
                episode_count += 1
                
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
                try:
                    self.db.upsert_episode_date(imdb_id, season, episode, aired, dateadded, source, True)
                    _log("DEBUG", f"S{season:02d}E{episode:02d}: Database record saved successfully")
                except Exception as e:
                    _log("ERROR", f"S{season:02d}E{episode:02d}: Database write failed: {e}")
                    # Continue processing other episodes
                
                # Yield control every 3 episodes to allow other requests (webhooks, web interface)
                if episode_count % 3 == 0:
                    import time
                    time.sleep(0.1)  # 100ms yield to improve responsiveness during episode processing
                    _log("DEBUG", f"Processed {episode_count} episodes, yielding to allow other requests...")
        
        # Skip season.nfo and tvshow.nfo creation - focus only on episode NFOs
        pass
        
        _log("INFO", f"Completed processing TV series: {series_path.name}")
        return "processed"
    
    def _extract_series_title_from_path(self, series_path: Path) -> Optional[str]:
        """Extract series title from directory path using unified file utilities"""
        return extract_title_from_directory_name(series_path.name)
    
    
    def _gather_episode_dates(self, series_path: Path, imdb_id: str, disk_episodes: Dict[Tuple[int, int], List[Path]]) -> Dict[Tuple[int, int], Tuple[Optional[str], Optional[str], str]]:
        """Gather episode air dates and date added information with database-first optimization"""
        _log("INFO", f"🎯 GATHERING EPISODE DATES for {imdb_id}: {len(disk_episodes)} episodes on disk")
        episode_dates = {}
        episodes_needing_lookup = []
        
        # TIER 1: Check database first for existing dates (fastest)
        _log("DEBUG", f"TIER 1 - Checking database for existing episode dates for {len(disk_episodes)} episodes")
        db_cache_hits = 0
        episodes_needing_nfo_check = []
        
        for (season, episode) in disk_episodes:
            # Try database first - this is much faster than API calls
            db_result = self.db.get_episode_date(imdb_id, season, episode)
            
            if db_result and db_result.get('dateadded'):
                # Found in database - use cached data
                aired = db_result.get('aired')
                dateadded = db_result.get('dateadded')
                source = db_result.get('source', 'database_cache')
                episode_dates[(season, episode)] = (aired, dateadded, source)
                db_cache_hits += 1
                _log("DEBUG", f"Database cache hit for S{season:02d}E{episode:02d}: {dateadded}")
            else:
                # Not in database or incomplete - needs NFO check
                episodes_needing_nfo_check.append((season, episode))
        
        _log("INFO", f"Database cache hits: {db_cache_hits}/{len(disk_episodes)} episodes. Need NFO check: {len(episodes_needing_nfo_check)}")
        
        # TIER 2: Check NFO files for NFOGuard dates and cache them in database
        nfo_cache_hits = 0
        episodes_needing_lookup = []
        
        if episodes_needing_nfo_check:
            _log("DEBUG", f"TIER 2 - Checking NFO files for NFOGuard dates for {len(episodes_needing_nfo_check)} episodes")
            
            for (season, episode) in episodes_needing_nfo_check:
                # Look for existing NFO files for this episode
                season_dir = series_path / config.tv_season_dir_format.format(season=season)
                episode_files = disk_episodes[(season, episode)]
                
                nfo_found = False
                for episode_file in episode_files:
                    # Try to find matching NFO file
                    nfo_path = episode_file.with_suffix('.nfo')
                    if nfo_path.exists():
                        # Extract NFOGuard data from episode NFO
                        nfo_data = self.nfo_manager.extract_nfoguard_dates_from_nfo(nfo_path)
                        if nfo_data:
                            aired = nfo_data.get('aired') 
                            dateadded = nfo_data.get('dateadded')
                            source = nfo_data.get('source', 'nfo_cache')
                            
                            _log("DEBUG", f"S{season:02d}E{episode:02d}: NFO data found - aired={aired}, dateadded={dateadded}, source={source}")
                            
                            # Skip incomplete NFO files with "unknown" source or no useful dates
                            if source == "unknown" and not dateadded and not aired:
                                _log("INFO", f"S{season:02d}E{episode:02d}: Ignoring incomplete NFO file with source 'unknown' and no dates")
                                break  # Break out of NFO search to mark episode as needing API lookup
                            
                            # Apply fallback logic if NFO has aired but no dateadded
                            if not dateadded and aired:
                                dateadded = aired
                                source = f"{source}_aired_fallback" if source != 'nfo_cache' else 'nfo_aired_fallback'
                                _log("DEBUG", f"S{season:02d}E{episode:02d}: NFO has aired but no dateadded, using aired as fallback: {dateadded}")
                            
                            if dateadded:
                                episode_dates[(season, episode)] = (aired, dateadded, source)
                                nfo_cache_hits += 1
                                nfo_found = True
                                
                                # Cache NFO data in database for future lookups
                                self.db.upsert_episode_date(imdb_id, season, episode, aired, dateadded, source, True)
                                _log("DEBUG", f"NFO cache hit for S{season:02d}E{episode:02d}: {dateadded} - cached in DB")
                                break
                
                if not nfo_found:
                    # No NFO data found - needs API lookup
                    episodes_needing_lookup.append((season, episode))
            
            _log("INFO", f"NFO cache hits: {nfo_cache_hits}/{len(episodes_needing_nfo_check)} episodes. Need API lookup: {len(episodes_needing_lookup)}")
        
        # TIER 3: Only call Sonarr API for episodes not in database or NFO files
        if episodes_needing_lookup:
            _log("DEBUG", f"TIER 3 - Querying Sonarr for {len(episodes_needing_lookup)} episodes missing from database and NFO files")
            sonarr_episodes = self._get_sonarr_episodes(imdb_id, episodes_needing_lookup)
            
            # Process episodes that needed lookup
            for (season, episode) in episodes_needing_lookup:
                aired = None
                dateadded = None
                source = "unknown"
                
                # Try Sonarr first
                if (season, episode) in sonarr_episodes:
                    sonarr_data = sonarr_episodes[(season, episode)]
                    aired = sonarr_data.get('airDate')
                    dateadded = sonarr_data.get('dateAdded')
                    if dateadded:
                        source = "sonarr:history.import"
                        _log("DEBUG", f"S{season:02d}E{episode:02d}: Got Sonarr import date: {dateadded}")
                    else:
                        # Sonarr has episode data but no import date - update source for better fallback handling
                        source = "sonarr:no_import_date"
                        _log("DEBUG", f"S{season:02d}E{episode:02d}: Sonarr has data but no dateAdded (aired: {aired})")
                
                # Fallback to external sources if needed
                if not aired:
                    _log("DEBUG", f"S{season:02d}E{episode:02d}: No aired date from Sonarr, trying external APIs for {imdb_id}")
                    external_aired = self.external_clients.get_episode_air_date(imdb_id, season, episode)
                    if external_aired:
                        aired = external_aired
                        if not dateadded:
                            source = "external"
                        _log("INFO", f"S{season:02d}E{episode:02d}: Found aired date from external APIs: {aired}")
                    else:
                        _log("WARNING", f"S{season:02d}E{episode:02d}: No aired date found from external APIs for {imdb_id}")
                
                # Use air date as fallback for dateadded if no import date found
                if not dateadded and aired:
                    # Always use air date as fallback when no import date is available
                    dateadded = aired
                    if source == "sonarr:no_import_date":
                        source = "sonarr:aired_fallback"
                    elif source == "sonarr:history.import":
                        # This shouldn't happen but handle it gracefully
                        source = "sonarr:aired_fallback"
                    else:
                        source = f"{source}_fallback" if source != "unknown" else "aired_fallback"
                    _log("DEBUG", f"S{season:02d}E{episode:02d}: Using aired date as fallback: {dateadded} (source: {source})")
                
                # Ensure air date is saved to database even if used as dateadded fallback
                if aired and not dateadded:
                    # This is a fallback for cases where we have air date but absolutely no dateadded
                    dateadded = aired
                    source = "aired_only_fallback"
                    _log("INFO", f"S{season:02d}E{episode:02d}: No import date found, using air date as both aired and dateadded: {dateadded}")
                
                episode_dates[(season, episode)] = (aired, dateadded, source)
        
        _log("INFO", f"🎯 EPISODE DATES GATHERED: {len(episode_dates)} episodes with dates")
        for (s, e), (aired, dateadded, source) in episode_dates.items():
            _log("INFO", f"   S{s:02d}E{e:02d}: aired={aired}, dateadded={dateadded}, source={source}")
        
        return episode_dates
    
    def _get_sonarr_episodes(self, imdb_id: str, episodes_filter: List[Tuple[int, int]] = None) -> Dict[Tuple[int, int], Dict[str, Any]]:
        """Get episode information from Sonarr including import history - optimized to only fetch needed episodes"""
        try:
            series_data = self.sonarr.series_by_imdb(imdb_id)
            if not series_data:
                # Try fuzzy matching if exact IMDb lookup fails
                _log("DEBUG", f"Exact IMDb lookup failed for {imdb_id}, trying fuzzy matching")
                
                # Get all series and try fuzzy matching
                all_series = self.sonarr.get_all_series()
                if all_series:
                    _log("DEBUG", f"Found {len(all_series)} total series in Sonarr")
                    
                    for series in all_series:
                        series_imdb = series.get('imdbId', '')
                        if series_imdb and series_imdb.startswith('tt'):
                            # Try fuzzy matching for IMDb numbers
                            try:
                                target_imdb_num = imdb_id.replace('tt', '').lower()
                                series_imdb_num = series_imdb.replace('tt', '').lower()
                                
                                target_num = int(target_imdb_num)
                                series_num = int(series_imdb_num)
                                diff = abs(target_num - series_num)
                                
                                if diff <= 10:  # Allow small IMDb ID differences
                                    _log("INFO", f"✅ Found fuzzy IMDb match: {series_imdb} vs {imdb_id} (diff: {diff})")
                                    _log("DEBUG", f"Series data found: True")
                                    _log("DEBUG", f"Found series '{series.get('title', 'Unknown')}' with ID {series.get('id')}")
                                    series_data = series
                                    break
                            except (ValueError, TypeError):
                                continue
                
                if not series_data:
                    return {}
            
            series_id = series_data.get('id')
            if not series_id:
                return {}
            
            episodes = self.sonarr.episodes_for_series(series_id)
            
            # Convert episodes_filter to set for faster lookup
            filter_set = set(episodes_filter) if episodes_filter else None
            
            episode_map = {}
            api_calls_made = 0
            episodes_processed = 0
            
            for episode in episodes:
                season = episode.get('seasonNumber', 0)
                episode_num = episode.get('episodeNumber', 0)
                
                # Skip episodes not in filter if filter is provided
                if filter_set and (season, episode_num) not in filter_set:
                    continue
                
                if season >= 0 and episode_num > 0:
                    episodes_processed += 1
                    
                    # Get basic episode info
                    episode_data = {
                        'airDate': episode.get('airDate'),
                        'dateAdded': None
                    }
                    
                    # First try to get import date from history (more accurate)
                    episode_id = episode.get('id')
                    if episode_id and episode.get('hasFile'):
                        import_date = self.sonarr.get_episode_import_history(episode_id)
                        api_calls_made += 1
                        if import_date:
                            episode_data['dateAdded'] = import_date
                            _log("DEBUG", f"Got import date from history for S{season:02d}E{episode_num:02d}: {import_date}")
                        
                        # Yield control every 5 API calls to allow other requests
                        if api_calls_made % 5 == 0:
                            import time
                            time.sleep(0.01)  # 10ms yield to other processes
                            _log("DEBUG", f"Yielded after {api_calls_made} Sonarr API calls to allow other requests...")
                    
                    # Fallback to episodeFile.dateAdded if history didn't work
                    if not episode_data['dateAdded'] and episode.get('hasFile'):
                        file_date = episode.get('episodeFile', {}).get('dateAdded')
                        if file_date:
                            episode_data['dateAdded'] = file_date
                            _log("DEBUG", f"Got file date for S{season:02d}E{episode_num:02d}: {file_date}")
                    
                    episode_map[(season, episode_num)] = episode_data
                    
                    # Also yield control every 20 episodes processed to prevent blocking
                    if episodes_processed % 20 == 0:
                        import time
                        time.sleep(0.01)  # 10ms yield for large episode lists
                        _log("DEBUG", f"Processed {episodes_processed} episodes, yielding to allow other requests...")
            
            if filter_set:
                _log("DEBUG", f"Made {api_calls_made} Sonarr history API calls for filtered episodes (instead of all episodes)")
            
            return episode_map
            
        except Exception as e:
            _log("ERROR", f"Failed to get Sonarr episodes for {imdb_id}: {e}")
            return {}
    
    def process_season(self, series_path: str, season_name: str) -> Dict[str, Any]:
        """Process a specific season"""
        series_path_obj = Path(series_path)
        if not series_path_obj.exists():
            raise FileNotFoundError(f"Series path not found: {series_path}")
        
        season_path = series_path_obj / season_name
        if not season_path.exists():
            raise FileNotFoundError(f"Season directory not found: {season_path}")
        
        # Extract season number from directory name
        season_match = re.search(r'(\d+)', season_name)
        if not season_match:
            raise ValueError(f"Could not extract season number from: {season_name}")
        
        season_num = int(season_match.group(1))
        
        # Get series IMDb ID
        imdb_id = self.nfo_manager.parse_imdb_from_path(series_path_obj)
        if not imdb_id:
            raise ValueError(f"No IMDb ID found in series path: {series_path}")
        
        _log("INFO", f"Processing season {season_num} of series: {series_path_obj.name}")
        
        # Find episodes in this season
        disk_episodes = find_episodes_on_disk(series_path_obj)
        season_episodes = {k: v for k, v in disk_episodes.items() if k[0] == season_num}
        
        if not season_episodes:
            return {"status": "no_episodes", "season": season_num, "episodes_found": 0}
        
        # Get episode dates
        episode_dates = self._gather_episode_dates(series_path_obj, imdb_id, season_episodes)
        
        # Process episodes
        processed_count = 0
        for (season, episode), (aired, dateadded, source) in episode_dates.items():
            if (season, episode) in season_episodes:
                # Create NFO
                if config.manage_nfo:
                    self.nfo_manager.create_episode_nfo(
                        season_path,
                        season, episode, aired, dateadded, source, config.lock_metadata
                    )
                
                # Update file mtimes
                if config.fix_dir_mtimes and dateadded:
                    video_files = season_episodes[(season, episode)]
                    for video_file in video_files:
                        self.nfo_manager.set_file_mtime(video_file, dateadded)
                
                # Save to database
                try:
                    self.db.upsert_episode_date(imdb_id, season, episode, aired, dateadded, source, True)
                    _log("DEBUG", f"S{season:02d}E{episode:02d}: Database record saved successfully")
                except Exception as e:
                    _log("ERROR", f"S{season:02d}E{episode:02d}: Database write failed: {e}")
                    # Continue processing other episodes
                processed_count += 1
        
        _log("INFO", f"Processed {processed_count} episodes in season {season_num}")
        
        return {
            "status": "success",
            "season": season_num,
            "episodes_found": len(season_episodes),
            "episodes_processed": processed_count
        }
    
    def process_episode(self, series_path: str, season_name: str, episode_name: str) -> Dict[str, Any]:
        """Process a specific episode"""
        series_path_obj = Path(series_path)
        if not series_path_obj.exists():
            raise FileNotFoundError(f"Series path not found: {series_path}")
        
        season_path = series_path_obj / season_name
        if not season_path.exists():
            raise FileNotFoundError(f"Season directory not found: {season_path}")
        
        episode_path = season_path / episode_name
        if not episode_path.exists():
            raise FileNotFoundError(f"Episode file not found: {episode_path}")
        
        # Extract season and episode numbers
        season_match = re.search(r'(\d+)', season_name)
        episode_match = re.search(r'[sS](\d+)[eE](\d+)|(\d+)x(\d+)', episode_name)
        
        if not season_match:
            raise ValueError(f"Could not extract season number from: {season_name}")
        
        if not episode_match:
            raise ValueError(f"Could not extract episode number from: {episode_name}")
        
        season_num = int(season_match.group(1))
        
        if episode_match.group(1) and episode_match.group(2):  # SxxExx format
            episode_num = int(episode_match.group(2))
        elif episode_match.group(3) and episode_match.group(4):  # NxNN format
            episode_num = int(episode_match.group(4))
        else:
            raise ValueError(f"Could not parse episode number from: {episode_name}")
        
        # Get series IMDb ID
        imdb_id = self.nfo_manager.parse_imdb_from_path(series_path_obj)
        if not imdb_id:
            raise ValueError(f"No IMDb ID found in series path: {series_path}")
        
        _log("INFO", f"Processing episode S{season_num:02d}E{episode_num:02d} of series: {series_path_obj.name}")
        
        # Get episode data
        disk_episodes = {(season_num, episode_num): [episode_path]}
        episode_dates = self._gather_episode_dates(series_path_obj, imdb_id, disk_episodes)
        
        if (season_num, episode_num) not in episode_dates:
            return {"status": "no_data", "season": season_num, "episode": episode_num}
        
        aired, dateadded, source = episode_dates[(season_num, episode_num)]
        
        # Create NFO
        if config.manage_nfo:
            self.nfo_manager.create_episode_nfo(
                season_path,
                season_num, episode_num, aired, dateadded, source, config.lock_metadata
            )
        
        # Update file mtime
        if config.fix_dir_mtimes and dateadded:
            self.nfo_manager.set_file_mtime(episode_path, dateadded)
        
        # Save to database
        self.db.upsert_episode_date(imdb_id, season_num, episode_num, aired, dateadded, source, True)
        
        _log("INFO", f"Processed episode S{season_num:02d}E{episode_num:02d}")
        
        return {
            "status": "success",
            "season": season_num,
            "episode": episode_num,
            "aired": aired,
            "dateadded": dateadded,
            "source": source
        }
    
    # ===== ASYNC METHODS =====
    
    async def async_process_series(self, series_path: Path) -> Dict[str, Any]:
        """
        Async process a TV series directory with concurrent episode processing
        
        Args:
            series_path: Path to series directory
            
        Returns:
            Dictionary with processing results
        """
        imdb_id = self.nfo_manager.parse_imdb_from_path(series_path)
        if not imdb_id:
            return {"status": "error", "reason": f"No IMDb ID found in series path: {series_path}"}
        
        _log("INFO", f"Async processing TV series: {series_path.name}")
        
        # Update database
        self.db.upsert_series(imdb_id, str(series_path))
        
        # Find video files asynchronously
        disk_episodes = await async_find_episodes_on_disk(series_path)
        _log("INFO", f"Found {len(disk_episodes)} episodes on disk")
        
        # Get episode dates (sync for now, could be made async later)
        episode_dates = self._gather_episode_dates(series_path, imdb_id, disk_episodes)
        
        # Prepare episode data for concurrent processing
        episode_data_list = []
        mtime_operations = []
        
        for (season, episode), (aired, dateadded, source) in episode_dates.items():
            if (season, episode) in disk_episodes:
                season_dir = series_path / config.tv_season_dir_format.format(season=season)
                
                # Prepare NFO creation data
                if config.manage_nfo:
                    episode_data_list.append({
                        'season_dir': season_dir,
                        'season': season,
                        'episode': episode,
                        'aired': aired,
                        'dateadded': dateadded,
                        'source': source,
                        'lock_metadata': config.lock_metadata
                    })
                
                # Prepare mtime operations
                if config.fix_dir_mtimes and dateadded:
                    video_files = disk_episodes[(season, episode)]
                    for video_file in video_files:
                        mtime_operations.append((video_file, dateadded))
                
                # Save to database
                try:
                    self.db.upsert_episode_date(imdb_id, season, episode, aired, dateadded, source, True)
                    _log("DEBUG", f"S{season:02d}E{episode:02d}: Database record saved successfully")
                except Exception as e:
                    _log("ERROR", f"S{season:02d}E{episode:02d}: Database write failed: {e}")
                    # Continue processing other episodes
        
        # Process NFOs and mtimes concurrently
        results = {}
        
        if episode_data_list:
            _log("INFO", f"Creating {len(episode_data_list)} episode NFOs concurrently")
            nfo_results = await self.async_nfo_manager.async_batch_create_episode_nfos(
                episode_data_list,
                max_concurrent=config.max_concurrent
            )
            results['nfo_created'] = sum(nfo_results)
            results['nfo_failed'] = len(nfo_results) - sum(nfo_results)
        
        if mtime_operations:
            _log("INFO", f"Setting mtimes for {len(mtime_operations)} files concurrently")
            mtime_results = await self.async_nfo_manager.async_batch_set_file_mtimes(
                mtime_operations,
                max_concurrent=10
            )
            results['mtime_updated'] = sum(mtime_results)
            results['mtime_failed'] = len(mtime_results) - sum(mtime_results)
        
        _log("INFO", f"Completed async processing TV series: {series_path.name}")
        
        return {
            "status": "success",
            "imdb_id": imdb_id,
            "episodes_found": len(disk_episodes),
            "episodes_processed": len(episode_data_list),
            "results": results
        }
    
    async def async_process_multiple_series(
        self,
        series_paths: List[Path],
        max_concurrent: int = 2
    ) -> List[Dict[str, Any]]:
        """
        Process multiple TV series concurrently
        
        Args:
            series_paths: List of series directory paths
            max_concurrent: Maximum concurrent series processing
            
        Returns:
            List of processing results for each series
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def _process_series_with_semaphore(series_path: Path) -> Dict[str, Any]:
            async with semaphore:
                try:
                    return await self.async_process_series(series_path)
                except Exception as e:
                    _log("ERROR", f"Failed to process series {series_path}: {e}")
                    return {"status": "error", "path": str(series_path), "reason": str(e)}
        
        _log("INFO", f"Processing {len(series_paths)} series with max {max_concurrent} concurrent")
        
        tasks = [_process_series_with_semaphore(path) for path in series_paths]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and convert to proper results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "status": "error",
                    "path": str(series_paths[i]),
                    "reason": str(result)
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
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
            
            series_data = self.sonarr.series_by_imdb(imdb_id)
            if not series_data:
                return None
            
            series_id = series_data.get('id')
            if not series_id:
                return None
            
            # Get all episodes for this series
            episodes = self.sonarr.episodes_for_series(series_id)
            
            # Organize episodes by season/episode
            episode_map = {}
            for episode in episodes:
                season = episode.get('seasonNumber', 0)
                episode_num = episode.get('episodeNumber', 0)
                
                if season >= 0 and episode_num > 0:
                    episode_map[(season, episode_num)] = episode
            
            return {
                'series': series_data,
                'episodes': episode_map
            }
            
        except Exception as e:
            _log("ERROR", f"Failed to get Sonarr series metadata for {imdb_id}: {e}")
            return None
    
    def _get_episode_metadata(self, series_metadata: Optional[Dict[str, Any]], season_num: int, episode_num: int, season_dir: Optional[Path] = None) -> Optional[Dict[str, Any]]:
        """Get enhanced episode metadata including title extraction from filename"""
        _log("DEBUG", f"Getting episode metadata for S{season_num:02d}E{episode_num:02d}, season_dir: {season_dir}")
        
        metadata = {}
        
        # Try to get title from Sonarr first
        if series_metadata and 'episodes' in series_metadata:
            episode_data = series_metadata['episodes'].get((season_num, episode_num))
            if episode_data:
                title = episode_data.get('title')
                if title and title != 'TBA':
                    metadata['title'] = title
                    _log("DEBUG", f"Got title from Sonarr for S{season_num:02d}E{episode_num:02d}: {title}")
        
        # If no title from Sonarr, try to extract from filename
        if 'title' not in metadata and season_dir:
            title = self._extract_title_from_filename(season_num, episode_num, season_dir)
            if title:
                metadata['title'] = title
                _log("DEBUG", f"Extracted title from filename for S{season_num:02d}E{episode_num:02d}: {title}")
        
        return metadata if metadata else None
    
    def _extract_title_from_filename(self, season_num: int, episode_num: int, season_dir: Path) -> Optional[str]:
        """Extract episode title from video filename using regex pattern"""
        season_pattern = f"S{season_num:02d}E{episode_num:02d}"
        
        try:
            # Look for video files in the season directory
            for file_path in season_dir.iterdir():
                if file_path.is_file() and file_path.suffix.lower() in ('.mkv', '.mp4', '.avi', '.mov', '.m4v'):
                    filename = file_path.name
                    
                    # Check if this file matches our season/episode
                    if season_pattern in filename.upper():
                        # Extract title using regex pattern: S01E01-Title[WEBDL-1080p]
                        match = re.search(rf'{season_pattern}-(.*?)\[', filename, re.IGNORECASE)
                        if match:
                            title = match.group(1)
                            # Clean up the title
                            title = title.replace('-', ' ').strip()
                            if title:
                                _log("DEBUG", f"Extracted title '{title}' from filename: {filename}")
                                return title
                            
        except Exception as e:
            _log("ERROR", f"Error extracting title from filename for S{season_num:02d}E{episode_num:02d}: {e}")
        
        return None
    
    def _get_webhook_episode_date(self, imdb_id: str, season_num: int, episode_num: int, series_metadata: Optional[Dict[str, Any]] = None) -> Tuple[Optional[str], Optional[str], str]:
        """
        Get episode date for webhook processing - avoid treating webhook as gospel.
        
        Logic:
        1. Check Sonarr import history FIRST (any history = show has existed for a while)
        2. If ANY import history exists → Use air date (webhook is likely upgrade/rename)
        3. If NO import history → Use webhook date (truly new show)
        
        This prevents upgrades from overriding dates for shows you've had for months/years.
        """
        # Get aired date and episode ID from Sonarr
        aired = None
        episode_id = None
        if series_metadata and 'episodes' in series_metadata:
            episode_data = series_metadata['episodes'].get((season_num, episode_num))
            if episode_data:
                aired = episode_data.get('airDate')
                episode_id = episode_data.get('id')
                _log("DEBUG", f"Got aired date from Sonarr for S{season_num:02d}E{episode_num:02d}: {aired}")
        
        # STEP 1: Check Sonarr import history FIRST (this is the key check)
        _log("INFO", f"Checking Sonarr import history for S{season_num:02d}E{episode_num:02d} to detect import vs rename events")
        
        if episode_id and hasattr(self, 'sonarr'):
            try:
                _log("DEBUG", f"Calling get_episode_import_history for episode_id: {episode_id}")
                import_history = self.sonarr.get_episode_import_history(episode_id)
                _log("DEBUG", f"Import history result: {import_history}")
                
                if import_history:
                    # Found actual import event - use this date
                    _log("INFO", f"Found real import event for S{season_num:02d}E{episode_num:02d}: {import_history}")
                    _log("INFO", f"Using first import date (not webhook): {import_history}")
                    return aired, import_history, "sonarr:import_history"
                else:
                    # No import events found - this means only renames/moves exist in history
                    # The episode was already in Sonarr, just being managed/renamed
                    _log("INFO", f"No import events found for S{season_num:02d}E{episode_num:02d} - only renames/moves in history")
                    
                    if aired:
                        _log("INFO", f"Using air date for existing episode (rename-only history): {aired}")
                        return aired, aired + "T20:00:00", "airdate"
                    else:
                        _log("DEBUG", f"No air date available for rename-only episode S{season_num:02d}E{episode_num:02d}")
                    
            except Exception as e:
                _log("ERROR", f"Error checking Sonarr import history for S{season_num:02d}E{episode_num:02d}: {e}")
                import traceback
                _log("ERROR", traceback.format_exc())
        else:
            if not episode_id:
                _log("DEBUG", f"No episode_id found for S{season_num:02d}E{episode_num:02d}")
            if not hasattr(self, 'sonarr'):
                _log("DEBUG", f"No sonarr client available")
        
        # STEP 2: No import history found - this is likely a genuinely new show
        # Check our database to avoid duplicates
        existing = self.db.get_episode_date(imdb_id, season_num, episode_num)
        if existing and existing.get('dateadded'):
            _log("INFO", f"Episode S{season_num:02d}E{episode_num:02d} already exists in our database: {existing['dateadded']}")
            return existing.get('aired'), existing.get('dateadded'), existing.get('source', 'nfoguard:database')
        
        # STEP 3: Truly new episode - use webhook date
        dateadded = datetime.now().isoformat()
        source = "sonarr:webhook"
        
        _log("INFO", f"No import history and not in database - using webhook date for genuinely new episode S{season_num:02d}E{episode_num:02d}: {dateadded}")
        
        return aired, dateadded, source
    
    async def async_batch_episode_processing(
        self,
        episodes_data: List[Dict[str, Any]],
        max_concurrent: int = 5
    ) -> Dict[str, Any]:
        """
        Process episodes from webhook data concurrently
        
        Args:
            episodes_data: List of episode data from webhooks
            max_concurrent: Maximum concurrent episode processing
            
        Returns:
            Processing results summary
        """
        async def _process_single_episode(episode_data: Dict[str, Any]) -> Dict[str, Any]:
            try:
                # Extract episode information
                series_path = Path(episode_data.get('series_path'))
                season = episode_data.get('season')
                episode = episode_data.get('episode')
                aired = episode_data.get('aired')
                dateadded = episode_data.get('dateadded')
                
                # Get IMDb ID
                imdb_id = self.nfo_manager.parse_imdb_from_path(series_path)
                if not imdb_id:
                    return {"status": "error", "reason": "No IMDb ID found"}
                
                # Create NFO if needed
                nfo_success = True
                if config.manage_nfo:
                    season_dir = series_path / config.tv_season_dir_format.format(season=season)
                    nfo_success = await self.async_nfo_manager.async_create_episode_nfo(
                        season_dir, season, episode, aired, dateadded, "webhook", config.lock_metadata
                    )
                
                # Update database
                self.db.upsert_episode_date(imdb_id, season, episode, aired, dateadded, "webhook", True)
                
                return {
                    "status": "success",
                    "season": season,
                    "episode": episode,
                    "nfo_created": nfo_success
                }
                
            except Exception as e:
                return {
                    "status": "error",
                    "episode_data": episode_data,
                    "reason": str(e)
                }
        
        _log("INFO", f"Processing {len(episodes_data)} episodes concurrently")
        
        results = await async_concurrent_episode_processing(
            episodes_data,
            _process_single_episode,
            max_concurrent
        )
        
        # Summarize results
        successful = sum(1 for r in results if r and r.get("status") == "success")
        failed = len(results) - successful
        
        return {
            "total_episodes": len(episodes_data),
            "successful": successful,
            "failed": failed,
            "results": results
        }