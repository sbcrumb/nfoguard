"""
FastAPI routes for NFOGuard - extracted from main nfoguard.py for modular architecture
"""
import os
import json
import requests
import asyncio
from pathlib import Path
from datetime import datetime, timezone
from fastapi import HTTPException, BackgroundTasks, Request
from typing import Optional

# Import models
from api.models import (
    SonarrWebhook, RadarrWebhook, HealthResponse, TVSeasonRequest, TVEpisodeRequest,
    MovieUpdateRequest, EpisodeUpdateRequest, BulkUpdateRequest
)
from api.web_routes import (
    get_movies_list, get_tv_series_list, get_series_episodes, get_series_sources, get_missing_dates_report,
    get_dashboard_stats, update_movie_date, update_episode_date, bulk_update_source,
    get_movie_date_options, get_episode_date_options, debug_series_date_distribution
)


# ---------------------------
# Helper Functions
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
        print(f"ERROR: Failed to read webhook payload: {e}")  # Using print since _log is not available
        return {}


# ---------------------------
# Route Handlers
# ---------------------------

async def sonarr_webhook(request: Request, background_tasks: BackgroundTasks, dependencies: dict):
    """Handle Sonarr webhooks"""
    tv_processor = dependencies["tv_processor"]
    batcher = dependencies["batcher"]
    config = dependencies["config"]
    
    try:
        payload = await _read_payload(request)
        if not payload:
            raise HTTPException(status_code=422, detail="Empty Sonarr payload")
        
        webhook = SonarrWebhook(**payload)
        print(f"INFO: Received Sonarr webhook: {webhook.eventType}")
        
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
            print(f"ERROR: No IMDb ID for series: {series_title}")
            return {"status": "error", "reason": "No IMDb ID"}
        
        # Find series path
        series_path = tv_processor.find_series_path(series_title, imdb_id, sonarr_path)
        if not series_path:
            print(f"ERROR: Could not find series directory: {series_title} ({imdb_id})")
            return {"status": "error", "reason": "Series directory not found"}
        
        # Extract episode data for targeted processing
        episodes_data = webhook.episodes or []
        print(f"DEBUG: Initial episodes_data from webhook.episodes: {len(episodes_data)} episodes")
        
        # For all webhook events, if no episodes in webhook.episodes, try to extract from episodeFile
        # This ensures targeted processing for single episode operations (Download, Rename, Upgrade)
        print(f"DEBUG: webhook.episodeFile present: {webhook.episodeFile is not None}")
        if webhook.episodeFile:
            print(f"DEBUG: episodeFile content: {webhook.episodeFile}")
        
        if not episodes_data and webhook.episodeFile:
            episode_file = webhook.episodeFile
            # Extract season and episode from episodeFile if available
            season_num = episode_file.get("seasonNumber")
            episode_num = episode_file.get("episodeNumber") 
            print(f"DEBUG: episodeFile seasonNumber: {season_num}, episodeNumber: {episode_num}")
            if season_num and episode_num:
                # Create episode data structure that matches what process_webhook_episodes expects
                episodes_data = [{
                    "seasonNumber": season_num,
                    "episodeNumber": episode_num,
                    "id": episode_file.get("id"),
                    "title": episode_file.get("title")
                }]
                print(f"INFO: Extracted episode info from episodeFile for {webhook.eventType}: S{season_num:02d}E{episode_num:02d}")
            else:
                print(f"DEBUG: Missing season/episode numbers in episodeFile for {webhook.eventType}")
        
        # Special handling for Rename events - Sonarr doesn't include episodeFile for renames
        # Try to find recently renamed episodes using Sonarr history API
        if not episodes_data and webhook.eventType == "Rename":
            print(f"DEBUG: Attempting to find recently renamed episode for series {imdb_id}")
            try:
                # Get series info from Sonarr to find series ID
                series_lookup_url = f"{config.sonarr_url}/api/v3/series/lookup?term=imdbid:{imdb_id}"
                print(f"DEBUG: Sonarr lookup for rename: {series_lookup_url}")
                
                response = requests.get(series_lookup_url, headers={"X-Api-Key": os.environ.get("SONARR_API_KEY", "")}, timeout=10)
                if response.status_code == 200:
                    series_results = response.json()
                    if series_results:
                        series_id = series_results[0].get("id")
                        print(f"DEBUG: Found series ID {series_id} for rename lookup")
                        
                        # Get recent history for the series and filter for rename events
                        from datetime import datetime, timedelta
                        since_date = (datetime.utcnow() - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
                        history_url = f"{config.sonarr_url}/api/v3/history?seriesId={series_id}&sortKey=date&sortDir=desc&page=1&pageSize=50"
                        print(f"DEBUG: Checking recent rename history: {history_url}")
                        
                        history_response = requests.get(history_url, headers={"X-Api-Key": os.environ.get("SONARR_API_KEY", "")}, timeout=10)
                        if history_response.status_code == 200:
                            history_data = history_response.json()
                            all_records = history_data.get("records", [])
                            print(f"DEBUG: Got {len(all_records)} total history records")
                            
                            # Filter for recent rename events
                            since_timestamp = datetime.utcnow() - timedelta(hours=1)
                            recent_renames = []
                            
                            for record in all_records:
                                event_type = record.get("eventType", "")
                                date_str = record.get("date", "")
                                
                                if event_type == "episodeFileRenamed" and date_str:
                                    try:
                                        event_time = datetime.strptime(date_str.replace('Z', '+00:00'), '%Y-%m-%dT%H:%M:%S.%f%z')
                                        event_time_utc = event_time.utctimetuple()
                                        if datetime(*event_time_utc[:6]) > since_timestamp:
                                            recent_renames.append(record)
                                    except:
                                        # If datetime parsing fails, include it anyway
                                        recent_renames.append(record)
                            
                            print(f"DEBUG: Found {len(recent_renames)} recent rename events")
                            
                            if recent_renames:
                                # Take the most recent rename event
                                latest_rename = recent_renames[0]
                                print(f"DEBUG: Processing latest rename event")
                                
                                # Extract episodeId directly from the rename event
                                episode_id = latest_rename.get("episodeId")
                                print(f"DEBUG: Found episodeId {episode_id} in rename event")
                                
                                if episode_id:
                                    # Fetch episode details using the episodeId
                                    episode_detail_url = f"{config.sonarr_url}/api/v3/episode/{episode_id}"
                                    episode_response = requests.get(episode_detail_url, headers={"X-Api-Key": os.environ.get("SONARR_API_KEY", "")}, timeout=10)
                                    
                                    if episode_response.status_code == 200:
                                        episode_detail = episode_response.json()
                                        season_num = episode_detail.get("seasonNumber")
                                        episode_num = episode_detail.get("episodeNumber")
                                        episode_title = episode_detail.get("title")
                                        
                                        print(f"DEBUG: Episode details - Season: {season_num}, Episode: {episode_num}, Title: {episode_title}")
                                        
                                        if season_num is not None and episode_num is not None:
                                            episodes_data = [{
                                                "seasonNumber": season_num,
                                                "episodeNumber": episode_num,
                                                "id": episode_id,
                                                "title": episode_title
                                            }]
                                            print(f"INFO: Successfully identified renamed episode: S{season_num:02d}E{episode_num:02d} - {episode_title}")
                                        else:
                                            print(f"DEBUG: Episode details missing season/episode numbers")
                                    else:
                                        print(f"DEBUG: Failed to fetch episode details: {episode_response.status_code}")
                                else:
                                    print(f"DEBUG: No episodeId found in rename event")
                            else:
                                print(f"DEBUG: No recent rename events found in last hour")
                        else:
                            print(f"DEBUG: Failed to get rename history: {history_response.status_code}")
                    else:
                        print(f"DEBUG: No series found for IMDb {imdb_id}")
                else:
                    print(f"DEBUG: Series lookup failed: {response.status_code}")
            except Exception as e:
                print(f"DEBUG: Error finding renamed episode: {e}")
                # Continue with series processing as fallback
        
        # Force targeted mode for single-episode webhooks to prevent full series processing
        processing_mode = config.tv_webhook_processing_mode
        if episodes_data and len(episodes_data) <= 3:  # Single episode or small batch
            processing_mode = "targeted"
            print(f"INFO: Forcing targeted mode for {len(episodes_data)} episode(s)")
        
        # Add to batch queue with TV-prefixed key to avoid movie conflicts
        tv_batch_key = f"tv:{imdb_id}"
        webhook_dict = {
            'path': str(series_path),
            'series_info': series_info,
            'event_type': webhook.eventType,
            'episodes': episodes_data,  # Include enhanced episode data for targeted processing
            'processing_mode': processing_mode  # Use forced targeted mode when appropriate
        }
        batcher.add_webhook(tv_batch_key, webhook_dict, 'tv')
        
        return {"status": "accepted", "message": f"Sonarr webhook queued for {tv_batch_key}"}
        
    except Exception as e:
        print(f"ERROR: Sonarr webhook error: {e}")
        raise HTTPException(status_code=422, detail=f"Invalid webhook: {e}")


async def radarr_webhook(request: Request, background_tasks: BackgroundTasks, dependencies: dict):
    """Handle Radarr webhooks"""
    path_mapper = dependencies["path_mapper"]
    batcher = dependencies["batcher"]
    
    try:
        payload = await _read_payload(request)
        print(f"INFO: Received Radarr webhook: {payload.get('eventType', 'Unknown')}")
        print(f"DEBUG: Full Radarr webhook payload: {payload}")
        
        # Filter supported event types (same as Sonarr: Download, Upgrade, Rename)
        event_type = payload.get('eventType', '')
        if event_type not in ["Download", "Upgrade", "Rename"]:
            return {"status": "ignored", "reason": f"Event type {event_type} not processed"}
        
        # Extract movie info
        movie_data = payload.get("movie", {})
        if not movie_data:
            print("WARNING: No movie data in Radarr webhook")
            return {"status": "error", "message": "No movie data"}
        
        # Get IMDb ID for batching key
        imdb_id = movie_data.get("imdbId", "").lower()
        if not imdb_id:
            print("WARNING: No IMDb ID in Radarr webhook movie data")
            return {"status": "error", "message": "No IMDb ID"}
        
        # Get movie path and map it
        movie_path = movie_data.get("folderPath") or movie_data.get("path", "")
        if not movie_path:
            print("ERROR: No movie path in Radarr webhook")
            return {"status": "error", "message": "No movie path provided"}
        
        # Map the path to container path
        container_path = path_mapper.radarr_path_to_container_path(movie_path)
        print(f"DEBUG: Mapped Radarr path {movie_path} -> {container_path}")
        
        # CRITICAL: Verify the mapped path actually exists
        if not Path(container_path).exists():
            print(f"ERROR: RADARR WEBHOOK REJECTED: Mapped path does not exist: {container_path}")
            print(f"ERROR: This prevents processing wrong movies due to path mapping issues")
            return {"status": "error", "message": f"Mapped movie path does not exist: {container_path}"}
        
        # Verify the path contains the expected IMDb ID
        if imdb_id not in container_path.lower():
            print(f"WARNING: IMDb ID {imdb_id} not found in container path {container_path}")
        
        # Create movie-specific webhook data with proper path validation
        movie_webhook_data = {
            'path': container_path,  # Use verified container path
            'movie_info': movie_data,
            'event_type': payload.get('eventType'),
            'original_payload': payload
        }
        
        # Add to batch queue with movie-prefixed key to avoid TV conflicts
        movie_batch_key = f"movie:{imdb_id}"
        print(f"DEBUG: Adding Radarr webhook to batch: key={movie_batch_key}, movie_title={movie_data.get('title', 'Unknown')}")
        batcher.add_webhook(movie_batch_key, movie_webhook_data, "movie")
        
        return {"status": "success", "message": f"Radarr webhook queued for {movie_batch_key}"}
        
    except Exception as e:
        print(f"ERROR: Radarr webhook error: {e}")
        return {"status": "error", "message": str(e)}


async def health(dependencies: dict) -> HealthResponse:
    """Health check endpoint with Radarr database status"""
    db = dependencies["db"]
    movie_processor = dependencies["movie_processor"]
    start_time = dependencies["start_time"]
    version = dependencies["version"]
    
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
        print(f"DEBUG: Skipping Radarr database health check: {e}")
    
    return HealthResponse(
        status=overall_status,
        version=version,
        uptime=str(uptime),
        database_status=db_status,
        radarr_database=radarr_db_health
    )


async def get_stats(dependencies: dict):
    """Get database statistics"""
    db = dependencies["db"]
    try:
        return db.get_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def batch_status(dependencies: dict):
    """Get batch queue status"""
    batcher = dependencies["batcher"]
    return batcher.get_status()


async def debug_movie_import_date(imdb_id: str, dependencies: dict):
    """Debug endpoint to analyze movie import date detection"""
    movie_processor = dependencies["movie_processor"]
    
    try:
        if not imdb_id.startswith("tt"):
            imdb_id = f"tt{imdb_id}"
            
        print(f"INFO: === DEBUG MOVIE IMPORT DATE: {imdb_id} ===")
        
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
        
        print(f"INFO: Found movie: {movie_title} (Radarr ID: {movie_id})")
        
        # Test the FULL movie processing pipeline (not just database lookup)
        print(f"INFO: === TESTING FULL MOVIE PROCESSING PIPELINE ===")
        
        # Create a dummy path for testing the decision logic
        dummy_path = Path("/tmp/test")
        
        try:
            # Use the global movie processor instance to test full decision logic
            if movie_processor:
                # First check external clients configuration
                print(f"INFO: === CHECKING EXTERNAL CLIENTS CONFIG ===")
                try:
                    tmdb_key = os.environ.get("TMDB_API_KEY", "")
                    print(f"INFO: TMDB API Key configured: {'✅ YES' if tmdb_key else '❌ NO'}")
                    if tmdb_key:
                        print(f"INFO: TMDB API Key length: {len(tmdb_key)} chars")
                    
                    # Check if external clients exist
                    external_clients_available = hasattr(movie_processor, 'external_clients') and movie_processor.external_clients
                    print(f"INFO: External clients initialized: {'✅ YES' if external_clients_available else '❌ NO'}")
                    
                except Exception as e:
                    print(f"ERROR: Error checking external clients config: {e}")
                
                # Test the full decision logic (including TMDB fallback)
                final_date, final_source, released = movie_processor._decide_movie_dates(
                    imdb_id, dummy_path, should_query=True, existing=None
                )
                
                print(f"INFO: === FULL PIPELINE RESULT ===")
                print(f"INFO: Final date: {final_date}")
                print(f"INFO: Final source: {final_source}")
                print(f"INFO: Released (theater): {released}")
                
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
                print("ERROR: Movie processor not available - testing database only")
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
            print(f"ERROR: Full pipeline test failed: {pipeline_error}")
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
        print(f"ERROR: Debug endpoint error for {imdb_id}: {e}")
        return {
            "error": str(e),
            "imdb_id": imdb_id,
            "success": False
        }


async def debug_movie_history(imdb_id: str, dependencies: dict):
    """Detailed history analysis for a movie"""
    movie_processor = dependencies["movie_processor"]
    
    try:
        if not imdb_id.startswith("tt"):
            imdb_id = f"tt{imdb_id}"
            
        print(f"INFO: === DETAILED HISTORY ANALYSIS: {imdb_id} ===")
        
        # This would need the rest of the implementation from the original function
        # For now, returning a placeholder
        return {
            "imdb_id": imdb_id,
            "message": "History analysis endpoint - implementation needed"
        }
        
    except Exception as e:
        print(f"ERROR: Debug history endpoint error for {imdb_id}: {e}")
        return {
            "error": str(e),
            "imdb_id": imdb_id,
            "success": False
        }


async def manual_scan(background_tasks: BackgroundTasks, path: Optional[str] = None, scan_type: str = "both", scan_mode: str = "smart", dependencies: dict = None):
    """Manual scan endpoint with smart optimization modes"""
    config = dependencies["config"]
    nfo_manager = dependencies["nfo_manager"]
    tv_processor = dependencies["tv_processor"]
    movie_processor = dependencies["movie_processor"]
    
    if scan_type not in ["both", "tv", "movies"]:
        raise HTTPException(status_code=400, detail="scan_type must be 'both', 'tv', or 'movies'")
    
    if scan_mode not in ["smart", "full", "incomplete"]:
        raise HTTPException(status_code=400, detail="scan_mode must be 'smart', 'full', or 'incomplete'")
    
    async def run_scan():
        from datetime import datetime, timezone
        import time
        import os
        start_time = datetime.now()
        
        # Handle timezone display - check if TZ is set in container
        try:
            tz_name = os.environ.get('TZ')
            if tz_name:
                # TZ is set, so datetime.now() already returns local time
                local_start = start_time
                tz_display = f" ({tz_name})"
            else:
                # No TZ set, assume container is UTC and convert to Eastern
                import zoneinfo
                local_tz = zoneinfo.ZoneInfo("America/New_York")
                local_start = start_time.replace(tzinfo=timezone.utc).astimezone(local_tz)
                tz_display = " (EDT/EST)"
        except:
            # Ultimate fallback - just show time as-is with note
            local_start = start_time
            tz_display = " (container time)"
            
        print(f"🚀 MANUAL SCAN STARTED: {scan_type} scan (mode: {scan_mode}) initiated at {local_start.strftime('%Y-%m-%d %H:%M:%S')}{tz_display}")
        
        # Initialize counters for scan statistics
        tv_series_total = 0
        tv_series_skipped = 0
        tv_series_processed = 0
        movie_total = 0
        movie_skipped = 0
        movie_processed = 0
        
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
                        print(f"INFO: Processing single season: {scan_path}")
                        try:
                            tv_processor.process_season(series_path, scan_path)
                        except Exception as e:
                            print(f"ERROR: Failed processing season {scan_path}: {e}")
                elif path and scan_path.is_file() and scan_path.suffix.lower() in ('.mkv', '.mp4', '.avi'):
                    # Single episode processing
                    season_path = scan_path.parent
                    series_path = season_path.parent
                    if nfo_manager.parse_imdb_from_path(series_path):
                        print(f"INFO: Processing single episode: {scan_path}")
                        try:
                            tv_processor.process_episode_file(series_path, season_path, scan_path)
                        except Exception as e:
                            print(f"ERROR: Failed processing episode {scan_path}: {e}")
                else:
                    # Check if this path itself is a series (has IMDb ID in the directory name)
                    if nfo_manager.parse_imdb_from_path(scan_path):
                        try:
                            # Determine force_scan based on scan mode
                            force_scan = (scan_mode == "full")
                            result = tv_processor.process_series(scan_path, force_scan=force_scan)
                            tv_series_total += 1
                            if result == "skipped":
                                tv_series_skipped += 1
                            elif result == "processed":
                                tv_series_processed += 1
                        except Exception as e:
                            print(f"ERROR: Failed processing TV series {scan_path}: {e}")
                            tv_series_total += 1
                    else:
                        # Full series processing - scan subdirectories
                        import re
                        tv_count = 0
                        for item in scan_path.iterdir():
                            # Check for shutdown signal at start of each item
                            shutdown_event = dependencies.get("shutdown_event")
                            if shutdown_event and shutdown_event.is_set():
                                print("INFO: ⚠️ SHUTDOWN SIGNAL RECEIVED - Stopping scan gracefully")
                                return
                                
                            if (item.is_dir() and 
                                not item.name.lower().startswith('season') and
                                not re.match(r'^season\s+\d+$', item.name, re.IGNORECASE) and
                                nfo_manager.parse_imdb_from_path(item)):
                                tv_count += 1
                                try:
                                    # Determine force_scan based on scan mode
                                    force_scan = (scan_mode == "full")
                                    result = tv_processor.process_series(item, force_scan=force_scan)
                                    tv_series_total += 1
                                    if result == "skipped":
                                        tv_series_skipped += 1
                                    elif result == "processed":
                                        tv_series_processed += 1
                                except Exception as e:
                                    print(f"ERROR: Failed processing TV series {item}: {e}")
                                    tv_series_total += 1
                                
                                # Yield control every TV series to allow other requests  
                                if tv_count % 1 == 0:
                                    await asyncio.sleep(0.2)  # 200ms yield to process other requests
                                    print(f"INFO: Processed {tv_count} TV series, yielding to other requests...")
                                    
                                    # Check for shutdown signal
                                    shutdown_event = dependencies.get("shutdown_event")
                                    if shutdown_event and shutdown_event.is_set():
                                        print("INFO: ⚠️ SHUTDOWN SIGNAL RECEIVED - Stopping scan gracefully")
                                        return
            
            if scan_type in ["both", "movies"] and scan_path in config.movie_paths:
                print(f"INFO: Scanning movies in: {scan_path}")
                movie_count = 0
                for item in scan_path.iterdir():
                    # Check for shutdown signal at start of each movie
                    shutdown_event = dependencies.get("shutdown_event")
                    if shutdown_event and shutdown_event.is_set():
                        print("INFO: ⚠️ SHUTDOWN SIGNAL RECEIVED - Stopping scan gracefully")
                        return
                        
                    if item.is_dir() and nfo_manager.find_movie_imdb_id(item):
                        movie_count += 1
                        print(f"INFO: Processing movie: {item.name}")
                        try:
                            # Determine force_scan based on scan mode
                            force_scan = (scan_mode == "full")
                            shutdown_event = dependencies.get("shutdown_event")
                            result = movie_processor.process_movie(item, webhook_mode=False, force_scan=force_scan, shutdown_event=shutdown_event)
                            movie_total += 1
                            if result == "skipped":
                                movie_skipped += 1
                            elif result == "processed":
                                movie_processed += 1
                            elif result == "no_video_files":
                                print(f"INFO: Skipped empty directory: {item.name}")
                                movie_skipped += 1
                            elif result == "shutdown":
                                print("INFO: ⚠️ SHUTDOWN SIGNAL RECEIVED - Stopping movie scan gracefully")
                                return
                        except Exception as e:
                            print(f"ERROR: Failed processing movie {item}: {e}")
                            movie_total += 1
                        
                        # Yield control every 2 movies to allow other requests (webhooks, web interface)
                        if movie_count % 2 == 0:
                            await asyncio.sleep(0.2)  # 200ms yield to process other requests
                            print(f"INFO: Processed {movie_count} movies, yielding to other requests...")
                            
                            # Check for shutdown signal
                            shutdown_event = dependencies.get("shutdown_event")
                            if shutdown_event and shutdown_event.is_set():
                                print("INFO: ⚠️ SHUTDOWN SIGNAL RECEIVED - Stopping scan gracefully")
                                return
                        
                print(f"INFO: Completed movie scan: {movie_count} movies processed in {scan_path}")
        
        # Log scan completion with duration
        end_time = datetime.now()
        duration = end_time - start_time
        duration_str = str(duration).split('.')[0]  # Remove microseconds
        
        # Use same timezone logic as start
        try:
            tz_name = os.environ.get('TZ')
            if tz_name:
                # TZ is set, so datetime.now() already returns local time
                local_end = end_time
                tz_display = f" ({tz_name})"
            else:
                # No TZ set, assume container is UTC and convert to Eastern
                import zoneinfo
                local_tz = zoneinfo.ZoneInfo("America/New_York")
                local_end = end_time.replace(tzinfo=timezone.utc).astimezone(local_tz)
                tz_display = " (EDT/EST)"
        except:
            local_end = end_time
            tz_display = " (container time)"
            
        print(f"✅ MANUAL SCAN COMPLETED: {scan_type} scan (mode: {scan_mode}) finished at {local_end.strftime('%Y-%m-%d %H:%M:%S')}{tz_display}")
        print(f"⏱️ MANUAL SCAN DURATION: {duration_str} (total time: {duration.total_seconds():.1f} seconds)")
        
        # Print optimization statistics for TV scans
        if scan_type in ["both", "tv"] and tv_series_total > 0:
            print(f"📊 TV SCAN OPTIMIZATION: Total: {tv_series_total}, Processed: {tv_series_processed}, Skipped: {tv_series_skipped}")
            if tv_series_skipped > 0:
                skip_percentage = (tv_series_skipped / tv_series_total) * 100
                print(f"⚡ TV PERFORMANCE BOOST: {tv_series_skipped}/{tv_series_total} series skipped ({skip_percentage:.1f}% time saved!)")
        
        # Print optimization statistics for movie scans
        if scan_type in ["both", "movies"] and movie_total > 0:
            print(f"📊 MOVIE SCAN OPTIMIZATION: Total: {movie_total}, Processed: {movie_processed}, Skipped: {movie_skipped}")
            if movie_skipped > 0:
                skip_percentage = (movie_skipped / movie_total) * 100
                print(f"⚡ MOVIE PERFORMANCE BOOST: {movie_skipped}/{movie_total} movies skipped ({skip_percentage:.1f}% time saved!)")
        
        # Print combined optimization statistics for "both" scans
        if scan_type == "both" and (tv_series_total > 0 or movie_total > 0):
            total_items = tv_series_total + movie_total
            total_skipped = tv_series_skipped + movie_skipped
            total_processed = tv_series_processed + movie_processed
            if total_skipped > 0:
                overall_skip_percentage = (total_skipped / total_items) * 100
                print(f"🎯 OVERALL OPTIMIZATION: {total_skipped}/{total_items} items skipped ({overall_skip_percentage:.1f}% total time saved!)")
    
    background_tasks.add_task(run_scan)
    return {"status": "started", "message": f"Manual {scan_type} scan started (mode: {scan_mode})"}


async def scan_tv_season(background_tasks: BackgroundTasks, request: TVSeasonRequest, dependencies: dict):
    """Scan a specific TV season - URL-safe endpoint"""
    nfo_manager = dependencies["nfo_manager"]
    tv_processor = dependencies["tv_processor"]
    
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
            print(f"INFO: Processing TV season: {season_dir}")
            try:
                tv_processor.process_season(series_dir, season_dir)
            except Exception as e:
                print(f"ERROR: Failed processing season {season_dir}: {e}")
        
        background_tasks.add_task(process_season)
        return {"status": "started", "message": f"Season scan started for {request.season_name}"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def scan_tv_episode(background_tasks: BackgroundTasks, request: TVEpisodeRequest, dependencies: dict):
    """Scan a specific TV episode - URL-safe endpoint"""
    nfo_manager = dependencies["nfo_manager"]
    tv_processor = dependencies["tv_processor"]
    
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
            print(f"INFO: Processing TV episode: {episode_file}")
            try:
                tv_processor.process_episode_file(series_dir, season_dir, episode_file)
            except Exception as e:
                print(f"ERROR: Failed processing episode {episode_file}: {e}")
        
        background_tasks.add_task(process_episode)
        return {"status": "started", "message": f"Episode scan started for {request.episode_name}"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def test_bulk_update(dependencies: dict):
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


async def test_movie_scan(dependencies: dict):
    """Test movie directory scanning logic"""
    config = dependencies["config"]
    nfo_manager = dependencies["nfo_manager"]
    
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


async def trigger_bulk_update(background_tasks: BackgroundTasks, dependencies: dict):
    """Trigger bulk update of all movies"""
    async def run_bulk_update():
        try:
            from bulk_update_movies import bulk_update_all_movies
            success = bulk_update_all_movies()
            print(f"INFO: Bulk update completed: {'success' if success else 'failed'}")
        except Exception as e:
            print(f"ERROR: Bulk update error: {e}")
    
    background_tasks.add_task(run_bulk_update)
    return {"status": "started", "message": "Bulk update started"}


async def debug_movie_priority_logic(imdb_id: str, dependencies: dict):
    """Debug endpoint showing how MOVIE_PRIORITY affects date selection"""
    config = dependencies["config"]
    movie_processor = dependencies["movie_processor"]
    
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


async def debug_tmdb_lookup(imdb_id: str, dependencies: dict):
    """Debug TMDB API lookup for a specific movie"""
    movie_processor = dependencies["movie_processor"]
    
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
        print(f"INFO: TMDB Debug: Looking up {imdb_id}")
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
            print(f"INFO: TMDB Debug: Getting release dates for TMDB ID {tmdb_id}")
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
# Database Cleanup Endpoints
# ---------------------------

async def delete_episode(imdb_id: str, season: int, episode: int, dependencies: dict):
    """Delete a specific episode from the database"""
    db = dependencies["db"]
    
    try:
        deleted = db.delete_episode(imdb_id, season, episode)
        
        if deleted:
            return {
                "success": True,
                "message": f"Deleted episode S{season:02d}E{episode:02d} from series {imdb_id}",
                "imdb_id": imdb_id,
                "season": season,
                "episode": episode
            }
        else:
            return {
                "success": False,
                "message": f"Episode S{season:02d}E{episode:02d} not found in series {imdb_id}",
                "imdb_id": imdb_id,
                "season": season,
                "episode": episode
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "imdb_id": imdb_id,
            "season": season,
            "episode": episode
        }


async def delete_series_episodes(imdb_id: str, dependencies: dict):
    """Delete all episodes for a series from the database"""
    db = dependencies["db"]
    
    try:
        deleted_count = db.delete_series_episodes(imdb_id)
        
        return {
            "success": True,
            "message": f"Deleted {deleted_count} episodes from series {imdb_id}",
            "imdb_id": imdb_id,
            "deleted_count": deleted_count
        }
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "imdb_id": imdb_id
        }


async def delete_movie(imdb_id: str, dependencies: dict):
    """Delete a specific movie from the database"""
    db = dependencies["db"]
    
    try:
        deleted = db.delete_movie(imdb_id)
        
        if deleted:
            return {
                "success": True,
                "message": f"Deleted movie {imdb_id} from database",
                "imdb_id": imdb_id
            }
        else:
            return {
                "success": False,
                "message": f"Movie {imdb_id} not found in database",
                "imdb_id": imdb_id
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "imdb_id": imdb_id
        }


async def cleanup_orphaned_episodes(dependencies: dict):
    """Find and delete episodes that don't have corresponding video files"""
    db = dependencies["db"]
    
    try:
        deleted_episodes = db.delete_orphaned_episodes()
        
        return {
            "success": True,
            "message": f"Cleaned up {len(deleted_episodes)} orphaned episodes",
            "deleted_count": len(deleted_episodes),
            "deleted_episodes": deleted_episodes
        }
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to cleanup orphaned episodes"
        }


async def cleanup_orphaned_movies(dependencies: dict):
    """Find and delete movies that don't have corresponding video files"""
    db = dependencies["db"]
    
    try:
        deleted_movies = db.delete_orphaned_movies()
        
        return {
            "success": True,
            "message": f"Cleaned up {len(deleted_movies)} orphaned movies",
            "deleted_count": len(deleted_movies),
            "deleted_movies": deleted_movies
        }
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to cleanup orphaned movies"
        }


async def cleanup_orphaned_series(dependencies: dict):
    """Find and delete TV series that don't have corresponding directories"""
    db = dependencies["db"]
    
    try:
        deleted_series = db.delete_orphaned_series()
        
        return {
            "success": True,
            "message": f"Cleaned up {len(deleted_series)} orphaned TV series",
            "deleted_count": len(deleted_series),
            "deleted_series": deleted_series
        }
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to cleanup orphaned TV series"
        }


# ---------------------------
# Route Registration
# ---------------------------

def register_routes(app, dependencies: dict):
    """
    Register all routes with the FastAPI app
    
    Args:
        app: FastAPI application instance
        dependencies: Dictionary containing:
            - db: NFOGuardDatabase instance
            - nfo_manager: NFOManager instance
            - path_mapper: PathMapper instance
            - tv_processor: TVProcessor instance
            - movie_processor: MovieProcessor instance
            - batcher: WebhookBatcher instance
            - start_time: Application start time
            - config: NFOGuardConfig instance
            - version: Application version string
    """
    
    @app.post("/webhook/sonarr")
    async def _sonarr_webhook(request: Request, background_tasks: BackgroundTasks):
        return await sonarr_webhook(request, background_tasks, dependencies)

    @app.post("/webhook/radarr") 
    async def _radarr_webhook(request: Request, background_tasks: BackgroundTasks):
        return await radarr_webhook(request, background_tasks, dependencies)

    @app.get("/health")
    async def _health() -> HealthResponse:
        return await health(dependencies)

    @app.get("/stats")
    async def _get_stats():
        return await get_stats(dependencies)

    @app.get("/batch/status") 
    async def _batch_status():
        return await batch_status(dependencies)

    @app.get("/debug/movie/{imdb_id}")
    async def _debug_movie_import_date(imdb_id: str):
        return await debug_movie_import_date(imdb_id, dependencies)

    @app.get("/debug/movie/{imdb_id}/history")
    async def _debug_movie_history(imdb_id: str):
        return await debug_movie_history(imdb_id, dependencies)

    @app.delete("/database/episode/{imdb_id}/{season}/{episode}")
    async def _delete_episode(imdb_id: str, season: int, episode: int):
        return await delete_episode(imdb_id, season, episode, dependencies)

    @app.delete("/database/series/{imdb_id}/episodes")
    async def _delete_series_episodes(imdb_id: str):
        return await delete_series_episodes(imdb_id, dependencies)

    @app.delete("/database/movie/{imdb_id}")
    async def _delete_movie(imdb_id: str):
        return await delete_movie(imdb_id, dependencies)

    @app.post("/database/cleanup/orphaned-episodes")
    async def _cleanup_orphaned_episodes():
        return await cleanup_orphaned_episodes(dependencies)

    @app.post("/database/cleanup/orphaned-movies")
    async def _cleanup_orphaned_movies():
        return await cleanup_orphaned_movies(dependencies)

    @app.post("/database/cleanup/orphaned-series")
    async def _cleanup_orphaned_series():
        return await cleanup_orphaned_series(dependencies)

    @app.post("/manual/scan")
    async def _manual_scan(background_tasks: BackgroundTasks, path: Optional[str] = None, scan_type: str = "both", scan_mode: str = "smart"):
        return await manual_scan(background_tasks, path, scan_type, scan_mode, dependencies)

    @app.post("/tv/scan-season")
    async def _scan_tv_season(background_tasks: BackgroundTasks, request: TVSeasonRequest):
        return await scan_tv_season(background_tasks, request, dependencies)

    @app.post("/tv/scan-episode")
    async def _scan_tv_episode(background_tasks: BackgroundTasks, request: TVEpisodeRequest):
        return await scan_tv_episode(background_tasks, request, dependencies)

    @app.post("/test/bulk-update")
    async def _test_bulk_update():
        return await test_bulk_update(dependencies)

    @app.post("/test/movie-scan")
    async def _test_movie_scan():
        return await test_movie_scan(dependencies)

    @app.post("/bulk/update")
    async def _trigger_bulk_update(background_tasks: BackgroundTasks):
        return await trigger_bulk_update(background_tasks, dependencies)

    @app.get("/debug/movie/{imdb_id}/priority")
    async def _debug_movie_priority_logic(imdb_id: str):
        return await debug_movie_priority_logic(imdb_id, dependencies)

    @app.get("/debug/tmdb/{imdb_id}")
    async def _debug_tmdb_lookup(imdb_id: str):
        return await debug_tmdb_lookup(imdb_id, dependencies)

    # Include monitoring routes
    from api.monitoring_routes import router as monitoring_router
    app.include_router(monitoring_router)
    
    # ---------------------------
    # Web Interface API Routes
    # ---------------------------
    
    @app.get("/api/dashboard")
    async def _dashboard_stats():
        """Get dashboard statistics"""
        return await get_dashboard_stats(dependencies)
    
    @app.get("/api/movies")
    async def _movies_list(skip: int = 0, limit: int = 100, has_date: Optional[bool] = None, 
                          source_filter: Optional[str] = None, search: Optional[str] = None,
                          imdb_search: Optional[str] = None):
        """Get paginated movies list with filtering"""
        return await get_movies_list(dependencies, skip, limit, has_date, source_filter, search, imdb_search)
    
    @app.get("/api/series")
    async def _series_list(skip: int = 0, limit: int = 50, search: Optional[str] = None, 
                          imdb_search: Optional[str] = None, date_filter: Optional[str] = None, 
                          source_filter: Optional[str] = None):
        """Get paginated TV series list with filtering"""
        return await get_tv_series_list(dependencies, skip, limit, search, imdb_search, date_filter, source_filter)
    
    @app.get("/api/series/{imdb_id}/episodes")
    async def _series_episodes(imdb_id: str):
        """Get episodes for a specific series"""
        return await get_series_episodes(dependencies, imdb_id)
    
    @app.get("/api/series/sources")
    async def _series_sources():
        """Get list of available episode sources for filtering"""
        return await get_series_sources(dependencies)
    
    @app.get("/api/debug/series-date-distribution")
    async def _debug_series_dates():
        """Debug endpoint showing TV series date distribution"""
        return await debug_series_date_distribution(dependencies)
    
    @app.get("/api/reports/missing-dates")
    async def _missing_dates_report():
        """Get report of content missing dateadded"""
        return await get_missing_dates_report(dependencies)
    
    @app.put("/api/movies/{imdb_id}")
    async def _update_movie(imdb_id: str, request: MovieUpdateRequest):
        """Update movie dateadded"""
        return await update_movie_date(dependencies, imdb_id, request.dateadded, request.source)
    
    @app.put("/api/episodes/{imdb_id}/{season}/{episode}")
    async def _update_episode(imdb_id: str, season: int, episode: int, request: EpisodeUpdateRequest):
        """Update episode dateadded"""
        return await update_episode_date(dependencies, imdb_id, season, episode, request.dateadded, request.source)
    
    @app.post("/api/bulk/update-source")
    async def _bulk_update_source(request: BulkUpdateRequest):
        """Bulk update source for movies or episodes"""
        return await bulk_update_source(dependencies, request.media_type, request.old_source, request.new_source)
    
    @app.get("/api/movies/{imdb_id}/date-options")
    async def _movie_date_options(imdb_id: str):
        """Get available date options for a movie"""
        return await get_movie_date_options(dependencies, imdb_id)
    
    @app.get("/api/episodes/{imdb_id}/{season}/{episode}/date-options")
    async def _episode_date_options(imdb_id: str, season: int, episode: int):
        """Get available date options for an episode"""
        return await get_episode_date_options(dependencies, imdb_id, season, episode)
    
    @app.get("/api/debug/movie/{imdb_id}/raw")
    async def _debug_movie_raw(imdb_id: str):
        """Debug endpoint to see raw movie database data"""
        db = dependencies["db"]
        movie = db.get_movie_dates(imdb_id)
        if not movie:
            raise HTTPException(status_code=404, detail="Movie not found")
        return {"raw_data": dict(movie), "imdb_id": imdb_id}
    # ---------------------------
    # Static Web Interface
    # ---------------------------
    
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import FileResponse
    import os
    
    # Serve static files for web interface
    static_dir = os.path.join(os.path.dirname(__file__), "..", "static")
    if os.path.exists(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")
    
    @app.get("/")
    async def _serve_index():
        """Serve web interface index page"""
        index_path = os.path.join(static_dir, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
        else:
            return {"message": "NFOGuard Web Interface - API endpoints available at /api/", "api_docs": "/docs"}