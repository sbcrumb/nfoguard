"""
Webhook Batching System for NFOGuard
Handles batching and processing of webhook events to avoid processing storms
"""
import threading
from pathlib import Path
from typing import Dict, Set
from concurrent.futures import ThreadPoolExecutor

from config.settings import config
from utils.logging import _log


class WebhookBatcher:
    """Batches webhook events to avoid processing storms"""
    
    def __init__(self, nfo_manager=None):
        self.pending: Dict[str, Dict] = {}
        self.timers: Dict[str, threading.Timer] = {}
        self.processing: Set[str] = set()
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=config.max_concurrent)
        # Will be set by the application when processors are available
        self.tv_processor = None
        self.movie_processor = None
        # NFO manager for comprehensive IMDb detection
        self.nfo_manager = nfo_manager
    
    def set_processors(self, tv_processor, movie_processor):
        """Set the processor instances"""
        self.tv_processor = tv_processor
        self.movie_processor = movie_processor
    
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
                
                # Use comprehensive IMDb detection (directory, filenames, NFO files)
                if self.nfo_manager:
                    detected_imdb = self.nfo_manager.find_movie_imdb_id(path_obj)
                    imdb_match = False
                    if detected_imdb:
                        # Compare with and without 'tt' prefix for flexibility
                        if detected_imdb == expected_imdb or detected_imdb.replace('tt', '') == expected_imdb.replace('tt', ''):
                            imdb_match = True
                    
                    if not imdb_match:
                        _log("ERROR", f"BATCH VALIDATION FAILED: Expected IMDb {expected_imdb} not found via comprehensive detection in path {path_str}")
                        _log("ERROR", f"Detected IMDb: {detected_imdb}, Expected: {expected_imdb}")
                        _log("ERROR", f"This prevents processing wrong movies due to batch corruption")
                        return
                    _log("DEBUG", f"Batch validation passed: IMDb {expected_imdb} matches detected {detected_imdb}")
                else:
                    # Fallback to simple string search if nfo_manager not available
                    if expected_imdb not in path_str.lower():
                        _log("ERROR", f"BATCH VALIDATION FAILED: Expected IMDb {expected_imdb} not found in path {path_str} (fallback mode)")
                        _log("ERROR", f"This prevents processing wrong movies due to batch corruption")
                        return
                    _log("DEBUG", f"Batch validation passed: IMDb {expected_imdb} found in path (fallback mode)")
            
            # CRITICAL: Validate that the path contains the expected IMDb ID for TV shows
            if media_type == 'tv':
                expected_imdb = key.replace('tv:', '') if key.startswith('tv:') else key
                
                # Use comprehensive IMDb detection (directory, filenames, tvshow.nfo files)
                if self.nfo_manager:
                    detected_imdb = self.nfo_manager.find_series_imdb_id(path_obj)
                    imdb_match = False
                    if detected_imdb:
                        # Compare with and without 'tt' prefix for flexibility
                        if detected_imdb == expected_imdb or detected_imdb.replace('tt', '') == expected_imdb.replace('tt', ''):
                            imdb_match = True
                    
                    if not imdb_match:
                        _log("ERROR", f"BATCH VALIDATION FAILED: Expected IMDb {expected_imdb} not found via comprehensive detection in TV path {path_str}")
                        _log("ERROR", f"Detected TV IMDb: {detected_imdb}, Expected: {expected_imdb}")
                        _log("ERROR", f"This prevents processing wrong TV series due to batch corruption")
                        return
                    _log("DEBUG", f"TV batch validation passed: IMDb {expected_imdb} matches detected {detected_imdb}")
                else:
                    # Fallback to simple string search if nfo_manager not available
                    if expected_imdb not in path_str.lower():
                        _log("ERROR", f"BATCH VALIDATION FAILED: Expected IMDb {expected_imdb} not found in TV path {path_str} (fallback mode)")
                        _log("ERROR", f"This prevents processing wrong TV series due to batch corruption")
                        return
                    _log("DEBUG", f"TV batch validation passed: IMDb {expected_imdb} found in path (fallback mode)")
                
                if not self.tv_processor:
                    _log("ERROR", "TV processor not available")
                    return
                    
                # Check processing mode for TV webhooks
                processing_mode = webhook_data.get('processing_mode', config.tv_webhook_processing_mode)
                episodes_data = webhook_data.get('episodes', [])
                
                if processing_mode == 'targeted' and episodes_data:
                    _log("INFO", f"Using targeted episode processing for {len(episodes_data)} episodes")
                    self.tv_processor.process_webhook_episodes(path_obj, episodes_data)
                else:
                    _log("INFO", f"Using series processing mode (fallback or configured)")
                    self.tv_processor.process_series(path_obj)
                    
            elif media_type == 'movie':
                if not self.movie_processor:
                    _log("ERROR", "Movie processor not available")
                    return
                    
                self.movie_processor.process_movie(path_obj, webhook_mode=True)
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
    
    def shutdown(self):
        """Shutdown the webhook batcher gracefully"""
        _log("INFO", "Shutting down webhook batcher...")
        
        with self.lock:
            # Cancel all pending timers
            for timer in self.timers.values():
                try:
                    timer.cancel()
                except Exception as e:
                    _log("WARNING", f"Error canceling timer: {e}")
            
            self.timers.clear()
            
            # Log any remaining items
            if self.pending:
                _log("WARNING", f"Shutting down with {len(self.pending)} pending items")
            if self.processing:
                _log("INFO", f"Waiting for {len(self.processing)} items to finish processing...")
        
        # Shutdown the thread pool executor
        try:
            self.executor.shutdown(wait=True, timeout=10)  # Wait up to 10 seconds
            _log("INFO", "Thread pool executor shut down successfully")
        except Exception as e:
            _log("WARNING", f"Error shutting down thread pool: {e}")
        
        _log("INFO", "Webhook batcher shutdown complete")