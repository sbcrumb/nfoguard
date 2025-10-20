"""
Async NFO Manager for NFOGuard
High-performance async NFO file operations with concurrent processing
"""
import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import xml.etree.ElementTree as ET

from utils.logging import _log
from utils.async_file_utils import (
    async_read_nfo_file,
    async_write_nfo_file,
    async_file_exists,
    async_set_file_mtime,
    async_batch_nfo_operations,
    async_concurrent_episode_processing
)
from utils.nfo_patterns import (
    create_basic_nfo_structure,
    extract_imdb_from_nfo_content,
    extract_dates_from_nfo,
    extract_imdb_id_from_text
)
from utils.validation import validate_date_string


class AsyncNFOManager:
    """Async NFO file manager with concurrent processing capabilities"""
    
    def __init__(self, manager_brand: str = "NFOGuard", debug: bool = False):
        self.manager_brand = manager_brand
        self.debug = debug
    
    async def async_parse_imdb_from_path(self, path: Path) -> Optional[str]:
        """
        Async extract IMDb ID from directory path or filename
        
        Args:
            path: Path to examine
            
        Returns:
            IMDb ID if found, None otherwise
        """
        # Use the sync version since it's just string processing
        return extract_imdb_id_from_text(str(path))
    
    async def async_parse_imdb_from_nfo(self, nfo_path: Path) -> Optional[str]:
        """
        Async extract IMDb ID from NFO file content
        
        Args:
            nfo_path: Path to NFO file
            
        Returns:
            IMDb ID if found, None otherwise
        """
        root = await async_read_nfo_file(nfo_path)
        if root is None:
            return None
        
        return extract_imdb_from_nfo_content(root)
    
    async def async_find_movie_imdb_id(self, movie_dir: Path) -> Optional[str]:
        """
        Async find IMDb ID from directory name, filenames, or NFO file
        
        Args:
            movie_dir: Path to movie directory
            
        Returns:
            IMDb ID if found, None otherwise
        """
        if self.debug:
            _log("DEBUG", f"Async searching for IMDb ID in: {movie_dir.name}")
        
        # First try directory name
        imdb_id = await self.async_parse_imdb_from_path(movie_dir)
        if imdb_id:
            if self.debug:
                _log("DEBUG", f"Found IMDb ID in directory name: {imdb_id}")
            return imdb_id
        
        # Try all files in the directory concurrently
        if await async_file_exists(movie_dir):
            try:
                from utils.async_file_utils import aiofiles
                entries = await aiofiles.os.listdir(movie_dir)
                
                # Create tasks to check all files
                async def check_file(filename: str) -> Optional[str]:
                    file_path = movie_dir / filename
                    if await aiofiles.os.path.isfile(file_path):
                        return await self.async_parse_imdb_from_path(file_path)
                    return None
                
                # Check all files concurrently
                results = await asyncio.gather(
                    *[check_file(filename) for filename in entries],
                    return_exceptions=True
                )
                
                # Find first valid IMDb ID
                for result in results:
                    if isinstance(result, str) and result:
                        if self.debug:
                            _log("DEBUG", f"Found IMDb ID in filename: {result}")
                        return result
                        
            except Exception as e:
                _log("WARNING", f"Failed to scan directory {movie_dir}: {e}")
        
        # Finally, try NFO file content
        nfo_path = movie_dir / "movie.nfo"
        imdb_id = await self.async_parse_imdb_from_nfo(nfo_path)
        if imdb_id:
            if self.debug:
                _log("DEBUG", f"Found IMDb ID in NFO file: {imdb_id}")
            return imdb_id
        
        if self.debug:
            _log("DEBUG", f"No IMDb ID found for: {movie_dir.name}")
        return None
    
    async def async_create_movie_nfo(
        self,
        movie_dir: Path,
        imdb_id: str,
        dateadded: str,
        premiered: Optional[str] = None,
        lock_metadata: bool = True
    ) -> bool:
        """
        Async create movie NFO file
        
        Args:
            movie_dir: Path to movie directory
            imdb_id: IMDb ID
            dateadded: Date added
            premiered: Optional premiere date
            lock_metadata: Whether to lock metadata
            
        Returns:
            True if successful, False otherwise
        """
        try:
            nfo_path = movie_dir / "movie.nfo"
            
            # Prepare dates
            dates = {"dateadded": dateadded}
            if premiered and validate_date_string(premiered):
                dates["premiered"] = premiered
            
            # Create NFO structure
            root = create_basic_nfo_structure(
                media_type="movie",
                title=movie_dir.name,
                imdb_id=imdb_id,
                dates=dates,
                additional_fields={"source": self.manager_brand}
            )
            
            # Write NFO file asynchronously
            success = await async_write_nfo_file(nfo_path, root, lock_metadata)
            
            if success and self.debug:
                _log("DEBUG", f"Created movie NFO: {nfo_path}")
            
            return success
            
        except Exception as e:
            _log("ERROR", f"Failed to create movie NFO for {movie_dir}: {e}")
            return False
    
    async def async_create_episode_nfo(
        self,
        season_dir: Path,
        season: int,
        episode: int,
        aired: Optional[str] = None,
        dateadded: Optional[str] = None,
        source: str = "unknown",
        lock_metadata: bool = True
    ) -> bool:
        """
        Async create episode NFO file
        
        Args:
            season_dir: Path to season directory
            season: Season number
            episode: Episode number
            aired: Optional air date
            dateadded: Optional date added
            source: Source of the data
            lock_metadata: Whether to lock metadata
            
        Returns:
            True if successful, False otherwise
        """
        try:
            nfo_filename = f"S{season:02d}E{episode:02d}.nfo"
            nfo_path = season_dir / nfo_filename
            
            # Prepare dates
            dates = {}
            if aired and validate_date_string(aired):
                dates["aired"] = aired
            if dateadded and validate_date_string(dateadded):
                dates["dateadded"] = dateadded
            
            # Create NFO structure
            root = create_basic_nfo_structure(
                media_type="episodedetails",
                title=f"S{season:02d}E{episode:02d}",
                dates=dates,
                additional_fields={
                    "season": str(season),
                    "episode": str(episode),
                    "source": source
                }
            )
            
            # Write NFO file asynchronously
            success = await async_write_nfo_file(nfo_path, root, lock_metadata)
            
            if success and self.debug:
                _log("DEBUG", f"Created episode NFO: {nfo_path}")
            
            return success
            
        except Exception as e:
            _log("ERROR", f"Failed to create episode NFO S{season:02d}E{episode:02d}: {e}")
            return False
    
    async def async_batch_create_episode_nfos(
        self,
        episode_data_list: List[Dict[str, Any]],
        max_concurrent: int = 5
    ) -> List[bool]:
        """
        Batch create multiple episode NFOs concurrently
        
        Args:
            episode_data_list: List of episode data dictionaries
            max_concurrent: Maximum concurrent NFO operations
            
        Returns:
            List of success/failure results
        """
        async def _create_episode_nfo(episode_data: Dict[str, Any]) -> bool:
            return await self.async_create_episode_nfo(
                season_dir=episode_data.get('season_dir'),
                season=episode_data.get('season'),
                episode=episode_data.get('episode'),
                aired=episode_data.get('aired'),
                dateadded=episode_data.get('dateadded'),
                source=episode_data.get('source', 'unknown'),
                lock_metadata=episode_data.get('lock_metadata', True)
            )
        
        return await async_concurrent_episode_processing(
            episode_data_list,
            _create_episode_nfo,
            max_concurrent
        )
    
    async def async_set_file_mtime(self, file_path: Path, date_str: str) -> bool:
        """
        Async set file modification time from date string
        
        Args:
            file_path: Path to file
            date_str: Date string in ISO format
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not validate_date_string(date_str):
                _log("WARNING", f"Invalid date format for mtime: {date_str}")
                return False
            
            # Parse date string to timestamp
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            timestamp = dt.timestamp()
            
            # Set file mtime asynchronously
            success = await async_set_file_mtime(file_path, timestamp)
            
            if success and self.debug:
                _log("DEBUG", f"Set mtime for {file_path}: {date_str}")
            
            return success
            
        except Exception as e:
            _log("ERROR", f"Failed to set mtime for {file_path}: {e}")
            return False
    
    async def async_batch_set_file_mtimes(
        self,
        file_mtime_pairs: List[Tuple[Path, str]],
        max_concurrent: int = 10
    ) -> List[bool]:
        """
        Batch set file modification times concurrently
        
        Args:
            file_mtime_pairs: List of (file_path, date_str) tuples
            max_concurrent: Maximum concurrent operations
            
        Returns:
            List of success/failure results
        """
        async def _set_single_mtime(file_path: Path, date_str: str) -> bool:
            return await self.async_set_file_mtime(file_path, date_str)
        
        # Create tasks for all mtime operations
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def _set_mtime_with_semaphore(file_path: Path, date_str: str) -> bool:
            async with semaphore:
                return await _set_single_mtime(file_path, date_str)
        
        tasks = [
            _set_mtime_with_semaphore(file_path, date_str)
            for file_path, date_str in file_mtime_pairs
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return [result if isinstance(result, bool) else False for result in results]
    
    async def async_validate_nfo_integrity(
        self,
        nfo_paths: List[Path],
        max_concurrent: int = 10
    ) -> Dict[str, Any]:
        """
        Async validate integrity of multiple NFO files
        
        Args:
            nfo_paths: List of NFO file paths to validate
            max_concurrent: Maximum concurrent validations
            
        Returns:
            Dictionary with validation results and statistics
        """
        results = {
            'total_files': len(nfo_paths),
            'valid_files': 0,
            'invalid_files': 0,
            'missing_files': 0,
            'validation_errors': [],
            'file_results': {}
        }
        
        async def _validate_single_nfo(nfo_path: Path) -> Dict[str, Any]:
            file_result = {
                'path': str(nfo_path),
                'exists': False,
                'valid_xml': False,
                'has_imdb_id': False,
                'has_dates': False,
                'error': None
            }
            
            try:
                # Check if file exists
                if not await async_file_exists(nfo_path):
                    file_result['error'] = 'File does not exist'
                    return file_result
                
                file_result['exists'] = True
                
                # Try to parse NFO
                root = await async_read_nfo_file(nfo_path)
                if root is None:
                    file_result['error'] = 'Failed to parse XML'
                    return file_result
                
                file_result['valid_xml'] = True
                
                # Check for IMDb ID
                imdb_id = extract_imdb_from_nfo_content(root)
                file_result['has_imdb_id'] = bool(imdb_id)
                
                # Check for dates
                dates = extract_dates_from_nfo(root)
                file_result['has_dates'] = any(dates.values())
                
            except Exception as e:
                file_result['error'] = str(e)
            
            return file_result
        
        # Validate all files concurrently
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def _validate_with_semaphore(nfo_path: Path) -> Dict[str, Any]:
            async with semaphore:
                return await _validate_single_nfo(nfo_path)
        
        tasks = [_validate_with_semaphore(nfo_path) for nfo_path in nfo_paths]
        file_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for i, result in enumerate(file_results):
            if isinstance(result, dict):
                path_str = str(nfo_paths[i])
                results['file_results'][path_str] = result
                
                if not result['exists']:
                    results['missing_files'] += 1
                elif result['valid_xml']:
                    results['valid_files'] += 1
                else:
                    results['invalid_files'] += 1
                
                if result.get('error'):
                    results['validation_errors'].append(f"{path_str}: {result['error']}")
        
        return results