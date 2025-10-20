#!/usr/bin/env python3
"""
Episode NFO Manager - Handles TV episode NFO creation with video filename matching
Core principle: NFO filenames should match video filenames
"""
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import re
from .logging import _log


class EpisodeNFOManager:
    """Manages episode NFO files with video filename matching"""
    
    def __init__(self, manager_brand: str = "NFOGuard"):
        self.manager_brand = manager_brand
    
    def find_video_files_for_season(self, season_dir: Path) -> Dict[Tuple[int, int], List[Path]]:
        """Find all video files in season directory, grouped by (season, episode)"""
        if not season_dir.exists():
            _log("DEBUG", f"Season directory does not exist: {season_dir}")
            return {}
        
        video_extensions = [".mkv", ".mp4", ".avi", ".mov", ".m4v"]
        episodes = {}
        
        _log("DEBUG", f"Scanning video files in: {season_dir}")
        for video_file in season_dir.iterdir():
            _log("DEBUG", f"Checking file: {video_file.name} (is_file: {video_file.is_file()}, suffix: {video_file.suffix.lower()})")
            if (video_file.is_file() and 
                video_file.suffix.lower() in video_extensions):
                
                episode_info = self._parse_episode_from_filename(video_file.name)
                _log("DEBUG", f"Episode parsing for '{video_file.name}': {episode_info}")
                if episode_info:
                    season_num, episode_num = episode_info
                    key = (season_num, episode_num)
                    if key not in episodes:
                        episodes[key] = []
                    episodes[key].append(video_file)
                    _log("DEBUG", f"Added video file: S{season_num:02d}E{episode_num:02d} → {video_file.name}")
        
        _log("DEBUG", f"Total video files found: {len(episodes)} episodes")
        return episodes
    
    def _parse_episode_from_filename(self, filename: str) -> Optional[Tuple[int, int]]:
        """Extract season and episode numbers from filename"""
        # Try S##E## format first (most common)
        match = re.search(r'[Ss](\d{1,2})[Ee](\d{1,2})', filename)
        if match:
            return int(match.group(1)), int(match.group(2))
        
        # Try ##x## format  
        match = re.search(r'(\d{1,2})x(\d{1,2})', filename)
        if match:
            return int(match.group(1)), int(match.group(2))
        
        return None
    
    def find_nfo_for_episode(self, season_dir: Path, season_num: int, episode_num: int) -> Optional[Path]:
        """Find existing NFO file for episode (prefer video-matching filename)"""
        if not season_dir.exists():
            return None
        
        # First, look for NFO files that match video filenames
        video_files = self.find_video_files_for_season(season_dir)
        key = (season_num, episode_num)
        
        if key in video_files:
            for video_file in video_files[key]:
                potential_nfo = season_dir / f"{video_file.stem}.nfo"
                if potential_nfo.exists():
                    _log("DEBUG", f"Found video-matching NFO: {potential_nfo.name}")
                    return potential_nfo
        
        # Fallback: look for short name NFO
        short_nfo = season_dir / f"S{season_num:02d}E{episode_num:02d}.nfo"
        if short_nfo.exists():
            _log("DEBUG", f"Found short-name NFO: {short_nfo.name}")
            return short_nfo
        
        # Last resort: search all NFO files for matching season/episode data
        for nfo_file in season_dir.glob("*.nfo"):
            if self._nfo_matches_episode(nfo_file, season_num, episode_num):
                _log("DEBUG", f"Found matching NFO by content: {nfo_file.name}")
                return nfo_file
        
        return None
    
    def _nfo_matches_episode(self, nfo_path: Path, season_num: int, episode_num: int) -> bool:
        """Check if NFO file contains the specified season/episode"""
        try:
            tree = ET.parse(nfo_path)
            root = tree.getroot()
            
            if root.tag == "episodedetails":
                season_elem = root.find("season")
                episode_elem = root.find("episode")
                
                if (season_elem is not None and episode_elem is not None and
                    season_elem.text and episode_elem.text):
                    try:
                        file_season = int(season_elem.text)
                        file_episode = int(episode_elem.text)
                        return file_season == season_num and file_episode == episode_num
                    except ValueError:
                        pass
        except (ET.ParseError, Exception):
            pass
        
        return False
    
    def get_target_nfo_path(self, season_dir: Path, season_num: int, episode_num: int) -> Path:
        """Get the target NFO path (prefer video filename, fallback to short name)"""
        video_files = self.find_video_files_for_season(season_dir)
        key = (season_num, episode_num)
        
        if key in video_files and video_files[key]:
            # Use the first video file found (handle multiple files gracefully)
            video_file = video_files[key][0]
            target_nfo = season_dir / f"{video_file.stem}.nfo"
            _log("DEBUG", f"Target NFO will match video: {target_nfo.name}")
            return target_nfo
        else:
            # Fallback to short name if no video file found
            target_nfo = season_dir / f"S{season_num:02d}E{episode_num:02d}.nfo"
            _log("WARNING", f"No video file found for S{season_num:02d}E{episode_num:02d}, using short name: {target_nfo.name}")
            return target_nfo
    
    def migrate_nfo_to_video_filename(self, season_dir: Path, season_num: int, episode_num: int) -> bool:
        """If short-name NFO exists, rename it to match video filename"""
        existing_nfo = self.find_nfo_for_episode(season_dir, season_num, episode_num)
        target_nfo = self.get_target_nfo_path(season_dir, season_num, episode_num)
        
        # If we already have the right filename, nothing to do
        if existing_nfo and existing_nfo == target_nfo:
            return True
        
        # If we have an NFO but it doesn't match target, rename it
        if existing_nfo and existing_nfo != target_nfo:
            try:
                _log("INFO", f"Migrating NFO filename: {existing_nfo.name} -> {target_nfo.name}")
                existing_nfo.rename(target_nfo)
                return True
            except Exception as e:
                _log("ERROR", f"Failed to rename NFO: {e}")
                return False
        
        return False
    
    def create_episode_nfo(self, season_dir: Path, season_num: int, episode_num: int,
                          aired: Optional[str], dateadded: Optional[str], source: str,
                          title: Optional[str] = None, plot: Optional[str] = None) -> bool:
        """Create or update episode NFO with video filename matching"""
        
        # Get the target NFO path (matching video filename)
        nfo_path = self.get_target_nfo_path(season_dir, season_num, episode_num)
        
        # Migrate existing NFO if needed
        self.migrate_nfo_to_video_filename(season_dir, season_num, episode_num)
        
        try:
            # Load existing NFO if it exists
            episode_elem = None
            if nfo_path.exists():
                try:
                    tree = ET.parse(nfo_path)
                    episode_elem = tree.getroot()
                    
                    if episode_elem.tag != "episodedetails":
                        raise ValueError("Root element is not <episodedetails>")
                    
                    # Remove NFOGuard-managed fields (we'll re-add them)
                    for tag in ["season", "episode", "aired", "premiered", "dateadded", "lockdata"]:
                        existing = episode_elem.find(tag)
                        if existing is not None:
                            episode_elem.remove(existing)
                    
                    _log("DEBUG", f"Loaded existing NFO content for {nfo_path.name}")
                    
                except (ET.ParseError, ValueError) as e:
                    _log("WARNING", f"Corrupted NFO file {nfo_path.name}: {e}. Creating new one.")
                    episode_elem = None
            
            # Create new structure if needed
            if episode_elem is None:
                episode_elem = ET.Element("episodedetails")
            
            # Add title if provided and not already present
            if title and not episode_elem.find("title"):
                title_elem = ET.SubElement(episode_elem, "title")
                title_elem.text = title
            
            # Add plot if provided and not already present  
            if plot and not episode_elem.find("plot"):
                plot_elem = ET.SubElement(episode_elem, "plot")
                plot_elem.text = plot
            
            # Add NFOGuard fields at the end
            season_elem = ET.SubElement(episode_elem, "season")
            season_elem.text = str(season_num)
            
            episode_num_elem = ET.SubElement(episode_elem, "episode")
            episode_num_elem.text = str(episode_num)
            
            if aired:
                aired_elem = ET.SubElement(episode_elem, "aired")
                aired_elem.text = aired[:10] if len(aired) >= 10 else aired
                
                # Also add premiered for compatibility
                premiered_elem = ET.SubElement(episode_elem, "premiered") 
                premiered_elem.text = aired[:10] if len(aired) >= 10 else aired
            
            if dateadded:
                dateadded_elem = ET.SubElement(episode_elem, "dateadded")
                dateadded_elem.text = dateadded
            
            # Add lockdata
            lockdata_elem = ET.SubElement(episode_elem, "lockdata")
            lockdata_elem.text = "true"
            
            # Add comment with source
            comment = ET.Comment(f" Created by {self.manager_brand} - Source: {source} ")
            episode_elem.append(comment)
            
            # Write the NFO file
            tree = ET.ElementTree(episode_elem)
            ET.indent(tree, space="  ", level=0)
            tree.write(nfo_path, encoding='utf-8', xml_declaration=True)
            
            _log("INFO", f"✅ Created/updated episode NFO: {nfo_path.name}")
            _log("INFO", f"   S{season_num:02d}E{episode_num:02d}, Aired: {aired}, DateAdded: {dateadded}, Source: {source}")
            
            return True
            
        except Exception as e:
            _log("ERROR", f"Failed to create episode NFO {nfo_path}: {e}")
            return False
    
    def extract_nfoguard_data(self, nfo_path: Path) -> Optional[Dict[str, str]]:
        """Extract NFOGuard-managed data from existing NFO"""
        if not nfo_path.exists():
            return None
        
        try:
            tree = ET.parse(nfo_path)
            root = tree.getroot()
            
            if root.tag != "episodedetails":
                return None
            
            # Look for NFOGuard fields
            dateadded_elem = root.find("dateadded")
            aired_elem = root.find("aired")
            lockdata_elem = root.find("lockdata")
            
            # Only consider it NFOGuard-managed if it has dateadded and lockdata
            if (dateadded_elem is not None and dateadded_elem.text and
                lockdata_elem is not None and lockdata_elem.text == "true"):
                
                result = {
                    "dateadded": dateadded_elem.text.strip(),
                    "source": "existing_nfo"
                }
                
                if aired_elem is not None and aired_elem.text:
                    result["aired"] = aired_elem.text.strip()
                
                _log("DEBUG", f"Found NFOGuard data in {nfo_path.name}: {result}")
                return result
        
        except (ET.ParseError, Exception) as e:
            _log("WARNING", f"Error parsing NFO {nfo_path.name}: {e}")
        
        return None