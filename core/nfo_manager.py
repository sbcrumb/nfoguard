#!/usr/bin/env python3
"""
NFO Manager for creating and managing metadata files
Handles NFO creation for movies, TV shows, seasons, and episodes
"""
import os
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import re


class NFOManager:
    """Manages NFO file creation and updates"""
    
    def __init__(self, manager_brand: str = "NFOGuard"):
        self.manager_brand = manager_brand
    
    def parse_imdb_from_path(self, path: Path) -> Optional[str]:
        """Extract IMDb ID from directory path or filename"""
        # Look for various IMDb patterns in both directory and file names
        path_str = str(path).lower()
        
        # Try [imdb-ttXXXXXXX] format first (most explicit)
        match = re.search(r'\[imdb-?(tt\d+)\]', path_str)
        if match:
            return match.group(1)
        
        # Try standalone [ttXXXXXXX] format in brackets
        match = re.search(r'\[(tt\d+)\]', path_str)
        if match:
            return match.group(1)
        
        # Try {imdb-ttXXXXXXX} format with curly braces
        match = re.search(r'\{imdb-?(tt\d+)\}', path_str)
        if match:
            return match.group(1)
        
        # Try (imdb-ttXXXXXXX) format with parentheses
        match = re.search(r'\(imdb-?(tt\d+)\)', path_str)
        if match:
            return match.group(1)
        
        # Try ttXXXXXXX at end of filename/dirname (common pattern)
        match = re.search(r'[-_\s](tt\d+)$', path_str)
        if match:
            return match.group(1)
        
        return None
    
    def create_movie_nfo(self, movie_dir: Path, imdb_id: str, dateadded: str, 
                        released: Optional[str] = None, source: str = "unknown",
                        lock_metadata: bool = True) -> None:
        """Create or update movie.nfo file preserving existing content"""
        nfo_path = movie_dir / "movie.nfo"
        
        try:
            # Try to load existing NFO file
            if nfo_path.exists():
                try:
                    tree = ET.parse(nfo_path)
                    movie = tree.getroot()
                    
                    # Ensure root element is <movie>
                    if movie.tag != "movie":
                        raise ValueError("Root element is not <movie>")
                    
                    # Remove existing NFOGuard-managed elements to avoid duplicates
                    # These will be re-added at the very bottom
                    nfoguard_fields = ["dateadded", "lockdata", "premiered", "year"]
                    for tag in nfoguard_fields:
                        existing = movie.find(tag)
                        if existing is not None:
                            # Store the value before removing (for premiered/year)
                            if tag == "premiered" and not released:
                                released = existing.text  # Preserve existing premiered date
                            movie.remove(existing)
                    
                    # Remove ALL existing uniqueid with type="imdb" regardless of attributes
                    # We'll add a clean one at the bottom
                    for uniqueid in movie.findall("uniqueid[@type='imdb']"):
                        movie.remove(uniqueid)
                        
                except (ET.ParseError, ValueError) as e:
                    print(f"⚠️  Corrupted NFO detected: {nfo_path} - {str(e)[:100]}...")
                    print(f"   Creating new clean NFO file to replace corrupted one")
                    movie = ET.Element("movie")
            else:
                # Create new NFO structure
                movie = ET.Element("movie")
            
            # Now append ALL NFOGuard and date fields at the VERY END of the file
            # This ensures they appear after all existing content including actors
            
            # Add IMDb uniqueid at the end (after all existing content)
            uniqueid = ET.SubElement(movie, "uniqueid", type="imdb", default="true")
            uniqueid.text = imdb_id
            
            # Add premiered date at the bottom if we have it
            if released:
                premiered_elem = ET.SubElement(movie, "premiered")
                premiered_elem.text = released[:10] if len(released) >= 10 else released
                
                # Extract year from premiered date for consistency
                try:
                    year_value = released[:4] if len(released) >= 4 else None
                    if year_value and year_value.isdigit():
                        year_elem = ET.SubElement(movie, "year")
                        year_elem.text = year_value
                except:
                    pass  # Skip year if we can't extract it
            
            # Add dateadded at the end
            if dateadded:
                dateadded_elem = ET.SubElement(movie, "dateadded")
                dateadded_elem.text = dateadded
            
            # Add lockdata at the very end
            if lock_metadata:
                lockdata = ET.SubElement(movie, "lockdata")
                lockdata.text = "true"
            
            # Add NFOGuard comment at the beginning
            comment_text = f" Created by {self.manager_brand} - Source: {source} "
            
            # Write file with proper formatting
            tree = ET.ElementTree(movie)
            ET.indent(tree, space="  ", level=0)
            
            # Write to string first to add comment
            import xml.etree.ElementTree as ET_temp
            xml_str = ET.tostring(movie, encoding='unicode')
            
            # Add XML declaration and comment
            full_xml = f'<?xml version="1.0" encoding="utf-8"?>\n<!--{comment_text}-->\n{xml_str}'
            
            # Write to file
            with open(nfo_path, 'w', encoding='utf-8') as f:
                f.write(full_xml)
            
            print(f"✅ Successfully created/updated movie NFO: {nfo_path}")
            print(f"   IMDb ID: {imdb_id}, Date Added: {dateadded}, Source: {source}")
            
        except Exception as e:
            print(f"❌ Error creating/updating movie NFO {nfo_path}: {e}")
    
    def create_tvshow_nfo(self, series_dir: Path, imdb_id: str, tvdb_id: Optional[str] = None) -> None:
        """Create or update tvshow.nfo file preserving existing content"""
        nfo_path = series_dir / "tvshow.nfo"
        
        try:
            # Try to load existing NFO file
            if nfo_path.exists():
                try:
                    tree = ET.parse(nfo_path)
                    tvshow = tree.getroot()
                    
                    # Ensure root element is <tvshow>
                    if tvshow.tag != "tvshow":
                        raise ValueError("Root element is not <tvshow>")
                    
                    # Remove existing NFOGuard-managed elements to avoid duplicates
                    # These will be re-added at the bottom
                    for tag in ["lockdata"]:
                        existing = tvshow.find(tag)
                        if existing is not None:
                            tvshow.remove(existing)
                    
                    # Remove ALL existing uniqueid with type="imdb" regardless of attributes
                    for uniqueid in tvshow.findall("uniqueid[@type='imdb']"):
                        tvshow.remove(uniqueid)
                        
                except (ET.ParseError, ValueError) as e:
                    print(f"⚠️  Corrupted TV show NFO detected: {nfo_path} - {str(e)[:100]}...")
                    print(f"   Creating new clean tvshow.nfo file to replace corrupted one")
                    tvshow = ET.Element("tvshow")
            else:
                # Create new NFO structure
                tvshow = ET.Element("tvshow")
            
            # Add NFOGuard fields at the bottom
            
            # Add IMDb uniqueid at the end
            imdb_uniqueid = ET.SubElement(tvshow, "uniqueid", type="imdb", default="true")
            imdb_uniqueid.text = imdb_id
            
            # Add TVDB ID if available (preserve existing or add new)
            if tvdb_id and not tvshow.find("uniqueid[@type='tvdb']"):
                tvdb_uniqueid = ET.SubElement(tvshow, "uniqueid", type="tvdb")
                tvdb_uniqueid.text = tvdb_id
            
            # Add lockdata at the very end
            lockdata = ET.SubElement(tvshow, "lockdata")
            lockdata.text = "true"
            
            # Add NFOGuard comment at the beginning
            comment_text = f" Created by {self.manager_brand} "
            
            # Write file with proper formatting
            tree = ET.ElementTree(tvshow)
            ET.indent(tree, space="  ", level=0)
            
            # Write to string first to add comment
            xml_str = ET.tostring(tvshow, encoding='unicode')
            
            # Add XML declaration and comment
            full_xml = f'<?xml version="1.0" encoding="utf-8"?>\n<!--{comment_text}-->\n{xml_str}'
            
            # Write to file
            with open(nfo_path, 'w', encoding='utf-8') as f:
                f.write(full_xml)
            
            print(f"✅ Successfully created/updated TV show NFO: {nfo_path}")
            print(f"   IMDb ID: {imdb_id}" + (f", TVDB ID: {tvdb_id}" if tvdb_id else ""))
            
        except Exception as e:
            print(f"❌ Error creating/updating tvshow NFO {nfo_path}: {e}")
    
    def create_season_nfo(self, season_dir: Path, season_number: int) -> None:
        """Create or update season.nfo file preserving existing content"""
        nfo_path = season_dir / "season.nfo"
        
        try:
            season_dir.mkdir(exist_ok=True)
            
            # Try to load existing NFO file
            if nfo_path.exists():
                try:
                    tree = ET.parse(nfo_path)
                    season = tree.getroot()
                    
                    # Ensure root element is <season>
                    if season.tag != "season":
                        raise ValueError("Root element is not <season>")
                    
                    # Remove existing NFOGuard-managed elements
                    for tag in ["seasonnumber", "lockdata"]:
                        existing = season.find(tag)
                        if existing is not None:
                            season.remove(existing)
                        
                except (ET.ParseError, ValueError) as e:
                    print(f"⚠️  Corrupted season NFO detected: {nfo_path} - {str(e)[:100]}...")
                    print(f"   Creating new clean season.nfo file to replace corrupted one")
                    season = ET.Element("season")
            else:
                # Create new NFO structure
                season = ET.Element("season")
            
            # Add NFOGuard fields at the bottom
            seasonnumber = ET.SubElement(season, "seasonnumber")
            seasonnumber.text = str(season_number)
            
            # Add lockdata at the end
            lockdata = ET.SubElement(season, "lockdata")
            lockdata.text = "true"
            
            # Add NFOGuard comment at the beginning
            comment_text = f" Created by {self.manager_brand} "
            
            # Write file with proper formatting
            tree = ET.ElementTree(season)
            ET.indent(tree, space="  ", level=0)
            
            # Write to string first to add comment
            xml_str = ET.tostring(season, encoding='unicode')
            
            # Add XML declaration and comment
            full_xml = f'<?xml version="1.0" encoding="utf-8"?>\n<!--{comment_text}-->\n{xml_str}'
            
            # Write to file
            with open(nfo_path, 'w', encoding='utf-8') as f:
                f.write(full_xml)
            
            print(f"✅ Successfully created/updated season NFO: {nfo_path}")
            print(f"   Season: {season_number}")
            
        except Exception as e:
            print(f"❌ Error creating/updating season NFO {nfo_path}: {e}")
    
    def find_existing_episode_nfo(self, season_dir: Path, season_num: int, episode_num: int) -> Optional[Path]:
        """Find existing episode NFO file that matches season/episode but isn't standardized name"""
        if not season_dir.exists():
            return None
            
        # Standard filename pattern we're looking to create
        standard_pattern = f"S{season_num:02d}E{episode_num:02d}.nfo"
        
        # Look for NFO files in the season directory
        for nfo_file in season_dir.glob("*.nfo"):
            # Skip if it's already the standard format
            if nfo_file.name == standard_pattern:
                continue
                
            # Check if this NFO contains the right season/episode
            try:
                tree = ET.parse(nfo_file)
                root = tree.getroot()
                
                if root.tag == "episodedetails":
                    # Check for season/episode elements
                    season_elem = root.find("season")
                    episode_elem = root.find("episode")
                    
                    if (season_elem is not None and episode_elem is not None and
                        season_elem.text and episode_elem.text):
                        try:
                            file_season = int(season_elem.text)
                            file_episode = int(episode_elem.text)
                            
                            if file_season == season_num and file_episode == episode_num:
                                print(f"🔍 Found existing episode NFO: {nfo_file.name} -> will migrate to {standard_pattern}")
                                return nfo_file
                        except ValueError:
                            continue
                            
            except (ET.ParseError, Exception):
                # Skip corrupted or non-XML files
                continue
                
        return None
    
    def create_episode_nfo(self, season_dir: Path, season_num: int, episode_num: int,
                          aired: Optional[str], dateadded: Optional[str], source: str,
                          lock_metadata: bool = True, enhanced_metadata: Optional[Dict[str, Any]] = None) -> None:
        """Create or update episode NFO file preserving existing content"""
        # Generate episode filename pattern
        episode_filename = f"S{season_num:02d}E{episode_num:02d}.nfo"
        nfo_path = season_dir / episode_filename
        
        # Track if we need to delete an old long-named NFO file
        old_nfo_to_delete = None
        
        try:
            # First, check for existing long-named NFO files that need migration
            existing_long_nfo = self.find_existing_episode_nfo(season_dir, season_num, episode_num)
            
            # Try to load existing NFO file (either standard or long-named)
            source_nfo_path = nfo_path if nfo_path.exists() else existing_long_nfo
            
            if source_nfo_path:
                try:
                    tree = ET.parse(source_nfo_path)
                    episode = tree.getroot()
                    
                    # Ensure root element is <episodedetails>
                    if episode.tag != "episodedetails":
                        raise ValueError("Root element is not <episodedetails>")
                    
                    # If we're migrating from a long-named file, mark it for deletion
                    if existing_long_nfo and source_nfo_path == existing_long_nfo:
                        old_nfo_to_delete = existing_long_nfo
                        print(f"📦 Migrating episode NFO: {existing_long_nfo.name} -> {episode_filename}")
                    
                    # Remove existing NFOGuard-managed elements to avoid duplicates
                    # These will be re-added at the bottom
                    nfoguard_fields = ["aired", "dateadded", "lockdata", "season", "episode"]
                    for tag in nfoguard_fields:
                        existing = episode.find(tag)
                        if existing is not None:
                            # Store the aired value before removing
                            if tag == "aired" and not aired:
                                aired = existing.text  # Preserve existing aired date
                            episode.remove(existing)
                        
                except (ET.ParseError, ValueError) as e:
                    print(f"⚠️  Corrupted episode NFO detected: {nfo_path} - {str(e)[:100]}...")
                    print(f"   Creating new clean episode NFO file to replace corrupted one")
                    episode = ET.Element("episodedetails")
            else:
                # Create new NFO structure
                episode = ET.Element("episodedetails")
            
            # Enhanced metadata should be preserved - only add if not already present
            if enhanced_metadata:
                if enhanced_metadata.get("title") and not episode.find("title"):
                    title_elem = ET.SubElement(episode, "title")
                    title_elem.text = enhanced_metadata["title"]
                
                if enhanced_metadata.get("overview") and not episode.find("plot"):
                    plot_elem = ET.SubElement(episode, "plot")
                    plot_elem.text = enhanced_metadata["overview"]
                
                if enhanced_metadata.get("runtime") and not episode.find("runtime"):
                    runtime_elem = ET.SubElement(episode, "runtime")
                    runtime_elem.text = str(enhanced_metadata["runtime"])
            
            # Add NFOGuard fields at the bottom
            
            # Basic episode info at the end
            season_elem = ET.SubElement(episode, "season")
            season_elem.text = str(season_num)
            
            episode_elem = ET.SubElement(episode, "episode")
            episode_elem.text = str(episode_num)
            
            # Dates at the end
            if aired:
                aired_elem = ET.SubElement(episode, "aired")
                aired_elem.text = aired[:10] if len(aired) >= 10 else aired
            
            if dateadded:
                dateadded_elem = ET.SubElement(episode, "dateadded")
                dateadded_elem.text = dateadded
            
            # Add lockdata at the very end
            if lock_metadata:
                lockdata = ET.SubElement(episode, "lockdata")
                lockdata.text = "true"
            
            # Add NFOGuard comment at the beginning
            comment_text = f" Created by {self.manager_brand} - Source: {source} "
            
            # Write file with proper formatting
            tree = ET.ElementTree(episode)
            ET.indent(tree, space="  ", level=0)
            
            # Write to string first to add comment
            xml_str = ET.tostring(episode, encoding='unicode')
            
            # Add XML declaration and comment
            full_xml = f'<?xml version="1.0" encoding="utf-8"?>\n<!--{comment_text}-->\n{xml_str}'
            
            # Write to file
            with open(nfo_path, 'w', encoding='utf-8') as f:
                f.write(full_xml)
            
            print(f"✅ Successfully created/updated episode NFO: {nfo_path}")
            print(f"   S{season_num:02d}E{episode_num:02d}, Aired: {aired}, Date Added: {dateadded}")
            
            # Clean up old long-named NFO file if we migrated from it
            if old_nfo_to_delete and old_nfo_to_delete.exists():
                try:
                    old_nfo_to_delete.unlink()
                    print(f"🗑️  Cleaned up old NFO file: {old_nfo_to_delete.name}")
                except Exception as cleanup_error:
                    print(f"⚠️  Warning: Could not delete old NFO file {old_nfo_to_delete.name}: {cleanup_error}")
            
        except Exception as e:
            print(f"❌ Error creating/updating episode NFO {nfo_path}: {e}")
    
    def set_file_mtime(self, file_path: Path, iso_timestamp: str) -> None:
        """Set file modification time to match import date"""
        try:
            # Parse ISO timestamp
            if iso_timestamp.endswith('Z'):
                dt = datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))
            elif '+' in iso_timestamp or 'T' in iso_timestamp:
                dt = datetime.fromisoformat(iso_timestamp)
            else:
                # Assume it's already a simple date
                dt = datetime.fromisoformat(iso_timestamp + 'T00:00:00')
            
            # Convert to timestamp
            timestamp = dt.timestamp()
            
            # Set both access and modification times
            os.utime(file_path, (timestamp, timestamp))
            print(f"✅ Updated file timestamp: {file_path.name} -> {iso_timestamp}")
            
        except Exception as e:
            print(f"❌ Error setting mtime for {file_path}: {e}")
    
    def update_movie_files_mtime(self, movie_dir: Path, iso_timestamp: str) -> None:
        """Update modification times for all video files in movie directory"""
        video_exts = (".mkv", ".mp4", ".avi", ".mov", ".m4v")
        updated_files = []
        
        for file_path in movie_dir.iterdir():
            if file_path.is_file() and file_path.suffix.lower() in video_exts:
                self.set_file_mtime(file_path, iso_timestamp)
                updated_files.append(file_path.name)
        
        if updated_files:
            print(f"✅ Updated {len(updated_files)} video file timestamps in {movie_dir.name}")
        else:
            print(f"⚠️  No video files found to update in {movie_dir.name}")