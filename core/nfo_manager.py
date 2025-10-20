#!/usr/bin/env python3
"""
NFO Manager for creating and managing metadata files
Handles NFO creation for movies, TV shows, seasons, and episodes
"""
import os
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, Tuple
import re


class NFOManager:
    """Manages NFO file creation and updates"""
    
    def __init__(self, manager_brand: str = "NFOGuard", debug: bool = False):
        self.manager_brand = manager_brand
        self.debug = debug
    
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
    
    def parse_imdb_from_nfo(self, nfo_path: Path) -> Optional[str]:
        """Extract IMDb ID from NFO file content"""
        if not nfo_path.exists():
            return None
            
        try:
            root = self._parse_nfo_with_tolerance(nfo_path)
            
            # Check for <uniqueid type="imdb">ttXXXXXX</uniqueid>
            imdb_uniqueid = root.find('.//uniqueid[@type="imdb"]')
            if imdb_uniqueid is not None and imdb_uniqueid.text:
                imdb_id = imdb_uniqueid.text.strip()
                if imdb_id.startswith('tt'):
                    return imdb_id
                    
            # Check for legacy <imdbid>ttXXXXXX</imdbid>
            imdbid_elem = root.find('.//imdbid')
            if imdbid_elem is not None and imdbid_elem.text:
                imdb_id = imdbid_elem.text.strip()
                if imdb_id.startswith('tt'):
                    return imdb_id
                    
            # Check for legacy <imdb>ttXXXXXX</imdb>
            imdb_elem = root.find('.//imdb')
            if imdb_elem is not None and imdb_elem.text:
                imdb_id = imdb_elem.text.strip()
                if imdb_id.startswith('tt'):
                    return imdb_id
            
            # Last resort: Check for TMDB ID as fallback identifier
            # This handles movies that only have TMDB IDs in NFO files
            tmdb_uniqueid = root.find('.//uniqueid[@type="tmdb"]')
            if tmdb_uniqueid is not None and tmdb_uniqueid.text:
                tmdb_id = tmdb_uniqueid.text.strip()
                if tmdb_id.isdigit():
                    print(f"⚠️ Found TMDB ID {tmdb_id} but no IMDb ID - using TMDB ID as fallback")
                    # Return TMDB ID with prefix to distinguish from IMDb IDs
                    return f"tmdb-{tmdb_id}"
                    
        except (ET.ParseError, Exception):
            # Skip corrupted or non-XML files
            pass
            
        return None
    
    def find_movie_imdb_id(self, movie_dir: Path) -> Optional[str]:
        """Find IMDb ID from directory name, filenames, or NFO file"""
        # First try directory name
        imdb_id = self.parse_imdb_from_path(movie_dir)
        if imdb_id:
            return imdb_id
            
        # Try all files in the directory for IMDb ID patterns
        for file_path in movie_dir.iterdir():
            if file_path.is_file():
                imdb_id = self.parse_imdb_from_path(file_path)
                if imdb_id:
                    return imdb_id
                    
        # Finally, try NFO file content (including TMDB fallback)
        nfo_path = movie_dir / "movie.nfo"
        imdb_id = self.parse_imdb_from_nfo(nfo_path)
        if imdb_id:
            return imdb_id
            
        return None
    
    def find_series_imdb_id(self, series_dir: Path) -> Optional[str]:
        """Find IMDb ID from TV series directory name, filenames, or tvshow.nfo file"""
        # First try directory name
        imdb_id = self.parse_imdb_from_path(series_dir)
        if imdb_id:
            return imdb_id
            
        # Try all files in the directory for IMDb ID patterns
        for file_path in series_dir.iterdir():
            if file_path.is_file():
                imdb_id = self.parse_imdb_from_path(file_path)
                if imdb_id:
                    return imdb_id
                    
        # Finally, try tvshow.nfo file content
        nfo_path = series_dir / "tvshow.nfo"
        imdb_id = self.parse_imdb_from_nfo(nfo_path)
        if imdb_id:
            return imdb_id
            
        return None
    
    def extract_nfoguard_dates_from_nfo(self, nfo_path: Path) -> Optional[Dict[str, str]]:
        """Extract NFOGuard-managed dates from existing NFO file"""
        if not nfo_path.exists():
            return None
            
        try:
            tree = ET.parse(nfo_path)
            root = tree.getroot()
            
            # Look for NFOGuard fields
            dateadded_elem = root.find('.//dateadded')
            premiered_elem = root.find('.//premiered')
            aired_elem = root.find('.//aired')  # For TV episodes
            lockdata_elem = root.find('.//lockdata')
            
            # Consider it NFOGuard-managed if it has lockdata=true (with or without dateadded)
            if lockdata_elem is not None and lockdata_elem.text == "true":
                # Extract original source from NFOGuard comment, default to nfo_file_existing
                source = "nfo_file_existing"
                
                # Parse XML content to find NFOGuard comment with source
                nfo_content = nfo_path.read_text(encoding='utf-8')
                import re
                source_match = re.search(r'<!--\s*NFOGuard\s*-\s*Source:\s*([^-]+?)\s*-->', nfo_content)
                if source_match:
                    source = source_match.group(1).strip()
                    print(f"🔍 Extracted original source from NFO comment: {source}")
                
                result = {
                    "source": source
                }
                
                if dateadded_elem is not None and dateadded_elem.text:
                    result["dateadded"] = dateadded_elem.text.strip()
                
                if premiered_elem is not None and premiered_elem.text:
                    result["released"] = premiered_elem.text.strip()
                
                if aired_elem is not None and aired_elem.text:
                    result["aired"] = aired_elem.text.strip()
                    
                print(f"✅ Found NFOGuard data in NFO: dateadded={result.get('dateadded', 'None')}, source={source}, released={result.get('released', 'None')}, aired={result.get('aired', 'None')}")
                return result
                
        except (ET.ParseError, Exception) as e:
            print(f"⚠️ Error parsing NFO for NFOGuard data: {e}")
            pass
            
        return None
    
    def extract_nfoguard_dates_from_episode_nfo(self, season_path: Path, season_num: int, episode_num: int) -> Optional[Dict[str, str]]:
        """Extract NFOGuard-managed dates from existing episode NFO file"""
        nfo_filename = f"s{season_num:02d}e{episode_num:02d}.nfo"
        nfo_path = season_path / nfo_filename
        
        if not nfo_path.exists():
            return None
            
        try:
            tree = ET.parse(nfo_path)
            root = tree.getroot()
            
            # Look for NFOGuard fields in episode NFO
            dateadded_elem = root.find('.//dateadded')
            aired_elem = root.find('.//aired')
            lockdata_elem = root.find('.//lockdata')
            
            # Consider it NFOGuard-managed if it has lockdata=true (with or without dateadded)
            if lockdata_elem is not None and lockdata_elem.text == "true":
                # Extract original source from NFOGuard comment, default to episode_nfo_existing
                source = "episode_nfo_existing"
                
                # Parse XML content to find NFOGuard comment with source
                nfo_content = nfo_path.read_text(encoding='utf-8')
                import re
                source_match = re.search(r'<!--\s*NFOGuard\s*-\s*Source:\s*([^-]+?)\s*-->', nfo_content)
                if source_match:
                    source = source_match.group(1).strip()
                    print(f"🔍 Extracted original source from episode NFO comment: {source}")
                
                result = {
                    "source": source
                }
                
                if dateadded_elem is not None and dateadded_elem.text:
                    result["dateadded"] = dateadded_elem.text.strip()
                
                if aired_elem is not None and aired_elem.text:
                    result["aired"] = aired_elem.text.strip()
                    
                print(f"✅ Found NFOGuard data in episode NFO S{season_num:02d}E{episode_num:02d}: dateadded={result.get('dateadded', 'None')}, source={source}, aired={result.get('aired', 'None')}")
                return result
                
        except (ET.ParseError, Exception) as e:
            print(f"⚠️ Error parsing episode NFO for NFOGuard data: {e}")
            pass
            
        return None
    
    def _parse_nfo_with_tolerance(self, nfo_path: Path):
        """Parse NFO file with tolerance for URLs appended after XML"""
        try:
            # First try normal parsing
            tree = ET.parse(nfo_path)
            return tree.getroot()
        except ET.ParseError as e:
            # If parsing fails, try to extract just the XML part
            try:
                with open(nfo_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Find the last </movie> tag and truncate after it
                last_movie_end = content.rfind('</movie>')
                if last_movie_end != -1:
                    xml_content = content[:last_movie_end + 8]  # +8 for </movie>
                    
                    # Try parsing the truncated content
                    root = ET.fromstring(xml_content)
                    print(f"✅ Successfully parsed NFO after removing trailing content: {nfo_path.name}")
                    return root
                else:
                    # Re-raise original error if we can't find </movie>
                    raise e
            except Exception:
                # Re-raise original error if our fix attempt fails
                raise e
    
    def create_movie_nfo(self, movie_dir: Path, imdb_id: str, dateadded: str, 
                        released: Optional[str] = None, source: str = "unknown",
                        lock_metadata: bool = True) -> None:
        """Create or update movie.nfo file preserving existing content"""
        nfo_path = movie_dir / "movie.nfo"
        
        # Debug output only if DEBUG=true in environment
        import os
        if os.environ.get("DEBUG", "false").lower() == "true":
            print(f"🔍 create_movie_nfo called: imdb_id={imdb_id}, dateadded={dateadded}, released={released}, source={source}")
            print(f"🔍 NFO path: {nfo_path}")
            print(f"🔍 NFO exists: {nfo_path.exists()}")
        
        try:
            # Try to load existing NFO file
            if nfo_path.exists():
                try:
                    # Try to parse the XML, handling URLs appended after </movie>
                    movie = self._parse_nfo_with_tolerance(nfo_path)
                    
                    # Ensure root element is <movie>
                    if movie.tag != "movie":
                        raise ValueError("Root element is not <movie>")
                    
                    # Only remove elements that are clearly NFOGuard-managed
                    # Look for elements that have NFOGuard characteristics
                    elements_to_remove = []
                    
                    # Remove lockdata=true (this is definitely NFOGuard)
                    for lockdata in movie.findall("lockdata"):
                        if lockdata.text == "true":
                            elements_to_remove.append(lockdata)
                    
                    # Remove IMDb uniqueids that we manage
                    for uniqueid in movie.findall("uniqueid[@type='imdb']"):
                        elements_to_remove.append(uniqueid)
                    
                    # For dateadded/premiered/year, only remove if they appear to be NFOGuard-managed
                    # (i.e., if lockdata=true exists, these are likely ours)
                    has_nfoguard_lockdata = any(ld.text == "true" for ld in movie.findall("lockdata"))
                    
                    if has_nfoguard_lockdata:
                        # This NFO was managed by NFOGuard, safe to remove our fields
                        for tag in ["dateadded", "premiered", "year"]:
                            existing = movie.find(tag)
                            if existing is not None:
                                # Store the value before removing (for premiered/year)
                                if tag == "premiered" and not released:
                                    print(f"🔍 Preserving existing premiered date: {existing.text}")
                                    released = existing.text
                                elements_to_remove.append(existing)
                    else:
                        # No NFOGuard lockdata found, be more conservative
                        # Only remove dateadded if it looks like NFOGuard format (ISO timestamp)
                        dateadded_elem = movie.find("dateadded")
                        if dateadded_elem is not None and dateadded_elem.text:
                            # NFOGuard uses ISO format like "2025-10-12 16:26:02"
                            if re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', dateadded_elem.text.strip()):
                                elements_to_remove.append(dateadded_elem)
                    
                    # Remove all identified elements
                    for elem in elements_to_remove:
                        movie.remove(elem)
                        
                except (ET.ParseError, ValueError) as e:
                    print(f"⚠️  Corrupted NFO detected: {nfo_path} - {str(e)[:100]}...")
                    print(f"   Creating new clean NFO file to replace corrupted one")
                    movie = ET.Element("movie")
            else:
                # Create new NFO structure
                movie = ET.Element("movie")
            
            # Create all NFOGuard elements first, then append them in correct order
            # This ensures they appear as a group at the bottom of the file
            nfoguard_elements = []
            
            # Add NFOGuard comment marker as the first of our additions
            nfoguard_comment = ET.Comment(f" NFOGuard - Source: {source} ")
            nfoguard_elements.append(nfoguard_comment)
            
            # Add IMDb uniqueid 
            uniqueid = ET.Element("uniqueid", type="imdb", default="true")
            uniqueid.text = imdb_id
            nfoguard_elements.append(uniqueid)
            
            # Add premiered date if we have it
            if released:
                premiered_elem = ET.Element("premiered")
                premiered_elem.text = released[:10] if len(released) >= 10 else released
                nfoguard_elements.append(premiered_elem)
                
                # Extract year from premiered date for consistency
                try:
                    year_value = released[:4] if len(released) >= 4 else None
                    if year_value and year_value.isdigit():
                        year_elem = ET.Element("year")
                        year_elem.text = year_value
                        nfoguard_elements.append(year_elem)
                except:
                    pass  # Skip year if we can't extract it
            
            # Add dateadded - THIS IS CRITICAL FOR EMBY PLUGIN
            if os.environ.get("DEBUG", "false").lower() == "true":
                print(f"🔍 About to add dateadded: {dateadded} (type: {type(dateadded)})")
            if dateadded:
                dateadded_elem = ET.Element("dateadded")
                dateadded_elem.text = dateadded
                nfoguard_elements.append(dateadded_elem)
                print(f"✅ Adding dateadded to NFO: {dateadded}")
            else:
                print(f"❌ dateadded is empty/None, not adding to NFO")
            
            # Add lockdata at the very end
            if lock_metadata:
                lockdata = ET.Element("lockdata")
                lockdata.text = "true"
                nfoguard_elements.append(lockdata)
            
            # Now append all NFOGuard elements to the movie in one batch
            # This ensures they appear as a contiguous block at the bottom
            for elem in nfoguard_elements:
                movie.append(elem)
            
            print(f"✅ Added {len(nfoguard_elements)} NFOGuard elements to bottom of NFO")
            
            # Write file with proper formatting
            tree = ET.ElementTree(movie)
            ET.indent(tree, space="  ", level=0)
            
            # Write directly to file (comment is already embedded in XML)
            with open(nfo_path, 'w', encoding='utf-8') as f:
                f.write('<?xml version="1.0" encoding="utf-8"?>\n')
                tree.write(f, encoding='unicode', xml_declaration=False)
            
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
        """Find any existing episode NFO file that matches season/episode"""
        if not season_dir.exists():
            return None
        
        # Look for NFO files in the season directory
        for nfo_file in season_dir.glob("*.nfo"):
                
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
                                print(f"🔍 Found existing episode NFO: {nfo_file.name}")
                                return nfo_file
                        except ValueError:
                            continue
                            
            except (ET.ParseError, Exception):
                # Skip corrupted or non-XML files
                continue
                
        return None
    
    def _get_target_episode_nfo_name(self, season_dir: Path, season_num: int, episode_num: int) -> str:
        """Get target NFO filename - prefer existing NFO, then matching video file, fallback to short name"""
        if not season_dir.exists():
            return f"S{season_num:02d}E{episode_num:02d}.nfo"
        
        # First check if an NFO already exists for this episode
        existing_nfo = self.find_existing_episode_nfo(season_dir, season_num, episode_num)
        if existing_nfo:
            print(f"📂 Existing NFO found, will preserve filename: {existing_nfo.name}")
            return existing_nfo.name
        
        # Look for video files with matching season/episode
        video_extensions = [".mkv", ".mp4", ".avi", ".mov", ".m4v", ".wmv", ".flv", ".webm"]
        
        for video_file in season_dir.iterdir():
            if (video_file.is_file() and 
                video_file.suffix.lower() in video_extensions):
                
                # Parse episode info from video filename
                episode_info = self._parse_episode_from_filename(video_file.name)
                if episode_info and episode_info == (season_num, episode_num):
                    # Found matching video file - use its name for NFO
                    target_nfo_name = f"{video_file.stem}.nfo"
                    print(f"🎯 Target NFO will match video: {target_nfo_name}")
                    return target_nfo_name
        
        # Fallback to short name if no matching video found
        short_name = f"S{season_num:02d}E{episode_num:02d}.nfo"
        print(f"⚠️  No matching video file found, using short name: {short_name}")
        return short_name
    
    def _parse_episode_from_filename(self, filename: str) -> Optional[Tuple[int, int]]:
        """Parse season and episode numbers from filename"""
        # Try S##E## format first
        match = re.search(r'[Ss](\d{1,2})[Ee](\d{1,2})', filename)
        if match:
            return int(match.group(1)), int(match.group(2))
        
        # Try ##x## format
        match = re.search(r'(\d{1,2})x(\d{1,2})', filename)
        if match:
            return int(match.group(1)), int(match.group(2))
        
        return None
    
    def create_episode_nfo(self, season_dir: Path, season_num: int, episode_num: int,
                          aired: Optional[str], dateadded: Optional[str], source: str,
                          lock_metadata: bool = True, enhanced_metadata: Optional[Dict[str, Any]] = None) -> None:
        """Create or update episode NFO file preserving existing content"""
        # Get target NFO filename (prefer long name matching video file)
        target_nfo_name = self._get_target_episode_nfo_name(season_dir, season_num, episode_num)
        nfo_path = season_dir / target_nfo_name
        
        try:
            # Check for existing NFO file at target location
            source_nfo_path = nfo_path if nfo_path.exists() else None
            
            if source_nfo_path:
                try:
                    tree = ET.parse(source_nfo_path)
                    episode = tree.getroot()
                    
                    # Ensure root element is <episodedetails>
                    if episode.tag != "episodedetails":
                        raise ValueError("Root element is not <episodedetails>")
                    
                    print(f"📝 Updating existing NFO: {nfo_path.name}")
                    
                    # Preserve existing content fields and store NFOGuard-managed fields for re-adding
                    preserved_values = {}
                    
                    # Extract and preserve NFOGuard-managed fields (we'll re-add these at the bottom)
                    nfoguard_fields = ["aired", "dateadded", "lockdata", "season", "episode"]
                    for tag in nfoguard_fields:
                        existing = episode.find(tag)
                        if existing is not None:
                            # Store the value before removing
                            if tag == "aired" and not aired:
                                aired = existing.text  # Preserve existing aired date
                            preserved_values[tag] = existing.text
                            episode.remove(existing)
                    
                    # Important: DO NOT remove content fields like title, plot, runtime, premiered, etc.
                    # These should be preserved from the long-named NFO files
                    
                    # Debug: Show what fields are preserved after removing NFOGuard fields (if DEBUG enabled)
                    if self.debug:
                        preserved_fields = [elem.tag for elem in episode]
                        if preserved_fields:
                            print(f"   🔍 Content preserved after cleanup: {', '.join(preserved_fields)}")
                        else:
                            print(f"   ℹ️  NFO contains only NFOGuard metadata (no additional content fields)")
                        
                except (ET.ParseError, ValueError) as e:
                    print(f"⚠️  Corrupted episode NFO detected: {nfo_path} - {str(e)[:100]}...")
                    print(f"   Creating new clean episode NFO file to replace corrupted one")
                    episode = ET.Element("episodedetails")
            else:
                # Create new NFO structure
                episode = ET.Element("episodedetails")
            
            # Add enhanced metadata only if not already present (preserve existing from long-named NFO)
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
                # Convert datetime objects to strings
                aired_str = str(aired)
                aired_elem.text = aired_str[:10] if len(aired_str) >= 10 else aired_str
            
            if dateadded:
                dateadded_elem = ET.SubElement(episode, "dateadded")
                # Convert datetime objects to strings
                dateadded_elem.text = str(dateadded)
            
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
            
            # NFO file created/updated successfully
            pass
        except Exception as e:
            print(f"❌ Error creating/updating episode NFO {nfo_path}: {e}")
    
    def set_file_mtime(self, file_path: Path, iso_timestamp) -> None:
        """Set file modification time to match import date"""
        try:
            # Convert datetime objects to strings first
            if hasattr(iso_timestamp, 'isoformat'):
                iso_timestamp = iso_timestamp.isoformat()
            elif not isinstance(iso_timestamp, str):
                iso_timestamp = str(iso_timestamp)
            
            # Parse ISO timestamp
            if iso_timestamp.endswith('Z'):
                dt = datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))
            elif '+' in iso_timestamp or 'T' in iso_timestamp:
                dt = datetime.fromisoformat(iso_timestamp)
            elif ' ' in iso_timestamp:
                # Handle space-separated datetime format (e.g., "2025-10-16 20:31:22")
                dt = datetime.fromisoformat(iso_timestamp.replace(' ', 'T'))
            else:
                # Assume it's a simple date (e.g., "2025-10-16")
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