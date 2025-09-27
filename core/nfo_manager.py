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
        print(f"🔍 Searching for IMDb ID in: {movie_dir.name}")
        
        # First try directory name
        imdb_id = self.parse_imdb_from_path(movie_dir)
        if imdb_id:
            print(f"✅ Found IMDb ID in directory name: {imdb_id}")
            return imdb_id
            
        # Try all files in the directory for IMDb ID patterns
        for file_path in movie_dir.iterdir():
            if file_path.is_file():
                imdb_id = self.parse_imdb_from_path(file_path)
                if imdb_id:
                    print(f"✅ Found IMDb ID in filename: {imdb_id} from {file_path.name}")
                    return imdb_id
                    
        # Finally, try NFO file content (including TMDB fallback)
        nfo_path = movie_dir / "movie.nfo"
        imdb_id = self.parse_imdb_from_nfo(nfo_path)
        if imdb_id:
            if imdb_id.startswith("tmdb-"):
                print(f"✅ Found TMDB ID in NFO file: {imdb_id} from {nfo_path} (fallback mode)")
            else:
                print(f"✅ Found IMDb ID in NFO file: {imdb_id} from {nfo_path}")
            return imdb_id
            
        print(f"❌ No IMDb or TMDB ID found in directory, filenames, or NFO for: {movie_dir.name}")
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
            lockdata_elem = root.find('.//lockdata')
            
            # Only consider it NFOGuard-managed if it has dateadded and lockdata
            if (dateadded_elem is not None and dateadded_elem.text and 
                lockdata_elem is not None and lockdata_elem.text == "true"):
                
                result = {
                    "dateadded": dateadded_elem.text.strip(),
                    "source": "nfo_file_existing"
                }
                
                if premiered_elem is not None and premiered_elem.text:
                    result["released"] = premiered_elem.text.strip()
                    
                print(f"✅ Found NFOGuard data in NFO: dateadded={result['dateadded']}, released={result.get('released', 'None')}")
                return result
                
        except (ET.ParseError, Exception) as e:
            print(f"⚠️ Error parsing NFO for NFOGuard data: {e}")
            pass
            
        return None
    
    def extract_nfoguard_dates_from_episode_nfo(self, season_path: Path, season_num: int, episode_num: int) -> Optional[Dict[str, str]]:
        """Extract NFOGuard-managed dates from existing episode NFO file"""
        nfo_filename = f"S{season_num:02d}E{episode_num:02d}.nfo"
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
            
            # Only consider it NFOGuard-managed if it has dateadded and lockdata
            if (dateadded_elem is not None and dateadded_elem.text and 
                lockdata_elem is not None and lockdata_elem.text == "true"):
                
                result = {
                    "dateadded": dateadded_elem.text.strip(),
                    "source": "episode_nfo_existing"
                }
                
                if aired_elem is not None and aired_elem.text:
                    result["aired"] = aired_elem.text.strip()
                    
                # Check if title is missing
                title_elem = root.find('.//title')
                result["has_title"] = title_elem is not None and title_elem.text and title_elem.text.strip()
                
                print(f"✅ Found NFOGuard data in episode NFO S{season_num:02d}E{episode_num:02d}: dateadded={result['dateadded']}, aired={result.get('aired', 'None')}, has_title={result['has_title']}")
                return result
                
        except (ET.ParseError, Exception) as e:
            print(f"⚠️ Error parsing episode NFO for NFOGuard data: {e}")
            pass
            
        return None
    
    def enhance_existing_episode_nfo_with_title(self, season_path: Path, season_num: int, episode_num: int, title: str) -> bool:
        """Add title to existing episode NFO file that's missing one"""
        nfo_filename = f"S{season_num:02d}E{episode_num:02d}.nfo"
        nfo_path = season_path / nfo_filename
        
        if not nfo_path.exists():
            return False
            
        try:
            tree = ET.parse(nfo_path)
            root = tree.getroot()
            
            # Check if title already exists
            existing_title = root.find('.//title')
            if existing_title is not None and existing_title.text and existing_title.text.strip():
                return False  # Title already exists
            
            # Add title element (insert near the top, after any existing plot element)
            title_elem = ET.Element("title")
            title_elem.text = title
            
            # Find the best position to insert title (after plot if it exists, otherwise at the beginning)
            plot_elem = root.find('.//plot')
            if plot_elem is not None:
                # Insert after plot
                plot_index = list(root).index(plot_elem)
                root.insert(plot_index + 1, title_elem)
            else:
                # Insert at the beginning (after any existing title that might be empty)
                if existing_title is not None:
                    title_index = list(root).index(existing_title)
                    root.remove(existing_title)
                    root.insert(title_index, title_elem)
                else:
                    root.insert(0, title_elem)
            
            # Write the updated NFO
            tree.write(nfo_path, encoding='utf-8', xml_declaration=True)
            print(f"📝 Enhanced existing NFO with title: S{season_num:02d}E{episode_num:02d} - '{title}'")
            return True
            
        except Exception as e:
            print(f"⚠️ Error enhancing NFO with title: {e}")
            return False
    
    def _extract_title_from_filename(self, season_dir: Path, season_num: int, episode_num: int) -> Optional[str]:
        """Extract episode title from video filename as fallback when metadata doesn't provide it"""
        try:
            import re
            # Look for video files matching this episode
            season_pattern = f"S{season_num:02d}E{episode_num:02d}"
            print(f"🔍 Searching for title in files for {season_pattern} in directory: {season_dir}")
            
            for video_file in season_dir.glob("*.mkv"):
                filename = video_file.name
                print(f"🔍 Checking file: {filename}")
                if season_pattern in filename:
                    print(f"✅ Found matching file: {filename}")
                    # Pattern: SeriesName-S01E01-Episode Title[WEBDL-1080p][AAC2.0][h264].mkv
                    # Extract the part between season/episode and the first bracket
                    pattern = rf'{season_pattern}-(.*?)\['
                    print(f"🔍 Using regex pattern: {pattern}")
                    match = re.search(pattern, filename)
                    if match:
                        title = match.group(1).strip()
                        print(f"🔍 Raw extracted title: '{title}'")
                        # Clean up common encoding artifacts and separators
                        title = title.replace('-', ' ').strip()
                        if title:
                            print(f"✅ Extracted title from filename: '{title}' for {season_pattern}")
                            return title
                        else:
                            print(f"⚠️ Title was empty after cleanup")
                    else:
                        print(f"⚠️ Regex pattern didn't match filename")
            
            # Also check .mp4 files
            for video_file in season_dir.glob("*.mp4"):
                filename = video_file.name
                print(f"🔍 Checking .mp4 file: {filename}")
                if season_pattern in filename:
                    print(f"✅ Found matching .mp4 file: {filename}")
                    pattern = rf'{season_pattern}-(.*?)\['
                    match = re.search(pattern, filename)
                    if match:
                        title = match.group(1).strip()
                        print(f"🔍 Raw extracted title from .mp4: '{title}'")
                        title = title.replace('-', ' ').strip()
                        if title:
                            print(f"✅ Extracted title from .mp4 filename: '{title}' for {season_pattern}")
                            return title
                            
        except Exception as e:
            print(f"⚠️ Error extracting title from filename for S{season_num:02d}E{episode_num:02d}: {e}")
        
        print(f"⚠️ No title found in filenames for {season_pattern}")
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
        
        print(f"🔍 create_movie_nfo called: imdb_id={imdb_id}, dateadded={dateadded}, released={released}, source={source}")
        
        try:
            # Try to load existing NFO file
            if nfo_path.exists():
                try:
                    # Try to parse the XML, handling URLs appended after </movie>
                    movie = self._parse_nfo_with_tolerance(nfo_path)
                    
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
                                print(f"🔍 Preserving existing premiered date: {existing.text} (released was None)")
                                released = existing.text  # Preserve existing premiered date
                            elif tag == "premiered":
                                print(f"🔍 NOT preserving premiered date: existing={existing.text}, released={released}")
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
            
            # Now append ALL NFOGuard fields at the VERY END of the file
            # Create elements and explicitly append them to ensure they're at the bottom
            
            # Add NFOGuard comment marker as the first of our additions
            nfoguard_comment = ET.Comment(f" NFOGuard - Source: {source} ")
            movie.append(nfoguard_comment)
            
            # Add IMDb uniqueid at the end (after all existing content)
            uniqueid = ET.Element("uniqueid", type="imdb", default="true")
            uniqueid.text = imdb_id
            movie.append(uniqueid)
            
            # Add premiered date at the bottom if we have it
            if released:
                premiered_elem = ET.Element("premiered")
                premiered_elem.text = released[:10] if len(released) >= 10 else released
                movie.append(premiered_elem)
                
                # Extract year from premiered date for consistency
                try:
                    year_value = released[:4] if len(released) >= 4 else None
                    if year_value and year_value.isdigit():
                        year_elem = ET.Element("year")
                        year_elem.text = year_value
                        movie.append(year_elem)
                except:
                    pass  # Skip year if we can't extract it
            
            # Add dateadded at the end - THIS IS CRITICAL FOR EMBY PLUGIN
            if dateadded:
                dateadded_elem = ET.Element("dateadded")
                dateadded_elem.text = dateadded
                movie.append(dateadded_elem)
                print(f"✅ Added dateadded to NFO: {dateadded}")
            
            # Add lockdata at the very end
            if lock_metadata:
                lockdata = ET.Element("lockdata")
                lockdata.text = "true"
                movie.append(lockdata)
            
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
            
            # Prioritize long-named file for migration, otherwise use standard file
            source_nfo_path = existing_long_nfo if existing_long_nfo else nfo_path if nfo_path.exists() else None
            
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
                        
                        # Show what content fields are being preserved
                        content_fields = ["title", "plot", "runtime", "premiered"]
                        preserved_content = []
                        for field in content_fields:
                            elem = episode.find(field)
                            if elem is not None and elem.text:
                                preserved_content.append(field)
                        if preserved_content:
                            print(f"   📄 Preserving content: {', '.join(preserved_content)}")
                    
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
                            print(f"   ⚠️  No content fields found after cleanup!")
                        
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
            
            # Fallback: Extract title from filename if no valid title present
            title_elem = episode.find("title")
            has_valid_title = title_elem is not None and title_elem.text and title_elem.text.strip()
            
            print(f"🔍 Title check for S{season_num:02d}E{episode_num:02d}: has_element={title_elem is not None}, has_text={title_elem.text if title_elem is not None else 'N/A'}, has_valid_title={has_valid_title}")
            
            if not has_valid_title:
                print(f"🔍 No valid title found in NFO for S{season_num:02d}E{episode_num:02d}, attempting filename extraction")
                filename_title = self._extract_title_from_filename(season_dir, season_num, episode_num)
                if filename_title:
                    # Remove existing empty/invalid title element if present
                    if title_elem is not None:
                        episode.remove(title_elem)
                    
                    # Add new title element
                    title_elem = ET.SubElement(episode, "title")
                    title_elem.text = filename_title
                    print(f"✅ Added filename-extracted title to NFO: S{season_num:02d}E{episode_num:02d} - '{filename_title}'")
                else:
                    print(f"⚠️ Could not extract title from filename for S{season_num:02d}E{episode_num:02d}")
            else:
                print(f"✅ Valid title already exists in NFO for S{season_num:02d}E{episode_num:02d}: '{title_elem.text.strip()}')")
            
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
            
            # Add NFOGuard comment at the bottom with other NFOGuard fields
            nfoguard_comment = ET.Comment(f" Created by {self.manager_brand} - Source: {source} ")
            episode.append(nfoguard_comment)
            
            # Write file with proper formatting
            tree = ET.ElementTree(episode)
            ET.indent(tree, space="  ", level=0)
            
            # Write to file normally (ET will handle the comment properly)
            tree.write(nfo_path, encoding='utf-8', xml_declaration=True)
            
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

