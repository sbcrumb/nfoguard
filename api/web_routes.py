"""
Web interface API routes for NFOGuard database management
Provides endpoints for the web-based database manipulation interface
"""
import json
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from fastapi import HTTPException, Query
from pathlib import Path

from api.models import *


def map_source_to_description(source: str) -> str:
    """Map technical source codes to user-friendly descriptions"""
    if not source or source == "no_valid_date_source":
        return "Unknown"
    
    # Handle different source patterns
    source_lower = source.lower()
    
    # TMDB sources
    if "tmdb:theatrical" in source_lower:
        return "TMDB Theatrical"
    elif "tmdb:digital" in source_lower:
        return "TMDB Digital"
    elif "tmdb:physical" in source_lower:
        return "TMDB Physical/DVD"
    elif "tmdb:" in source_lower:
        return "TMDB Release"
    
    # Radarr sources
    elif "radarr:db.history.import" in source_lower:
        return "Radarr Import History"
    elif "radarr:db.file.dateadded" in source_lower:
        return "Radarr File Date"
    elif "radarr:nfo.premiered" in source_lower:
        return "Radarr NFO"
    elif "radarr:" in source_lower:
        return "Radarr"
    
    # OMDb sources
    elif "omdb:dvd" in source_lower:
        return "OMDb DVD"
    elif "omdb:" in source_lower:
        return "OMDb Release"
    
    # Manual and other sources
    elif "manual" in source_lower:
        return "Manual Entry"
    elif "digital_release" in source_lower:
        return "Digital Release"
    elif "nfo:" in source_lower:
        return "NFO File"
    elif "webhook:" in source_lower:
        return "Webhook/API"
    
    # Fallback for unknown patterns
    return source.title()


# ---------------------------
# Database Query Endpoints
# ---------------------------

async def get_movies_list(dependencies: dict, 
                         skip: int = Query(0, ge=0),
                         limit: int = Query(100, le=1000),
                         has_date: Optional[bool] = Query(None),
                         source_filter: Optional[str] = Query(None),
                         search: Optional[str] = Query(None),
                         imdb_search: Optional[str] = Query(None)):
    """Get paginated list of movies with filtering options"""
    db = dependencies["db"]
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        
        # Build dynamic query
        where_conditions = []
        params = []
        
        if has_date is not None:
            if has_date:
                # PostgreSQL - NULL handling
                where_conditions.append("dateadded IS NOT NULL")
            else:
                # PostgreSQL - NULL handling
                where_conditions.append("dateadded IS NULL")
        
        if source_filter:
            where_conditions.append("source = %s")
            params.append(source_filter)
        
        if search:
            where_conditions.append("(imdb_id LIKE %s OR path LIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])
        
        if imdb_search:
            where_conditions.append("imdb_id LIKE %s")
            params.append(f"%{imdb_search}%")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM movies WHERE {where_clause}"
        cursor.execute(count_query, params)
        total_count = db._get_first_value(cursor.fetchone())
        
        # Get paginated results - PostgreSQL
        query = f"""
            SELECT imdb_id, path, released, dateadded, source, has_video_file, last_updated
            FROM movies 
            WHERE {where_clause}
            ORDER BY last_updated DESC
            LIMIT %s OFFSET %s
        """
        cursor.execute(query, params + [limit, skip])
        
        movies = []
        for row in cursor.fetchall():
            movie = dict(row)
            # Extract title from path for display
            try:
                movie['title'] = Path(movie['path']).name if movie['path'] else movie['imdb_id']
            except:
                movie['title'] = movie['imdb_id']
            # Map source to user-friendly description
            movie['source_description'] = map_source_to_description(movie.get('source'))
            movies.append(movie)
        
        return {
            "movies": movies,
            "total_count": total_count,
            "page": skip // limit + 1,
            "pages": (total_count + limit - 1) // limit,
            "has_next": skip + limit < total_count,
            "has_prev": skip > 0
        }


async def get_tv_series_list(dependencies: dict,
                           skip: int = Query(0, ge=0), 
                           limit: int = Query(50, le=500),
                           search: Optional[str] = Query(None),
                           imdb_search: Optional[str] = Query(None),
                           date_filter: Optional[str] = Query(None),
                           source_filter: Optional[str] = Query(None)):
    """Get paginated list of TV series with episode counts"""
    db = dependencies["db"]
    
    # Validate date_filter values
    if date_filter and date_filter not in ['complete', 'incomplete', 'none']:
        raise HTTPException(status_code=422, detail=f"Invalid date_filter: must be 'complete', 'incomplete', or 'none', got '{date_filter}'")
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        
        # Build dynamic query
        where_conditions = []
        params = []
        having_conditions = []
        
        if search:
            where_conditions.append("(s.imdb_id LIKE %s OR s.path LIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])
            
        if imdb_search:
            where_conditions.append("s.imdb_id LIKE %s")
            params.append(f"%{imdb_search}%")
            
        if source_filter:
            # Need to check episodes for source filter
            where_conditions.append("e.source = %s")
            params.append(source_filter)
            
        if date_filter:
            if date_filter == "complete":
                # All episodes have dates
                having_conditions.append("COUNT(e.episode) > 0 AND COUNT(CASE WHEN e.dateadded IS NULL THEN 1 END) = 0")
            elif date_filter == "incomplete":
                # Some episodes have dates, some don't
                having_conditions.append("COUNT(e.episode) > 0 AND COUNT(CASE WHEN e.dateadded IS NOT NULL THEN 1 END) > 0 AND COUNT(CASE WHEN e.dateadded IS NULL THEN 1 END) > 0")
            elif date_filter == "none":
                # No episodes have dates
                having_conditions.append("COUNT(e.episode) > 0 AND COUNT(CASE WHEN e.dateadded IS NOT NULL THEN 1 END) = 0")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        having_clause = " AND ".join(having_conditions) if having_conditions else ""
        
        # Get total count with same filtering logic as main query
        if having_clause:
            # When using HAVING clause, need to count filtered results
            count_query = f"""
                SELECT COUNT(*) FROM (
                    SELECT s.imdb_id
                    FROM series s
                    LEFT JOIN episodes e ON s.imdb_id = e.imdb_id
                    WHERE {where_clause}
                    GROUP BY s.imdb_id
                    HAVING {having_clause}
                ) filtered_series
            """
            cursor.execute(count_query, params)
        else:
            # Simple count when no HAVING clause
            count_query = f"SELECT COUNT(*) FROM series s WHERE {where_clause}"
            cursor.execute(count_query, params)
        total_count = db._get_first_value(cursor.fetchone())
        
        # Get series with episode statistics
        having_part = f" HAVING {having_clause}" if having_clause else ""
        # PostgreSQL query
        query = f"""
            SELECT 
                s.imdb_id, 
                s.path, 
                s.last_updated,
                COUNT(e.episode) as total_episodes,
                COUNT(CASE WHEN e.dateadded IS NOT NULL THEN 1 END) as episodes_with_dates,
                COUNT(CASE WHEN e.has_video_file = TRUE THEN 1 END) as episodes_with_video
            FROM series s
            LEFT JOIN episodes e ON s.imdb_id = e.imdb_id
            WHERE {where_clause}
            GROUP BY s.imdb_id, s.path, s.last_updated{having_part}
            ORDER BY s.last_updated DESC
            LIMIT %s OFFSET %s
        """
        cursor.execute(query, params + [limit, skip])
        
        series = []
        for row in cursor.fetchall():
            series_data = dict(row)
            # Extract title from path
            try:
                series_data['title'] = Path(series_data['path']).name if series_data['path'] else series_data['imdb_id']
            except:
                series_data['title'] = series_data['imdb_id']
            series.append(series_data)
        
        return {
            "series": series,
            "total_count": total_count,
            "page": skip // limit + 1,
            "pages": (total_count + limit - 1) // limit,
            "has_next": skip + limit < total_count,
            "has_prev": skip > 0
        }


async def get_series_sources(dependencies: dict):
    """Get unique sources from episodes table for filtering"""
    db = dependencies["db"]
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT source 
            FROM episodes 
            WHERE source IS NOT NULL AND source != '' 
            ORDER BY source
        """)
        
        rows = cursor.fetchall()
        # PostgreSQL RealDictCursor returns dict-like objects
        sources = [list(row.values())[0] for row in rows]
        return {"sources": sources}


async def debug_series_date_distribution(dependencies: dict):
    """Debug function to show TV series date distribution"""
    db = dependencies["db"]
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        
        # Get series with episode date statistics
        cursor.execute("""
            SELECT 
                s.imdb_id,
                s.path,
                COUNT(e.episode) as total_episodes,
                COUNT(CASE WHEN e.dateadded IS NOT NULL THEN 1 END) as episodes_with_dates,
                COUNT(CASE WHEN e.dateadded IS NULL THEN 1 END) as episodes_without_dates
            FROM series s
            LEFT JOIN episodes e ON s.imdb_id = e.imdb_id
            GROUP BY s.imdb_id, s.path
            HAVING COUNT(e.episode) > 0
            ORDER BY total_episodes DESC
            LIMIT 50
        """)
        
        series_stats = []
        complete_count = 0
        incomplete_count = 0 
        none_count = 0
        
        for row in cursor.fetchall():
            stats = dict(row)
            total = stats['total_episodes']
            with_dates = stats['episodes_with_dates']
            without_dates = stats['episodes_without_dates']
            
            if without_dates == 0:
                category = "complete"
                complete_count += 1
            elif with_dates == 0:
                category = "none"
                none_count += 1
            else:
                category = "incomplete"
                incomplete_count += 1
                
            stats['category'] = category
            stats['title'] = stats['path'].split('/')[-1] if stats['path'] else stats['imdb_id']
            series_stats.append(stats)
        
        return {
            "series_sample": series_stats[:20],  # First 20 for debugging
            "distribution": {
                "complete": complete_count,
                "incomplete": incomplete_count,
                "none": none_count,
                "total": complete_count + incomplete_count + none_count
            }
        }


async def get_series_episodes(dependencies: dict, imdb_id: str):
    """Get all episodes for a specific TV series"""
    db = dependencies["db"]
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        
        # Get series info - PostgreSQL
        cursor.execute("SELECT * FROM series WHERE imdb_id = %s", (imdb_id,))
        series_row = cursor.fetchone()
        if not series_row:
            raise HTTPException(status_code=404, detail="Series not found")
        
        series_info = dict(series_row)
        try:
            series_info['title'] = Path(series_info['path']).name if series_info['path'] else imdb_id
        except:
            series_info['title'] = imdb_id
        
        # Get episodes - PostgreSQL
        cursor.execute("""
            SELECT season, episode, aired, dateadded, source, has_video_file, last_updated
            FROM episodes 
            WHERE imdb_id = %s
            ORDER BY season, episode
        """, (imdb_id,))
        
        episodes = []
        for row in cursor.fetchall():
            episode = dict(row)
            # Map source to user-friendly description
            episode['source_description'] = map_source_to_description(episode.get('source'))
            episodes.append(episode)
        
        return {
            "series": series_info,
            "episodes": episodes
        }


async def get_missing_dates_report(dependencies: dict):
    """Generate report of movies and episodes missing dateadded"""
    db = dependencies["db"]
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        
        # Movies without dates - PostgreSQL
        cursor.execute("""
            SELECT imdb_id, path, released, source, last_updated
            FROM movies 
            WHERE dateadded IS NULL OR source = 'no_valid_date_source'
            ORDER BY last_updated DESC
        """)
        movies_missing = []
        for row in cursor.fetchall():
            movie = dict(row)
            try:
                movie['title'] = Path(movie['path']).name if movie['path'] else movie['imdb_id']
            except:
                movie['title'] = movie['imdb_id']
            # Map source to user-friendly description
            movie['source_description'] = map_source_to_description(movie.get('source'))
            movies_missing.append(movie)
        
        # Episodes without dates - PostgreSQL
        cursor.execute("""
            SELECT e.imdb_id, e.season, e.episode, e.aired, e.source, e.last_updated, s.path
            FROM episodes e
            JOIN series s ON e.imdb_id = s.imdb_id
            WHERE e.dateadded IS NULL OR e.source = 'no_valid_date_source'
            ORDER BY e.last_updated DESC
        """)
        episodes_missing = []
        for row in cursor.fetchall():
            episode = dict(row)
            try:
                episode['series_title'] = Path(episode['path']).name if episode['path'] else episode['imdb_id']
            except:
                episode['series_title'] = episode['imdb_id']
            # Map source to user-friendly description
            episode['source_description'] = map_source_to_description(episode.get('source'))
            episodes_missing.append(episode)
        
        # Summary statistics - PostgreSQL
        cursor.execute("SELECT COUNT(*) FROM movies WHERE dateadded IS NOT NULL")
        movies_with_dates = db._get_first_value(cursor.fetchone())
        
        cursor.execute("SELECT COUNT(*) FROM movies")
        total_movies = db._get_first_value(cursor.fetchone())
        
        cursor.execute("SELECT COUNT(*) FROM episodes WHERE dateadded IS NOT NULL")
        episodes_with_dates = db._get_first_value(cursor.fetchone())
        
        cursor.execute("SELECT COUNT(*) FROM episodes")
        total_episodes = db._get_first_value(cursor.fetchone())
        
        return {
            "summary": {
                "movies_with_dates": movies_with_dates,
                "movies_missing_dates": len(movies_missing),
                "total_movies": total_movies,
                "episodes_with_dates": episodes_with_dates,
                "episodes_missing_dates": len(episodes_missing),
                "total_episodes": total_episodes
            },
            "movies_missing": movies_missing,
            "episodes_missing": episodes_missing
        }


async def get_dashboard_stats(dependencies: dict):
    """Get comprehensive dashboard statistics"""
    db = dependencies["db"]
    
    # Get basic stats from existing method
    stats = db.get_stats()
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        
        # Enhanced statistics - PostgreSQL
        cursor.execute("SELECT COUNT(*) FROM movies WHERE dateadded IS NOT NULL")
        movies_with_dates = db._get_first_value(cursor.fetchone())
        
        cursor.execute("SELECT COUNT(*) FROM movies WHERE dateadded IS NULL OR source = 'no_valid_date_source'")
        movies_without_dates = db._get_first_value(cursor.fetchone())
        
        cursor.execute("SELECT COUNT(*) FROM episodes WHERE dateadded IS NOT NULL")
        episodes_with_dates = db._get_first_value(cursor.fetchone())
        
        cursor.execute("SELECT COUNT(*) FROM episodes WHERE dateadded IS NULL OR source = 'no_valid_date_source'")
        episodes_without_dates = db._get_first_value(cursor.fetchone())
        
        cursor.execute("SELECT COUNT(*) FROM movies WHERE source = 'no_valid_date_source'")
        movies_no_valid_source = db._get_first_value(cursor.fetchone())
        
        cursor.execute("SELECT COUNT(*) FROM episodes WHERE source = 'no_valid_date_source'")
        episodes_no_valid_source = db._get_first_value(cursor.fetchone())
        
        # Recent activity (last 7 days)
        cursor.execute("""
            SELECT COUNT(*) FROM processing_history 
            WHERE processed_at > NOW() - INTERVAL '7 days'
        """)
        recent_activity = db._get_first_value(cursor.fetchone())
        
        # Source distribution for movies
        cursor.execute("""
            SELECT source, COUNT(*) as count
            FROM movies 
            WHERE source IS NOT NULL
            GROUP BY source
            ORDER BY count DESC
        """)
        movie_sources = [{"source": list(row.values())[0], "source_description": map_source_to_description(list(row.values())[0]), "count": list(row.values())[1]} for row in cursor.fetchall()]
        
        # Source distribution for episodes
        cursor.execute("""
            SELECT source, COUNT(*) as count
            FROM episodes 
            WHERE source IS NOT NULL
            GROUP BY source
            ORDER BY count DESC
        """)
        episode_sources = [{"source": list(row.values())[0], "source_description": map_source_to_description(list(row.values())[0]), "count": list(row.values())[1]} for row in cursor.fetchall()]
        
    # Calculate total missing dates (movies + episodes)
    total_missing_dates = movies_without_dates + episodes_without_dates
    
    # Combine with enhanced stats
    stats.update({
        "movies_with_dates": movies_with_dates,
        "movies_without_dates": movies_without_dates,
        "movies_missing_dates": movies_without_dates,  # Keep for backward compatibility
        "episodes_with_dates": episodes_with_dates,
        "episodes_without_dates": episodes_without_dates,
        "episodes_missing_dates": episodes_without_dates,  # Keep for backward compatibility
        "total_missing_dates": total_missing_dates,
        "movies_no_valid_source": movies_no_valid_source,
        "episodes_no_valid_source": episodes_no_valid_source,
        "recent_activity_count": recent_activity,
        "movie_sources": movie_sources,
        "episode_sources": episode_sources
    })
    
    return stats


# ---------------------------
# Database Modification Endpoints
# ---------------------------

async def update_movie_date(dependencies: dict, imdb_id: str, dateadded: Optional[str], source: str):
    """Update dateadded for a specific movie"""
    db = dependencies["db"]
    
    # Debug logging to track the issue
    print(f"🔍 UPDATE_MOVIE_DATE: imdb_id={imdb_id}, dateadded={dateadded}, source={source}")
    print(f"   - dateadded type: {type(dateadded)}")
    print(f"   - dateadded repr: {repr(dateadded)}")
    
    # Validate inputs
    if not imdb_id or not imdb_id.strip():
        print(f"❌ Invalid imdb_id: {repr(imdb_id)}")
        raise HTTPException(status_code=422, detail="Invalid IMDb ID")
    
    if not source or not source.strip():
        print(f"❌ Invalid source: {repr(source)}")
        raise HTTPException(status_code=422, detail="Invalid source")
    
    # Validate date format if provided
    if dateadded:
        try:
            from datetime import datetime
            datetime.fromisoformat(dateadded.replace('Z', '+00:00'))
        except Exception as e:
            print(f"❌ Invalid dateadded format: {repr(dateadded)} - {e}")
            raise HTTPException(status_code=422, detail=f"Invalid date format: {dateadded}")
    
    # Validate movie exists
    movie = db.get_movie_dates(imdb_id)
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found")
    
    # Update the date
    db.upsert_movie_dates(
        imdb_id=imdb_id,
        released=movie.get('released'),
        dateadded=dateadded,
        source=source,
        has_video_file=movie.get('has_video_file', False)
    )
    
    # Add to processing history
    try:
        db.add_processing_history(
            imdb_id=imdb_id,
            media_type="movie",
            event_type="manual_date_update",
            details={"old_source": movie.get('source'), "new_source": source, "dateadded": dateadded}
        )
    except Exception as e:
        print(f"⚠️ Failed to add processing history: {e}")
        # Don't fail the entire update for history logging issues
    
    print(f"✅ Successfully updated movie {imdb_id}")
    return {"status": "success", "message": f"Updated movie {imdb_id}"}


async def update_episode_date(dependencies: dict, imdb_id: str, season: int, episode: int, 
                            dateadded: Optional[str], source: str):
    """Update dateadded for a specific episode"""
    db = dependencies["db"]
    
    # Get existing episode
    episode_data = db.get_episode_date(imdb_id, season, episode)
    if not episode_data:
        raise HTTPException(status_code=404, detail="Episode not found")
    
    # Update the date
    db.upsert_episode_date(
        imdb_id=imdb_id,
        season=season,
        episode=episode,
        aired=episode_data.get('aired'),
        dateadded=dateadded,
        source=source,
        has_video_file=episode_data.get('has_video_file', False)
    )
    
    # Create/update NFO file with new data
    nfo_manager = dependencies["nfo_manager"]
    config = dependencies["config"]
    
    if config.manage_nfo:
        try:
            # Find the series directory based on IMDb ID
            series_path = None
            for tv_path in config.tv_paths:
                for series_dir in Path(tv_path).iterdir():
                    if series_dir.is_dir() and imdb_id.lower() in series_dir.name.lower():
                        series_path = series_dir
                        break
                if series_path:
                    break
            
            if series_path:
                season_dir = series_path / config.tv_season_dir_format.format(season=season)
                if season_dir.exists():
                    nfo_manager.create_episode_nfo(
                        season_dir=season_dir,
                        season_num=season,
                        episode_num=episode,
                        aired=episode_data.get('aired'),
                        dateadded=dateadded,
                        source=source,
                        lock_metadata=config.lock_metadata
                    )
                    print(f"✅ Updated NFO file for {imdb_id} S{season:02d}E{episode:02d}")
                else:
                    print(f"⚠️ Season directory not found: {season_dir}")
            else:
                print(f"⚠️ Series directory not found for {imdb_id}")
                
        except Exception as e:
            print(f"❌ Error updating NFO file for {imdb_id} S{season:02d}E{episode:02d}: {e}")
    
    # Add to processing history
    db.add_processing_history(
        imdb_id=imdb_id,
        media_type="episode",
        event_type="manual_date_update",
        details={
            "season": season, 
            "episode": episode,
            "old_source": episode_data.get('source'), 
            "new_source": source, 
            "dateadded": dateadded
        }
    )
    
    return {"status": "success", "message": f"Updated episode {imdb_id} S{season:02d}E{episode:02d}"}


async def bulk_update_source(dependencies: dict, media_type: str, old_source: str, new_source: str):
    """Bulk update source for movies or episodes"""
    db = dependencies["db"]
    
    if media_type not in ["movies", "episodes"]:
        raise HTTPException(status_code=400, detail="media_type must be 'movies' or 'episodes'")
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        
        if media_type == "movies":
            # Update movies
            cursor.execute("UPDATE movies SET source = %s WHERE source = %s", (new_source, old_source))
            updated_count = cursor.rowcount
            
            # Add history entries
            cursor.execute("SELECT imdb_id FROM movies WHERE source = %s", (new_source,))
            for row in cursor.fetchall():
                db.add_processing_history(
                    imdb_id=row[0],
                    media_type="movie",
                    event_type="bulk_source_update",
                    details={"old_source": old_source, "new_source": new_source}
                )
        else:
            # Update episodes
            cursor.execute("UPDATE episodes SET source = %s WHERE source = %s", (new_source, old_source))
            updated_count = cursor.rowcount
            
            # Add history entries
            cursor.execute("SELECT imdb_id, season, episode FROM episodes WHERE source = %s", (new_source,))
            for row in cursor.fetchall():
                db.add_processing_history(
                    imdb_id=row[0],
                    media_type="episode",
                    event_type="bulk_source_update",
                    details={
                        "season": row[1],
                        "episode": row[2],
                        "old_source": old_source, 
                        "new_source": new_source
                    }
                )
    
    return {
        "status": "success", 
        "message": f"Updated {updated_count} {media_type} from source '{old_source}' to '{new_source}'"
    }


async def get_movie_date_options(dependencies: dict, imdb_id: str):
    """Get available date options for a movie (Radarr import, digital release, etc.)"""
    db = dependencies["db"]
    nfo_manager = dependencies["nfo_manager"]
    
    # Get current movie data
    movie = db.get_movie_dates(imdb_id)
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found")
    
    # Debug logging (can be removed once Smart Fix is working)
    print(f"🔍 DEBUG: Movie data for {imdb_id}:")
    print(f"   - released: {repr(movie.get('released'))}")
    print(f"   - dateadded: {repr(movie.get('dateadded'))}")
    print(f"   - source: {repr(movie.get('source'))}")
    
    options = []
    
    # Option 1: Current dateadded (if exists and is different from released)
    if movie.get('dateadded'):
        current_source = movie.get('source', 'Unknown')
        current_date = movie['dateadded']
        
        # Determine what type of current date this is
        if 'radarr' in current_source.lower() and 'import' in current_source.lower():
            label = "Keep Current (Radarr Import Date)"
            description = f"Keep using Radarr download/import date: {current_date}"
        elif current_source == 'digital_release':
            label = "Keep Current (Digital Release)"
            description = f"Keep using digital release date: {current_date}"
        elif current_source == 'nfo_file_existing':
            label = "Keep Current (From Existing NFO)"
            description = f"Keep using date from existing NFO file: {current_date}"
        else:
            label = f"Keep Current ({current_source})"
            description = f"Keep using current date from {current_source}: {current_date}"
            
        options.append({
            "type": "current",
            "label": label,
            "date": current_date,
            "source": current_source,
            "description": description
        })
    
    # Option 2: Released date as digital release (if different from current)
    if movie.get('released') and movie['released'].strip():
        try:
            released_raw = movie['released']
            
            # Handle different released date formats
            if 'T' in released_raw:
                # Already has time component: 2018-07-27T00:00:00+00:00
                release_date = released_raw
            else:
                # Just date: 2018-07-27
                release_date = f"{released_raw}T00:00:00"
            
            # Validate the date format
            from datetime import datetime
            datetime.fromisoformat(release_date.replace('Z', '+00:00'))
            
            # Only add if it's different from current dateadded
            current_dateadded = movie.get('dateadded')
            current_date_str = current_dateadded.strftime('%Y-%m-%d') if current_dateadded else ''
            if not current_dateadded or not current_date_str.startswith(released_raw[:10]):  # Compare just the date part
                options.append({
                    "type": "digital_release", 
                    "label": "Use Actual Release Date",
                    "date": release_date,
                    "source": "digital_release",
                    "description": f"Use the movie's actual release date: {released_raw[:10]} (instead of download date)"
                })
        except Exception as e:
            print(f"⚠️ Invalid released date format for {imdb_id}: {movie.get('released')} - {e}")
            # Don't add this option if the date is invalid
    
    # Option 3: Manual entry
    options.append({
        "type": "manual",
        "label": "Manual Entry", 
        "date": None,
        "source": "manual",
        "description": "Enter custom date and time"
    })
    
    # Option 4: Active lookup from external sources
    try:
        # Get movie processor and clients from dependencies
        movie_processor = dependencies.get("movie_processor")
        if movie_processor and hasattr(movie_processor, 'external_clients'):
            external_clients = movie_processor.external_clients
            
            # Check Radarr for import dates
            if movie_processor.radarr and movie_processor.radarr.enabled:
                try:
                    radarr_movie = movie_processor.radarr.movie_by_imdb(imdb_id)
                    if radarr_movie:
                        movie_id = radarr_movie.get('id')
                        if movie_id:
                            import_date, source = movie_processor.radarr.get_movie_import_date(movie_id, fallback_to_file_date=True)
                            if import_date and source != "no_valid_date_source":
                                # Check if this is different from current date
                                current_dateadded = movie.get('dateadded')
                                current_date_str = current_dateadded.strftime('%Y-%m-%d') if current_dateadded else ''
                                if not current_dateadded or not current_date_str.startswith(import_date[:10]):
                                    options.append({
                                        "type": "radarr_import",
                                        "label": f"Radarr Import Date ({source})",
                                        "date": import_date,
                                        "source": f"radarr:{source}",
                                        "description": f"Import date from Radarr: {import_date[:10]} (source: {source})"
                                    })
                except Exception as e:
                    print(f"⚠️ Failed to get Radarr import date for {imdb_id}: {e}")
            
            # Check TMDB for digital release dates
            if external_clients.tmdb.enabled:
                try:
                    digital_release = external_clients.tmdb.get_digital_release_date(imdb_id)
                    if digital_release:
                        # Check if this is different from current date
                        current_dateadded = movie.get('dateadded')
                        current_date_str = current_dateadded.strftime('%Y-%m-%d') if current_dateadded else ''
                        if not current_dateadded or not current_date_str.startswith(digital_release[:10]):
                            options.append({
                                "type": "tmdb_digital",
                                "label": "TMDB Digital Release",
                                "date": f"{digital_release}T00:00:00",
                                "source": "tmdb:digital_release",
                                "description": f"Digital release date from TMDB: {digital_release}"
                            })
                except Exception as e:
                    print(f"⚠️ Failed to get TMDB digital release for {imdb_id}: {e}")
                    
            # Check OMDb for additional release info
            if external_clients.omdb.enabled:
                try:
                    omdb_details = external_clients.omdb.get_movie_details(imdb_id)
                    if omdb_details and omdb_details.get('Released') and omdb_details['Released'] != 'N/A':
                        from datetime import datetime
                        try:
                            # Parse OMDb date format (e.g., "27 Jul 2018")
                            omdb_date = datetime.strptime(omdb_details['Released'], '%d %b %Y')
                            omdb_iso = omdb_date.strftime('%Y-%m-%d')
                            
                            # Check if this is different from current date
                            current_dateadded = movie.get('dateadded')
                            current_date_str = current_dateadded.strftime('%Y-%m-%d') if current_dateadded else ''
                            if not current_dateadded or not current_date_str.startswith(omdb_iso):
                                options.append({
                                    "type": "omdb_release",
                                    "label": "OMDb Release Date",
                                    "date": f"{omdb_iso}T00:00:00",
                                    "source": "omdb:release",
                                    "description": f"Release date from OMDb: {omdb_iso}"
                                })
                        except ValueError:
                            # Skip if date parsing fails
                            pass
                except Exception as e:
                    print(f"⚠️ Failed to get OMDb details for {imdb_id}: {e}")
                    
    except Exception as e:
        print(f"⚠️ External source lookup failed for {imdb_id}: {e}")
    
    print(f"🔍 DEBUG: Generated {len(options)} options for {imdb_id}:")
    for i, option in enumerate(options):
        print(f"   Option {i}: {option}")
    
    return {
        "imdb_id": imdb_id,
        "current_data": movie,
        "options": options
    }


async def get_episode_date_options(dependencies: dict, imdb_id: str, season: int, episode: int):
    """Get available date options for an episode"""
    print(f"🔍 DEBUG: get_episode_date_options called with imdb_id={imdb_id}, season={season}, episode={episode}")
    db = dependencies["db"]
    
    # Validate parameters with enhanced checking
    try:
        if not imdb_id or not imdb_id.strip():
            print(f"❌ Invalid imdb_id: '{imdb_id}'")
            raise HTTPException(status_code=422, detail="Invalid imdb_id parameter")
        
        # Convert and validate season
        season = int(season) if isinstance(season, str) else season
        if season < 0:
            print(f"❌ Invalid season: {season}")
            raise HTTPException(status_code=422, detail="Season must be >= 0")
            
        # Convert and validate episode  
        episode = int(episode) if isinstance(episode, str) else episode
        if episode < 1:
            print(f"❌ Invalid episode: {episode}")
            raise HTTPException(status_code=422, detail="Episode must be >= 1")
    except ValueError as e:
        print(f"❌ Parameter conversion error: {e}")
        raise HTTPException(status_code=422, detail=f"Invalid parameter types: {e}")
    
    # Get current episode data
    episode_data = db.get_episode_date(imdb_id, season, episode)
    print(f"🔍 DEBUG: Episode data from DB: {episode_data}")
    if not episode_data:
        print(f"❌ Episode not found in database: {imdb_id} S{season:02d}E{episode:02d}")
        raise HTTPException(status_code=404, detail="Episode not found")
    
    options = []
    
    # Option 1: Current dateadded (if exists)
    if episode_data.get('dateadded'):
        options.append({
            "type": "current",
            "label": f"Keep Current ({episode_data.get('source', 'Unknown')})",
            "date": episode_data['dateadded'],
            "source": episode_data.get('source', 'manual'),
            "description": f"Currently using: {episode_data.get('source', 'Unknown')}"
        })
    
    # Option 2: Aired date (if exists in database)
    if episode_data.get('aired'):
        options.append({
            "type": "airdate",
            "label": "Use Air Date",
            "date": f"{episode_data['aired']}T20:00:00",  # Default to 8 PM
            "source": "airdate",
            "description": f"Use original air date: {episode_data['aired']}"
        })
    
    # Option 3: Active lookup from external sources
    try:
        # Get TV processor and clients from dependencies
        tv_processor = dependencies.get("tv_processor")
        print(f"🔍 DEBUG: tv_processor available: {tv_processor is not None}")
        if tv_processor:
            print(f"🔍 DEBUG: tv_processor has external_clients: {hasattr(tv_processor, 'external_clients')}")
            print(f"🔍 DEBUG: tv_processor has sonarr: {hasattr(tv_processor, 'sonarr')}")
        
        if tv_processor and hasattr(tv_processor, 'external_clients'):
            external_clients = tv_processor.external_clients
            print(f"🔍 DEBUG: external_clients available: {external_clients is not None}")
            if external_clients:
                print(f"🔍 DEBUG: TMDB enabled: {external_clients.tmdb.enabled if hasattr(external_clients, 'tmdb') else 'No TMDB client'}")
            
            # Check Sonarr for import dates
            if tv_processor.sonarr and tv_processor.sonarr.enabled:
                try:
                    print(f"🔍 DEBUG: Attempting Sonarr lookup for {imdb_id}")
                    # Look up the series and episode in Sonarr
                    series_data = tv_processor.sonarr.series_by_imdb(imdb_id)
                    
                    # If IMDb lookup fails, try direct series lookup as fallback
                    if not series_data:
                        print(f"🔍 DEBUG: IMDb lookup failed, trying direct series lookup")
                        try:
                            # Let's also debug what series are available
                            all_series = tv_processor.sonarr.get_all_series()
                            print(f"🔍 DEBUG: Found {len(all_series)} total series in Sonarr")
                            
                            # Look for Lincoln Lawyer specifically
                            lincoln_series = [s for s in all_series if 'lincoln' in s.get('title', '').lower()]
                            print(f"🔍 DEBUG: Lincoln Lawyer series found: {len(lincoln_series)}")
                            for ls in lincoln_series:
                                print(f"   - Title: '{ls.get('title')}', IMDb: '{ls.get('imdbId')}', ID: {ls.get('id')}")
                            
                            # Try direct lookup first
                            series_data = tv_processor.sonarr.series_by_imdb_direct(imdb_id)
                            
                            # If still no match but we found Lincoln Lawyer series, try fuzzy matching
                            if not series_data and lincoln_series:
                                target_imdb_num = imdb_id.replace('tt', '').lower()
                                print(f"🔍 DEBUG: Trying fuzzy match for IMDb number: {target_imdb_num}")
                                
                                for ls in lincoln_series:
                                    ls_imdb = ls.get('imdbId', '')
                                    ls_imdb_num = ls_imdb.replace('tt', '').lower()
                                    print(f"   - Comparing {target_imdb_num} vs {ls_imdb_num}")
                                    
                                    # Check if IMDb numbers are close (within 10 digits)
                                    if ls_imdb_num and target_imdb_num:
                                        try:
                                            target_num = int(target_imdb_num)
                                            ls_num = int(ls_imdb_num)
                                            diff = abs(target_num - ls_num)
                                            print(f"   - Numeric difference: {diff}")
                                            
                                            if diff <= 10:  # Allow small IMDb ID differences
                                                print(f"✅ Found close IMDb match: {ls_imdb} vs {imdb_id} (diff: {diff})")
                                                series_data = ls
                                                break
                                        except ValueError:
                                            continue
                        except Exception as e:
                            print(f"⚠️ Direct series lookup also failed: {e}")
                            import traceback
                            print(f"   Traceback: {traceback.format_exc()}")
                    
                    print(f"🔍 DEBUG: Series data found: {series_data is not None}")
                    if series_data:
                        series_id = series_data.get('id')
                        series_title = series_data.get('title', 'Unknown')
                        print(f"🔍 DEBUG: Found series '{series_title}' with ID {series_id}")
                        
                        if series_id:
                            # Get episodes for the series
                            print(f"🔍 DEBUG: Getting episodes for series {series_id}")
                            episodes = tv_processor.sonarr.episodes_for_series(series_id)
                            print(f"🔍 DEBUG: Found {len(episodes)} episodes")
                            
                            for ep in episodes:
                                ep_season = ep.get('seasonNumber')
                                ep_episode = ep.get('episodeNumber')
                                # Convert to int for proper comparison (handle both string and int from Sonarr)
                                try:
                                    ep_season = int(ep_season) if ep_season is not None else None
                                    ep_episode = int(ep_episode) if ep_episode is not None else None
                                except (ValueError, TypeError):
                                    continue  # Skip episodes with invalid season/episode numbers
                                    
                                if ep_season == season and ep_episode == episode:
                                    episode_id = ep.get('id')
                                    ep_title = ep.get('title', 'Unknown')
                                    ep_air_date = ep.get('airDate')  # Get air date from Sonarr
                                    print(f"🔍 DEBUG: Found target episode '{ep_title}' with ID {episode_id}, airDate: {ep_air_date}")
                                    
                                    if episode_id:
                                        # Get import history for this specific episode
                                        print(f"🔍 DEBUG: Getting import history for episode {episode_id}")
                                        import_date = tv_processor.sonarr.get_episode_import_history(episode_id)
                                        print(f"🔍 DEBUG: Import date found: {import_date}")
                                        
                                        if import_date:
                                            # Check if this is different from current date
                                            current_dateadded = episode_data.get('dateadded')
                                            current_date_str = current_dateadded.strftime('%Y-%m-%d') if current_dateadded else ''
                                            if not current_dateadded or not current_date_str.startswith(import_date[:10]):
                                                options.append({
                                                    "type": "sonarr_import",
                                                    "label": "Sonarr Import Date",
                                                    "date": import_date,
                                                    "source": "sonarr:import_history",
                                                    "description": f"Import date from Sonarr: {import_date[:10]}"
                                                })
                                                print(f"✅ Added Sonarr import option: {import_date[:10]}")
                                    
                                        # If no import date but we have air date from Sonarr, add as air date option
                                        if not import_date and ep_air_date:
                                            current_aired = episode_data.get('aired', '')
                                            current_dateadded = episode_data.get('dateadded', '')
                                            
                                            # Add air date option if different from current or missing
                                            if not current_aired or current_aired != ep_air_date:
                                                options.append({
                                                    "type": "sonarr_air",
                                                    "label": "Sonarr Air Date",
                                                    "date": f"{ep_air_date}T20:00:00",
                                                    "source": "sonarr:airdate",
                                                    "description": f"Air date from Sonarr: {ep_air_date}"
                                                })
                                                print(f"✅ Added Sonarr air date option: {ep_air_date}")
                                            
                                            # If no dateadded, suggest using air date as import date fallback
                                            if not current_dateadded:
                                                options.append({
                                                    "type": "sonarr_air_fallback",
                                                    "label": "Use Air Date as Import Date",
                                                    "date": f"{ep_air_date}T20:00:00",
                                                    "source": "sonarr:aired_fallback",
                                                    "description": f"Use Sonarr air date as import date: {ep_air_date}"
                                                })
                                                print(f"✅ Added Sonarr air date fallback option: {ep_air_date}")
                                    
                                    break
                    else:
                        print(f"❌ No series found in Sonarr for {imdb_id}")
                except Exception as e:
                    print(f"⚠️ Failed to get Sonarr import date for {imdb_id} S{season:02d}E{episode:02d}: {e}")
                    import traceback
                    print(f"   Traceback: {traceback.format_exc()}")
            
            # Check TMDB for episode air dates
            if external_clients.tmdb.enabled:
                try:
                    print(f"🔍 DEBUG: Attempting TMDB lookup for {imdb_id}")
                    # Get TMDB TV series ID from IMDb ID using find endpoint
                    tv_find_result = external_clients.tmdb._get(f"/find/{imdb_id}", {"external_source": "imdb_id"})
                    print(f"🔍 DEBUG: TMDB find result: {tv_find_result is not None}")
                    print(f"🔍 DEBUG: TMDB raw response: {tv_find_result}")
                    
                    # Check both tv_results and tv_episode_results
                    tmdb_id = None
                    tv_title = "Unknown"
                    
                    if tv_find_result and tv_find_result.get("tv_results"):
                        tv_results = tv_find_result.get("tv_results", [])
                        print(f"🔍 DEBUG: Found {len(tv_results)} TV results")
                        
                        if tv_results:
                            tv_show = tv_results[0]
                            tmdb_id = tv_show.get("id")
                            tv_title = tv_show.get("name", "Unknown")
                            print(f"🔍 DEBUG: Found TMDB series '{tv_title}' with ID {tmdb_id}")
                    
                    # Fallback: Check tv_episode_results for show_id
                    elif tv_find_result and tv_find_result.get("tv_episode_results"):
                        episode_results = tv_find_result.get("tv_episode_results", [])
                        print(f"🔍 DEBUG: Found {len(episode_results)} TV episode results")
                        
                        if episode_results:
                            tmdb_episode_data = episode_results[0]
                            tmdb_id = tmdb_episode_data.get("show_id")
                            episode_name = tmdb_episode_data.get("name", "Unknown")
                            print(f"🔍 DEBUG: Found TMDB series via episode '{episode_name}' with show_id {tmdb_id}")
                    
                    if tmdb_id:
                        print(f"🔍 DEBUG: Using TMDB ID {tmdb_id} for series lookup")
                        
                        # Get episode air date from TMDB
                        print(f"🔍 DEBUG: Getting TMDB season {season} episodes for series {tmdb_id}")
                        episodes = external_clients.tmdb.get_tv_season_episodes(tmdb_id, season)
                        print(f"🔍 DEBUG: TMDB episodes found: {episodes}")
                        
                        if episode in episodes:
                            air_date = episodes[episode]
                            print(f"🔍 DEBUG: TMDB air date for S{season:02d}E{episode:02d}: {air_date}")
                            
                            if air_date:
                                # Check if this is different from current aired date
                                current_aired = episode_data.get('aired', '')
                                if not current_aired or current_aired != air_date:
                                    options.append({
                                        "type": "tmdb_air",
                                        "label": "TMDB Air Date",
                                        "date": f"{air_date}T20:00:00",  # Default to 8 PM
                                        "source": "tmdb:airdate",
                                        "description": f"Air date from TMDB: {air_date}"
                                    })
                                    print(f"✅ Added TMDB air date option: {air_date}")
                                
                                # If no aired date in database, also add this as "Use Air Date" option
                                if not current_aired:
                                    options.insert(1, {  # Insert after current option
                                        "type": "airdate_tmdb",
                                        "label": "Use Air Date (TMDB)",
                                        "date": f"{air_date}T20:00:00",
                                        "source": "airdate",
                                        "description": f"Use air date from TMDB: {air_date}"
                                    })
                                    print(f"✅ Added 'Use Air Date' option from TMDB: {air_date}")
                        else:
                            print(f"❌ Episode {episode} not found in TMDB season {season} data")
                    else:
                        print(f"❌ No TV series ID found in TMDB for {imdb_id}")
                except Exception as e:
                    print(f"⚠️ Failed to get TMDB air date for {imdb_id} S{season:02d}E{episode:02d}: {e}")
                    import traceback
                    print(f"   Traceback: {traceback.format_exc()}")
            
            # Check external clients for episode air dates (TVDB, OMDb)
            if hasattr(external_clients, 'get_episode_air_date'):
                try:
                    air_date = external_clients.get_episode_air_date(imdb_id, season, episode)
                    if air_date:
                        # Check if this is different from current aired date
                        current_aired = episode_data.get('aired', '')
                        if not current_aired or current_aired != air_date:
                            options.append({
                                "type": "external_air",
                                "label": "External Air Date",
                                "date": f"{air_date}T20:00:00",  # Default to 8 PM
                                "source": "external:airdate",
                                "description": f"Air date from external sources: {air_date}"
                            })
                        
                        # If no aired date in database and not already added from TMDB, add this as "Use Air Date" option
                        if not current_aired and not any(opt.get('type') == 'airdate_tmdb' for opt in options):
                            options.insert(1, {  # Insert after current option
                                "type": "airdate_external",
                                "label": "Use Air Date (External)",
                                "date": f"{air_date}T20:00:00",
                                "source": "airdate",
                                "description": f"Use air date from external sources: {air_date}"
                            })
                except Exception as e:
                    print(f"⚠️ Failed to get external air date for {imdb_id} S{season:02d}E{episode:02d}: {e}")
                    
    except Exception as e:
        print(f"⚠️ External source lookup failed for {imdb_id} S{season:02d}E{episode:02d}: {e}")
    
    # Option 4: Manual entry
    options.append({
        "type": "manual",
        "label": "Manual Entry",
        "date": None,
        "source": "manual", 
        "description": "Enter custom date and time"
    })
    
    print(f"🔍 DEBUG: Generated {len(options)} options for {imdb_id} S{season:02d}E{episode:02d}:")
    for i, option in enumerate(options):
        print(f"   Option {i}: {option}")
    
    print(f"🔍 DEBUG: Returning result with {len(options)} options")
    return {
        "imdb_id": imdb_id,
        "season": season,
        "episode": episode,
        "current_data": episode_data,
        "options": options
    }


def register_web_routes(app, dependencies):
    """Register all web API routes with FastAPI app"""
    from fastapi import Request, Response
    
    # Dashboard and stats endpoints  
    @app.get("/api/dashboard")
    async def api_dashboard():
        return await get_dashboard_stats(dependencies)
    
    @app.get("/api/dashboard/stats")
    async def api_dashboard_stats():
        return await get_dashboard_stats(dependencies)
    
    # Movies endpoints
    @app.get("/api/movies")
    async def api_movies_list(skip: int = 0, limit: int = 100, has_date: bool = None, 
                             source_filter: str = None, search: str = None, imdb_search: str = None):
        return await get_movies_list(dependencies, skip, limit, has_date, source_filter, search, imdb_search)
    
    @app.post("/api/movies/{imdb_id}/update-date")
    async def api_update_movie_date(imdb_id: str, dateadded: str = None, source: str = "manual"):
        return {"error": "Updates not available in web container. Use core container on port 8085."}
    
    @app.put("/api/movies/{imdb_id}")
    async def api_update_movie(imdb_id: str, dateadded: str = None, source: str = "manual"):
        return {"error": "Updates not available in web container. Use core container on port 8085."}
    
    @app.get("/api/movies/{imdb_id}/date-options")
    async def api_movie_date_options(imdb_id: str):
        return {"options": [], "message": "Date options not available in web container. Use core container on port 8085."}
    
    # TV series endpoints
    @app.get("/api/series")
    async def api_series_list(skip: int = 0, limit: int = 50, search: str = None, 
                             imdb_search: str = None, date_filter: str = None, source_filter: str = None):
        return await get_tv_series_list(dependencies, skip, limit, search, imdb_search, date_filter, source_filter)
    
    @app.get("/api/series/{imdb_id}/episodes")
    async def api_series_episodes(imdb_id: str):
        return await get_series_episodes(dependencies, imdb_id)
    
    @app.get("/api/series/sources")
    async def api_series_sources():
        return await get_series_sources(dependencies)
    
    @app.get("/api/series/debug/date-distribution")
    async def api_debug_series_date_distribution():
        return await debug_series_date_distribution(dependencies)
    
    # Episode endpoints
    @app.post("/api/episodes/{imdb_id}/{season}/{episode}/update-date")
    async def api_update_episode_date(imdb_id: str, season: int, episode: int, 
                                     dateadded: str = None, source: str = "manual"):
        return {"error": "Updates not available in web container. Use core container on port 8085."}
    
    @app.put("/api/episodes/{imdb_id}/{season}/{episode}")
    async def api_update_episode(imdb_id: str, season: int, episode: int, 
                                dateadded: str = None, source: str = "manual"):
        return {"error": "Updates not available in web container. Use core container on port 8085."}
    
    @app.get("/api/episodes/{imdb_id}/{season}/{episode}/date-options")
    async def api_episode_date_options(imdb_id: str, season: int, episode: int):
        return {"options": [], "message": "Date options not available in web container. Use core container on port 8085."}
    
    # Bulk operations
    @app.post("/api/bulk/update-source")
    async def api_bulk_update_source(media_type: str, old_source: str, new_source: str):
        return {"error": "Bulk operations not available in web container. Use core container on port 8085."}
    
    # Reports
    @app.get("/api/reports/missing-dates")
    async def api_missing_dates_report():
        return await get_missing_dates_report(dependencies)
    
    # Authentication endpoints (for web interface compatibility)
    @app.get("/api/auth/status")
    async def api_auth_status(request: Request):
        """Check authentication status"""
        auth_enabled = dependencies.get("auth_enabled", False)
        
        if not auth_enabled:
            return {"authenticated": True, "auth_enabled": False, "message": "Authentication disabled"}
        
        session_manager = dependencies.get("session_manager")
        if not session_manager:
            return {"authenticated": False, "auth_enabled": True, "message": "Session manager not available"}
        
        session_token = request.cookies.get("nfoguard_session")
        if session_token:
            username = session_manager.get_session_user(session_token)
            if username:
                return {"authenticated": True, "auth_enabled": True, "username": username}
        
        return {"authenticated": False, "auth_enabled": True, "message": "Not authenticated"}
    
    @app.post("/api/auth/logout")
    async def api_auth_logout(request: Request, response: Response):
        """Logout endpoint - clears session"""
        session_manager = dependencies.get("session_manager")
        if session_manager:
            session_token = request.cookies.get("nfoguard_session")
            if session_token:
                session_manager.delete_session(session_token)
        
        response.delete_cookie("nfoguard_session")
        return {"status": "logged_out", "message": "Session cleared"}
    
    # Manual scan endpoints (proxy to core container)
    @app.post("/manual/scan")
    async def api_manual_scan(request: Request):
        """Proxy manual scan requests to core container"""
        import urllib.request
        import urllib.parse
        import urllib.error
        import json
        import os
        import socket
        
        # Get core container URL
        core_host = os.environ.get("CORE_API_HOST", "nfoguard-core")
        core_port = os.environ.get("CORE_API_PORT", "8080")
        
        # Forward query parameters from the request
        query_string = str(request.url.query) if request.url.query else ""
        core_url = f"http://{core_host}:{core_port}/manual/scan"
        if query_string:
            core_url += f"?{query_string}"
        
        try:
            # Create request with timeout (no body needed for query parameters)
            req = urllib.request.Request(core_url, method='POST')
            
            # Make request with timeout
            with urllib.request.urlopen(req, timeout=30) as response:
                response_data = response.read().decode('utf-8')
                return json.loads(response_data)
                
        except urllib.error.HTTPError as e:
            raise HTTPException(status_code=e.code, detail=f"Core container HTTP error: {e.reason}")
        except urllib.error.URLError as e:
            raise HTTPException(status_code=503, detail=f"Could not connect to core container: {str(e)}")
        except socket.timeout:
            raise HTTPException(status_code=504, detail="Core container request timed out")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Manual scan request failed: {str(e)}")
    
    # Simple scan tracking (since we can't reliably access docker logs from container)
    scan_tracking = {"last_scan_time": None, "scanning": False}
    
    @app.post("/api/scan/track")
    async def track_scan_start():
        """Called when a scan is initiated to track timing"""
        from datetime import datetime
        scan_tracking["last_scan_time"] = datetime.now()
        scan_tracking["scanning"] = True
        return {"status": "tracked"}
    
    @app.get("/api/scan/status")
    async def api_scan_status():
        """Proxy scan status requests to core container for detailed progress"""
        import urllib.request
        import urllib.error
        import json
        import os
        import socket
        
        # Get core container connection details
        core_host = os.environ.get("CORE_API_HOST", "nfoguard-core")
        core_port = os.environ.get("CORE_API_PORT", "8080")
        
        try:
            # Call core container's detailed scan status endpoint
            status_url = f"http://{core_host}:{core_port}/api/scan/status"
            req = urllib.request.Request(status_url)
            
            with urllib.request.urlopen(req, timeout=5) as response:
                status_data = json.loads(response.read().decode())
                return status_data
                
        except urllib.error.HTTPError as e:
            if e.code == 404:
                # Core container doesn't have the endpoint, fallback to simple tracking
                return {"scanning": False, "message": "Detailed status not available"}
            else:
                return {"scanning": False, "message": f"Core container error: {e.code}"}
        except (urllib.error.URLError, socket.timeout):
            return {"scanning": False, "message": "Core container unavailable"}
        except json.JSONDecodeError:
            return {"scanning": False, "message": "Invalid response from core container"}
        except Exception as e:
            return {"scanning": False, "message": f"Unable to check scan status: {str(e)}"}