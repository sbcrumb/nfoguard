"""
Pydantic models for NFOGuard API
"""
from pydantic import BaseModel
from typing import Optional, Dict, Any, List


class SonarrWebhook(BaseModel):
    """Sonarr webhook payload model"""
    eventType: str
    series: Optional[Dict[str, Any]] = None
    episodes: Optional[list] = []
    episodeFile: Optional[Dict[str, Any]] = None
    isUpgrade: Optional[bool] = False

    class Config:
        extra = "allow"


class RadarrWebhook(BaseModel):
    """Radarr webhook payload model"""
    eventType: str
    movie: Optional[Dict[str, Any]] = None
    movieFile: Optional[Dict[str, Any]] = None
    isUpgrade: Optional[bool] = False
    deletedFiles: Optional[list] = []
    remoteMovie: Optional[Dict[str, Any]] = None
    renamedMovieFiles: Optional[List[Dict[str, Any]]] = None

    class Config:
        extra = "allow"


class HealthResponse(BaseModel):
    """Health check response model"""
    status: str
    version: str
    uptime: str
    database_status: str
    radarr_database: Optional[Dict[str, Any]] = None


class TVSeasonRequest(BaseModel):
    """TV season processing request model"""
    series_path: str
    season_name: str


class TVEpisodeRequest(BaseModel):
    """TV episode processing request model"""
    series_path: str
    season: int
    episode: int


# Web interface models
class MovieUpdateRequest(BaseModel):
    """Request to update movie dateadded"""
    dateadded: Optional[str]
    source: str


class EpisodeUpdateRequest(BaseModel):
    """Request to update episode dateadded"""
    dateadded: Optional[str]
    source: str


class BulkUpdateRequest(BaseModel):
    """Request for bulk source updates"""
    media_type: str  # "movies" or "episodes"
    old_source: str
    new_source: str


class MovieResponse(BaseModel):
    """Movie data response"""
    imdb_id: str
    title: str
    path: str
    released: Optional[str]
    dateadded: Optional[str]
    source: Optional[str]
    has_video_file: bool
    last_updated: str


class SeriesResponse(BaseModel):
    """TV series data response"""
    imdb_id: str
    title: str
    path: str
    last_updated: str
    total_episodes: int
    episodes_with_dates: int
    episodes_with_video: int


class EpisodeResponse(BaseModel):
    """TV episode data response"""
    season: int
    episode: int
    aired: Optional[str]
    dateadded: Optional[str]
    source: Optional[str]
    has_video_file: bool
    last_updated: str
    series_path: str
    season_name: str
    episode_name: str