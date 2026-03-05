import logging
from typing import Optional
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials

from src.config import SPOTIFY_CONFIG

logger = logging.getLogger(__name__)

class SpotifyServiceClient:
    """Service for fetching album information from Spotify API."""

    def __init__(self):
        """Initialize Spotify API client."""
        self.client_id = SPOTIFY_CONFIG['CLIENT_ID']
        self.client_secret = SPOTIFY_CONFIG['CLIENT_SECRET']
        
        if not self.client_id or not self.client_secret:
            logger.warning("Spotify credentials not found.")
            self.sp = None
        else:
            try:
                auth_manager = SpotifyClientCredentials(
                    client_id=self.client_id,
                    client_secret=self.client_secret
                )
                self.sp = Spotify(auth_manager=auth_manager)
                logger.info("Spotify API client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Spotify API client: {e}")
                self.sp = None

    def get_album_info(self, song_title: str, artist_name: str) -> Optional[tuple]:
        """
        Get album name and cover URL for a song using Spotify API. If no album information is found, return None.

        Args:
            song_title (str): The title of the song
            artist_name (str): The name of the artist
            
        Returns:
            Optional[tuple]: (album_name, cover_url) or None if not found
        """
        if not self.sp:
            logger.warning("Spotify API not available")
            return None
            
        try:
            # Search for the track
            query = f"track:{song_title} artist:{artist_name}"
            results = self.sp.search(q=query, type='track', limit=1)
            
            if results['tracks']['items']:
                track = results['tracks']['items'][0]
                album = track['album']
                album_name = album['name']
                album_images = album['images']
                
                if album_images:
                    # Return album name and highest resolution image URL
                    cover_url = album_images[0]['url']
                    return (album_name, cover_url)
                else:
                    logger.warning(f"No album cover found for {song_title} by {artist_name}")
                    return (album_name, None)
            else:
                logger.warning(f"Track not found: {song_title} by {artist_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching album info for {song_title} by {artist_name}: {e}")
            return None

# Global instance
spotify_service = SpotifyServiceClient()
