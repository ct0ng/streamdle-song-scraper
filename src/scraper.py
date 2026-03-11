import logging
import requests
from bs4 import BeautifulSoup

from src.db_utils import query_artist_data
from src.config import SCRAPER_CONFIG, SONG_RANGES

ARTISTS_URL = SCRAPER_CONFIG['ARTISTS_URL']
BASE_URL = SCRAPER_CONFIG['BASE_URL']
ARTISTS_COUNT = SCRAPER_CONFIG['ARTISTS_COUNT']

logger = logging.getLogger(__name__)

def scrape_artist_data():
    """
    Scrapes artist data from the kworb website and returns a list of artist information.
    Fetches the site's HTML content, parses the page to extract artist names and their corresponding Spotify IDs,
    and returns a list of tuples containing this info mation for a specified number of artists (ARTISTS_COUNT)

    Returns:
        list of tuples - (artist_name, spotify_id)
    """
    artist_data = []

    logger.info(f'Scraping data for {ARTISTS_COUNT} artists...')

    response = requests.get(ARTISTS_URL)
    response.encoding = 'utf-8'  # Ensure UTF-8 encoding
    soup = BeautifulSoup(response.text, 'html.parser')
    for a_tag in soup.select('table a')[:ARTISTS_COUNT]:
        href = a_tag.get('href')
        name = a_tag.text.strip()
        if href and href.startswith('/spotify/artist/'):
            spotify_id = href.split("/")[-1].replace("_songs.html", "")
            artist_data.append((name, spotify_id))

    logger.info(f'Successfully scraped artist data for {len(artist_data)} artists')

    return artist_data

def get_album_cover_url(spotify_track_id):
    """
    Fetches the album cover URL for a Spotify track using the oEmbed API.

    Args:
        spotify_track_id (str): the Spotify track ID

    Returns:
        str: the thumbnail_url (album cover URL)
    """
    if not spotify_track_id:
        return None
    
    try:
        oembed_url = f"https://open.spotify.com/oembed?url=https://open.spotify.com/track/{spotify_track_id}"
        response = requests.get(oembed_url)
        response.raise_for_status()

        data = response.json()
        thumbnail_url = data.get('thumbnail_url')

        if thumbnail_url:
            return thumbnail_url
        else:
            logger.warning(f'No thumbnail_url found in oEmbed response for track {spotify_track_id}')
            return None

    except requests.exceptions.RequestException as e:
        logger.warning(f'Error fetching album cover URL for track {spotify_track_id}: {e}')
        return None
    except Exception as e:
        logger.warning(f'Unexpected error fetching album cover URL for track {spotify_track_id}: {e}')
        return None

def scrape_song_data(): 
    """
    Scrapes song data for the list of artists previously scraped and returns said data.
    Queries the artist table to grab each artists' ID. For each artist, construct a URL that lists each artists' top streamed songs, fetch and parse the HTML to extract song titles and stream counts.
    The number of songs scraped per artist is determined by the SONG_RANGES mapping, with default equal to 5

    Returns:
        List of tuples (song_name, artist_id, stream_count, spotify_track_id, album_cover_url)
    """
    
    song_data = []

    logger.info(f'Scraping song data for {ARTISTS_COUNT} artists...')

    artist_data = query_artist_data()
    for idx, (artist_id, spotify_id, artist_name) in enumerate(artist_data):
        logger.info(f'Scraping song data for artist {artist_name}; ({idx + 1} / {len(artist_data)})...')
        songs_url = f"{BASE_URL}/{spotify_id}_songs.html"
        try:
            response = requests.get(songs_url)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')

            rows = []
            row_limit = next((limit for r, limit in SONG_RANGES.items() if idx in r), 5)
            rows = soup.select("table")[1].select("tr")[1:row_limit]
            for row in rows:
                cols = row.find_all("td")
                song_name = cols[0].text.strip()
                # ignore songs where the artist is a feature
                if not song_name.startswith('* '):
                    # extract Spotify track ID from the first column
                    spotify_track_id = None
                    a_tag = cols[0].find('a')
                    if a_tag and a_tag.get('href'):
                        href = a_tag.get('href')
                        if '/track/' in href:
                            spotify_track_id = href.split('/track/')[-1].split('?')[0].split('#')[0]

                    # extract stream count from the second column
                    streams_str = cols[1].text.strip().replace(",", "")
                    if streams_str.isdigit():
                        stream_count = int(streams_str)

                        # fetch album cover URL using Spotify oEmbed API
                        album_cover_url = get_album_cover_url(spotify_track_id)

                        logger.info(f'Adding song data for {song_name} by {artist_name}')
                        song_data.append((
                            song_name, 
                            artist_id, 
                            stream_count,
                            spotify_track_id,
                            album_cover_url
                        ))
                    else:
                        continue

            logger.info(f'Total songs scraped so far: {len(song_data)}')

        except Exception as e:
            logger.info(f"Error scraping {songs_url}: {e}")

    logger.info(f'Successfully scraped song data for {len(song_data)} songs')
    
    return song_data