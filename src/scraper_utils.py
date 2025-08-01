import logging

logger = logging.getLogger(__name__)

def remove_dupes(artist_data):
    seen_artist_names = {}
    cleaned_artist_data = []

    logger.info('Removing duplicate artist data...')
    
    for artist in artist_data:
        name = artist[0]
        if (name not in seen_artist_names):
            seen_artist_names[name] = artist
            cleaned_artist_data.append(artist)

    logger.info(f'Total artist count after removing dupes: {len(cleaned_artist_data)}')

    return cleaned_artist_data
