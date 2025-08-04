import logging

logger = logging.getLogger(__name__)

def remove_dupes(artist_data):
    """
    Removes duplicate artist entries from the list of artist data
    kwrob's artist data will rarely have artist data entries with the same name. To prevent foreign key constraint errors when upserting data, this function should be called beforehand to sanitize the data

    Args:
        artist_data (list): The list of artist data tuples, where each tuple contains (name, spotify_id)

    Returns:
        list: A list of artist entries with duplicates removed
    """
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
