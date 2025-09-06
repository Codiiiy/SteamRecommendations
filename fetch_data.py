import requests
from config_key import load_config

DB_FILE = 'game_library.db'


def fetch_owned_games(steam_id):
    """Fetch the list of games a user owns."""
    config = load_config()
    if not config:
        return []
    API_KEY = config['steam_api_key']
    url = "http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/"
    params = {
        'key': API_KEY,
        'steamid': steam_id,
        'include_appinfo': True,
        'include_played_free_games': True
    }
    try:
        response = requests.get(url, params=params, timeout=10).json()
        return response.get('response', {}).get('games', [])
    except Exception:
        return []


def fetch_friends(steam_id):
    """Fetch a user's Steam friends list."""
    config = load_config()
    if not config:
        return []
    API_KEY = config['steam_api_key']
    url = "http://api.steampowered.com/ISteamUser/GetFriendList/v0001/"
    params = {'key': API_KEY, 'steamid': steam_id, 'relationship': 'friend'}
    try:
        response = requests.get(url, params=params, timeout=10).json()
        return [f['steamid'] for f in response.get('friendslist', {}).get('friends', [])]
    except Exception:
        return []


def get_review_count(appid):
    """Get the number of reviews for a game. Returns 0 if unable to fetch."""
    url = f"https://store.steampowered.com/appreviews/{appid}"
    params = {
        'json': 1,
        'num_per_page': 1 
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        review_count = data.get('query_summary', {}).get('total_reviews')
        return review_count if review_count is not None else 0
    except (requests.RequestException, ValueError):
        return 0


def fetch_store_info(appid, country_code='us'):
    """Fetch store info including price, developer, tags, DLCs, and release date."""
    url = "https://store.steampowered.com/api/appdetails"
    params = {'appids': appid, 'cc': country_code}
    try:
        r = requests.get(url, params=params, timeout=10)
        data_entry = r.json().get(str(appid))
    except (requests.RequestException, ValueError):
        return None

    if not data_entry or not data_entry.get('success'):
        return None

    data = data_entry.get('data', {})
    price_info = data.get('price_overview')
    base_price = price_info['initial'] / 100 if price_info else None
    developer = ", ".join(data.get('developers', []))
    release_date = data.get('release_date', {}).get('date')
    tags_list = [g.get('description', '') for g in data.get('genres', [])] if 'genres' in data else ['none']
    dlcs = data.get('dlc', [])
    coming_soon = 1 if data.get('release_date', {}).get('coming_soon') else 0

    return {
        'base_price': base_price,
        'developer': developer,
        'release_date': release_date,
        'tags': tags_list if tags_list else ['none'],
        'dlcs': dlcs,
        'coming_soon': coming_soon
    }


def fetch_all_steam_games():
    """Fetch the full list of Steam apps (game_id + name)."""
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    try:
        response = requests.get(url, timeout=20).json()
        return response.get("applist", {}).get("apps", [])
    except Exception:
        return []