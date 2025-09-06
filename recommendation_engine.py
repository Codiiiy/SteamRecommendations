import json
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from fetch_data import fetch_owned_games, fetch_all_steam_games, fetch_store_info, get_review_count
from pathlib import Path
import sqlite3
import numpy as np
import random
from collections import Counter
from datetime import datetime, timedelta

DB_FILE = "game_library.db"
CACHE_FILE = Path("steam_cache.json")
MIN_REVIEWS = 500

if CACHE_FILE.exists():
    with open(CACHE_FILE, "r") as f:
        steam_cache = json.load(f)
else:
    steam_cache = {}

def get_user_profile(user_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT g.game_id, g.title, g.tags, g.developer, ug.hours_played, g.release_date
        FROM Games g
        JOIN UserGames ug ON g.game_id = ug.game_id
        WHERE ug.user_id = ? AND g.tags IS NOT NULL
        ORDER BY ug.hours_played DESC
    """, (user_id,))
    owned_games = cursor.fetchall()
    
    cursor.execute("""
        SELECT ug.game_id, COUNT(*) as friend_count
        FROM UserGames ug
        JOIN Friends f ON ug.user_id = f.friend_id
        WHERE f.user_id = ? AND ug.game_id NOT IN (
            SELECT game_id FROM UserGames WHERE user_id = ?
        )
        GROUP BY ug.game_id
        ORDER BY friend_count DESC
        LIMIT 100
    """, (user_id, user_id))
    friends_games = dict(cursor.fetchall())
    
    conn.close()
    
    if not owned_games:
        return None, friends_games
    
    total_hours = sum(g[4] for g in owned_games)
    
    weighted_tags = []
    weighted_developers = []
    
    for game in owned_games:
        tags = game[2] if game[2] else ""
        developer = game[3] if game[3] else ""
        hours = max(game[4], 0.1)
        weight = hours / total_hours if total_hours > 0 else 1/len(owned_games)
        
        if tags:
            tag_list = [t.strip() for t in tags.split(',') if t.strip()]
            weighted_tags.extend(tag_list * int(weight * 100 + 1))
        
        if developer:
            weighted_developers.extend([developer] * int(weight * 50 + 1))
    
    top_tags = [tag for tag, count in Counter(weighted_tags).most_common(10)]
    top_developers = [dev for dev, count in Counter(weighted_developers).most_common(5)]
    
    high_playtime_games = [g for g in owned_games if g[4] > 10]
    avg_playtime = np.mean([g[4] for g in owned_games]) if owned_games else 0
    
    profile = {
        'preferred_tags': top_tags,
        'preferred_developers': top_developers,
        'avg_playtime': avg_playtime,
        'high_playtime_count': len(high_playtime_games),
        'total_games': len(owned_games)
    }
    
    return profile, friends_games

def get_smart_candidates(owned_game_ids, profile, friends_games):
    all_steam_games = fetch_all_steam_games()
    if not all_steam_games:
        return []
    
    candidates = []
    
    friend_candidates = []
    for game_id, friend_count in friends_games.items():
        if game_id not in owned_game_ids:
            game_info = next((g for g in all_steam_games if g['appid'] == game_id), None)
            if game_info:
                friend_candidates.append((game_info, friend_count * 2))
    
    recent_candidates = []
    for game in random.sample(all_steam_games, min(5000, len(all_steam_games))):
        if game['appid'] not in owned_game_ids:
            name = game['name'].lower()
            if not any(skip in name for skip in ['dlc', 'soundtrack', 'wallpaper', 'demo', 'beta']):
                recent_candidates.append((game, 1))
    
    candidates = friend_candidates[:200] + recent_candidates[:1800]
    random.shuffle(candidates)
    
    return [c[0] for c in candidates[:500]]

def calculate_personalized_score(game_info, profile, friends_games, game_id):
    score = 0.0
    
    rating = game_info.get('average_rating', 0)
    review_count = game_info.get('review_count', 0)
    popularity_score = rating + np.log1p(review_count) * 0.1
    score += popularity_score * 0.3
    
    tags = game_info.get('tags', [])
    if isinstance(tags, list):
        game_tags = [str(tag).strip().lower() for tag in tags]
    else:
        game_tags = [str(tag).strip().lower() for tag in str(tags).split(',')]
    
    tag_score = 0
    if profile and profile['preferred_tags']:
        user_tags = [tag.lower() for tag in profile['preferred_tags']]
        common_tags = len(set(game_tags) & set(user_tags))
        tag_score = common_tags / max(len(user_tags), 1)
    
    score += tag_score * 0.4
    
    developer = game_info.get('developer', '').lower()
    dev_score = 0
    if profile and profile['preferred_developers']:
        user_devs = [dev.lower() for dev in profile['preferred_developers']]
        if any(dev in developer for dev in user_devs):
            dev_score = 0.5
    
    score += dev_score * 0.2
    
    friends_score = friends_games.get(game_id, 0) / 10.0
    score += min(friends_score, 0.5) * 0.3
    
    price = game_info.get('base_price', 0) or 0
    if price == 0:
        price_score = 0.2
    elif price < 20:
        price_score = 0.1
    elif price < 40:
        price_score = 0.05
    else:
        price_score = -0.1
    
    score += price_score
    
    return score

def recommend(user_id, top_n=10):
    print(f"[DEBUG] Starting personalized recommendations for user {user_id}")
    
    profile, friends_games = get_user_profile(user_id)
    print(f"[DEBUG] User profile: {len(profile['preferred_tags']) if profile else 0} preferred tags")
    print(f"[DEBUG] Friends data: {len(friends_games)} games from friends")
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT game_id FROM UserGames WHERE user_id = ?", (user_id,))
    owned_game_ids = set([row[0] for row in cursor.fetchall()])
    conn.close()
    
    print(f"[DEBUG] User owns {len(owned_game_ids)} games")
    
    if not owned_game_ids:
        return pd.DataFrame(columns=["game_id","title","tags","developer","price"])
    
    candidates = get_smart_candidates(owned_game_ids, profile, friends_games)
    print(f"[DEBUG] Testing {len(candidates)} smart candidates")
    
    recommendations = []
    api_calls = 0
    
    for i, candidate in enumerate(candidates):
        if i % 100 == 0:
            print(f"[DEBUG] Processed {i}/{len(candidates)} candidates, found {len(recommendations)} valid")
            
        appid = candidate['appid']
        title = candidate['name']
        
        review_count_key = f"{appid}_reviews"
        if review_count_key in steam_cache:
            review_count = steam_cache[review_count_key]
        else:
            review_count = get_review_count(appid)
            steam_cache[review_count_key] = review_count
            api_calls += 1
        
        if review_count < MIN_REVIEWS:
            continue
        
        appid_str = str(appid)
        if appid_str in steam_cache:
            info = steam_cache[appid_str]
        else:
            try:
                info = fetch_store_info(appid)
                if info:
                    info['review_count'] = review_count
                    steam_cache[appid_str] = info
                    if api_calls % 20 == 0:
                        with open(CACHE_FILE, "w") as f:
                            json.dump(steam_cache, f)
            except Exception as e:
                print(f"[DEBUG] Error fetching info for {appid}: {e}")
                info = None
        
        api_calls += 1

        if not info:
            continue
        
        if 'review_count' not in info:
            info['review_count'] = review_count
        
        final_score = calculate_personalized_score(info, profile, friends_games, appid)
        
        tags = info.get('tags', [])
        if isinstance(tags, list):
            tag_str = ", ".join(str(tag) for tag in tags)
        else:
            tag_str = str(tags)
        
        recommendations.append({
            'game_id': appid,
            'title': title,
            'tags': tag_str,
            'developer': info.get('developer', 'Unknown'),
            'price': info.get('base_price', 0),
            'final_score': final_score,
            'rating': info.get('average_rating', 0),
            'review_count': info.get('review_count', 0),
            'friends_own': friends_games.get(appid, 0)
        })
        
        if len(recommendations) >= top_n * 3:
            break
    
    print(f"[DEBUG] Made {api_calls} API calls, found {len(recommendations)} valid games")
    
    if not recommendations:
        print("[DEBUG] No valid recommendations found")
        return pd.DataFrame(columns=["game_id","title","tags","developer","price"])
    
    rec_df = pd.DataFrame(recommendations)
    top_df = rec_df.sort_values("final_score", ascending=False).head(top_n)
    
    print(f"[DEBUG] Top recommendation scores: {top_df['final_score'].tolist()}")
    print(f"[DEBUG] Returning {len(top_df)} personalized recommendations")
    
    return top_df[["game_id","title","tags","developer","price"]]