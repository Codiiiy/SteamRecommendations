import sqlite3
import os
import time
from datetime import datetime, timedelta
from fetch_data import fetch_owned_games, fetch_friends, fetch_store_info
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

DB_FILE = 'game_library.db'


def normalize_data():
    """Normalize and clean data in the database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            DELETE FROM GameTags WHERE game_id NOT IN (SELECT game_id FROM Games)
        """)

        cursor.execute("""
            UPDATE Games
            SET tags = (
                SELECT GROUP_CONCAT(tag, ', ')
                FROM GameTags
                WHERE GameTags.game_id = Games.game_id
            )
            WHERE EXISTS (
                SELECT 1 FROM GameTags WHERE GameTags.game_id = Games.game_id
            )
        """)

        cursor.execute("""
            DELETE FROM Games WHERE game_id NOT IN (SELECT DISTINCT game_id FROM UserGames)
        """)

        conn.commit()
        print("[DEBUG] Data normalization completed successfully")
    except Exception as e:
        print(f"[ERROR] Error during data normalization: {e}")
        conn.rollback()
    finally:
        conn.close()


def setup_database_schema():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        country TEXT,
        last_updated TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Games (
        game_id INTEGER PRIMARY KEY,
        title TEXT,
        tags TEXT,
        developer TEXT,
        release_date TEXT,
        base_price REAL,
        average_rating REAL,
        review_count INTEGER,
        coming_soon INTEGER DEFAULT 0,
        last_updated TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS DLCs (
        dlc_id INTEGER PRIMARY KEY,
        game_id INTEGER,
        title TEXT,
        price REAL,
        FOREIGN KEY (game_id) REFERENCES Games(game_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS UserGames (
        user_id INTEGER,
        game_id INTEGER,
        hours_played REAL,
        purchase_price REAL,
        dlc_owned INTEGER,
        last_updated TEXT,
        PRIMARY KEY(user_id, game_id),
        FOREIGN KEY (user_id) REFERENCES Users(user_id),
        FOREIGN KEY (game_id) REFERENCES Games(game_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Friends (
        user_id INTEGER,
        friend_id INTEGER,
        PRIMARY KEY(user_id, friend_id),
        FOREIGN KEY (user_id) REFERENCES Users(user_id),
        FOREIGN KEY (friend_id) REFERENCES Users(user_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Reviews (
        review_id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER,
        user_id INTEGER,
        review_text TEXT,
        sentiment REAL,
        FOREIGN KEY (game_id) REFERENCES Games(game_id),
        FOREIGN KEY (user_id) REFERENCES Users(user_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS GameTags (
        game_id INTEGER,
        tag TEXT,
        PRIMARY KEY (game_id, tag),
        FOREIGN KEY (game_id) REFERENCES Games(game_id)
    )
    ''')

    conn.commit()
    conn.close()


def setup_database():
    if not os.path.exists(DB_FILE):
        print("[DEBUG] Database file not found. Setting up database...")
        setup_database_schema()
        print("[DEBUG] Database setup complete!")
    else:
        print("[DEBUG] Database already exists. Skipping setup.")


def should_update_user(steam_id, hours_threshold=24):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT last_updated FROM Users WHERE user_id = ?", (steam_id,))
    result = cursor.fetchone()
    conn.close()

    if not result or not result[0]:
        return True

    try:
        last_updated = datetime.fromisoformat(result[0])
    except ValueError:
        return True

    return datetime.now() - last_updated > timedelta(hours=hours_threshold)


def get_games_needing_store_data():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT game_id FROM Games
        WHERE developer IS NULL OR tags IS NULL OR last_updated IS NULL
        OR last_updated IS NULL
        OR datetime(last_updated) < datetime('now', '-30 days')
    """)
    games_to_update = [row[0] for row in cursor.fetchall()]
    conn.close()
    return games_to_update


def get_current_playtime(steam_id, game_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT hours_played FROM UserGames WHERE user_id = ? AND game_id = ?", (steam_id, game_id))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else 0


def update_reviews_and_stats():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("SELECT game_id FROM Games WHERE review_count IS NULL OR review_count = 0")
    games_without_reviews = [row[0] for row in cursor.fetchall()]

    if not games_without_reviews:
        print("[DEBUG] All games already have review data.")
        conn.close()
        return

    print(f"[DEBUG] Updating reviews for {len(games_without_reviews)} games...")

    for game_id in games_without_reviews:
        cursor.execute("SELECT review_id, review_text FROM Reviews WHERE game_id = ?", (game_id,))
        reviews = cursor.fetchall()

        updates = []
        for review_id, text in reviews:
            sentiment = analyzer.polarity_scores(text)['compound']
            updates.append((sentiment, review_id))

        cursor.executemany("UPDATE Reviews SET sentiment = ? WHERE review_id = ?", updates)

        cursor.execute("SELECT COUNT(*), AVG(sentiment) FROM Reviews WHERE game_id = ?", (game_id,))
        count, avg_sentiment = cursor.fetchone()
        cursor.execute("UPDATE Games SET review_count = ?, average_rating = ? WHERE game_id = ?",
                       (count, avg_sentiment if avg_sentiment is not None else 0, game_id))

    conn.commit()
    conn.close()


def update_user_data(steam_id, api_key, force_update=False):
    if not force_update and not should_update_user(steam_id):
        print(f"[DEBUG] User {steam_id} was recently updated. Skipping...")
        return

    print(f"[DEBUG] Fetching data for Steam ID: {steam_id}")
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    now = datetime.now().isoformat()
    cursor.execute("""
        INSERT INTO Users (user_id, last_updated)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET last_updated=excluded.last_updated
    """, (steam_id, now))

    games_data = fetch_owned_games(steam_id)
    games_needing_store_data = get_games_needing_store_data()

    updated_games = 0
    api_calls_made = 0

    for game in games_data:
        game_id = game['appid']
        title = game.get('name', 'Unknown')
        current_hours = game.get('playtime_forever', 0) / 60
        stored_hours = get_current_playtime(steam_id, game_id)

        if abs(current_hours - stored_hours) > 0.1:
            print(f"[DEBUG] Playtime changed for {title}: {stored_hours:.1f}h -> {current_hours:.1f}h")
            updated_games += 1

        if game_id in games_needing_store_data:
            store_data = fetch_store_info(game_id)
            api_calls_made += 1
            if store_data:
                cursor.execute("""
                    INSERT INTO Games (game_id, title, tags, developer, release_date, base_price, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(game_id) DO UPDATE SET
                        title=excluded.title,
                        tags=excluded.tags,
                        developer=excluded.developer,
                        release_date=excluded.release_date,
                        base_price=excluded.base_price,
                        last_updated=excluded.last_updated
                """, (game_id, title, store_data.get('tags'), store_data.get('developer'),
                      store_data.get('release_date'), store_data.get('base_price'), now))

                dlc_entries = [(dlc_id, game_id, None, None) for dlc_id in store_data.get('dlcs', [])]
                cursor.executemany("""
                    INSERT OR IGNORE INTO DLCs (dlc_id, game_id, title, price) VALUES (?, ?, ?, ?)
                """, dlc_entries)
        else:
            cursor.execute("""
                INSERT OR IGNORE INTO Games (game_id, title) VALUES (?, ?)
            """, (game_id, title))

        cursor.execute("""
            INSERT INTO UserGames (user_id, game_id, hours_played, purchase_price, dlc_owned, last_updated)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, game_id) DO UPDATE SET
                hours_played=excluded.hours_played,
                last_updated=excluded.last_updated
        """, (steam_id, game_id, current_hours, None, 0, now))

    print(f"[DEBUG] Made {api_calls_made} API calls, updated {updated_games} games with playtime changes")

    cursor.execute("SELECT COUNT(*) FROM Friends WHERE user_id = ?", (steam_id,))
    existing_friends = cursor.fetchone()[0]

    if existing_friends == 0:
        print("[DEBUG] Fetching friends list...")
        friends = fetch_friends(steam_id)
        cursor.executemany("INSERT OR IGNORE INTO Users (user_id) VALUES (?)", [(f,) for f in friends])
        cursor.executemany("INSERT OR IGNORE INTO Friends (user_id, friend_id) VALUES (?, ?)",
                           [(steam_id, f) for f in friends])
    else:
        print(f"[DEBUG] Friends already cached ({existing_friends} friends)")

    conn.commit()
    conn.close()

    print("[DEBUG] Normalizing data...")
    normalize_data()
    print("[DEBUG] Data normalization complete.")

    print("[DEBUG] Updating reviews and game stats...")
    update_reviews_and_stats()
    print("[DEBUG] Game statistics updated.")


