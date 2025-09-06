from database import setup_database, update_user_data
from recommendation_engine import recommend
import sqlite3
import pandas as pd

DB_FILE = "game_library.db"
FIXED_STEAM_ID = 76561198117995382 

def main():
    from config_key import get_api_key
    API_KEY = get_api_key()
    if not API_KEY:
        print("[ERROR] API key not found. Exiting.")
        exit(1)

    setup_database()

    try:
        steam_id = FIXED_STEAM_ID
        force_update = False  

        try:
            update_user_data(steam_id, API_KEY, force_update)

            conn = sqlite3.connect(DB_FILE)
            usrgames = pd.read_sql("SELECT * FROM UserGames", conn)
            friends = pd.read_sql("SELECT * FROM Friends", conn)
            conn.close()

            print("[DEBUG] Generating recommendations...")
            recommendations = recommend(steam_id, top_n=2)

            print(f"\nRecommended Games for Steam ID {steam_id}:")
            print("-" * 50)
            for idx, row in recommendations.iterrows():
                print(f"• {row['title']} (Game ID: {row['game_id']})")

        except Exception as e:
            print(f"[ERROR] Error processing Steam ID {steam_id}: {e}")

    except KeyboardInterrupt:
        print("\n[DEBUG] Exiting program.")

if __name__ == "__main__":
    main()
