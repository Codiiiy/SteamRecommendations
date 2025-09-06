import json
import os

def load_config():
    config_file = 'config.json'
    
    if not os.path.exists(config_file):
        create_config_template()
        print(f"Created {config_file} template. Please add your Steam API key and run again.")
        return None
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        api_key = config.get('steam_api_key')
        if not api_key or api_key == 'your_steam_api_key_here':
            print("Please add your Steam API key to config.json")
            return None
            
        return config
    except json.JSONDecodeError:
        print("Invalid JSON in config.json")
        return None
    except Exception as e:
        print(f"Error reading config: {e}")
        return None

def create_config_template():
    template = {
        "steam_api_key": "your_steam_api_key_here",
        "default_steam_id": "",
        "database_name": "game_library.db"
    }
    
    with open('config.json', 'w', encoding='utf-8') as f:
        json.dump(template, f, indent=2)

def get_api_key():
    config = load_config()
    return config['steam_api_key'] if config else None