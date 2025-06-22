
import requests
import json

# Configuration: replace with your actual credentials
USER = "system5@mail.ru"
TOKEN = "d1a34f9989d7b33d5a55e2a10d9b91a6"
API_URL = "https://ads-api.ru/main/api"

# Query parameters
params = {
    "user": USER,
    "token": TOKEN,
    "city": "Москва",
    "metro": "Текстильщики",
    "limit": 10,
    "format": "json",
    "nedvigimost_type": 1
}

# Perform the request
response = requests.get(API_URL, params=params)
response.raise_for_status()  # Raise an exception if the request failed

# Parse JSON response
data = response.json()

# Print basic info for each ad
for ad in data.get("data", []):
    print(f"{ad['id']}: {ad.get('source')} | {ad.get('title')} | {ad.get('price')} ₽ | {ad.get('city')}")

# Save full response to a JSON file
with open("ads_tekstilshiki.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("\nSaved full response to ads_tekstilshiki.json")
