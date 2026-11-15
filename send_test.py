import os
import urllib.parse
import urllib.request

token = os.environ["TELEGRAM_TOKEN"]
chat_id = os.environ["TELEGRAM_CHAT_ID"]

data = urllib.parse.urlencode({
    "chat_id": chat_id,
    "text": "Bonjour ! Le bot Ça te concerne fonctionne.",
}).encode()

url = f"https://api.telegram.org/bot{token}/sendMessage"
with urllib.request.urlopen(url, data=data) as r:
    print("Telegram a répondu :", r.status)
