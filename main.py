import os
import json
import time
import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone

# 1. Environment Variable Check
if "FIREBASE_SERVICE_ACCOUNT" not in os.environ:
    raise ValueError("❌ FIREBASE_SERVICE_ACCOUNT environment variable not found!")

# 2. Firebase Initialization
service_account_json = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])

if "private_key" in service_account_json:
    service_account_json["private_key"] = service_account_json["private_key"].replace("\\n", "\n")

cred = credentials.Certificate(service_account_json)
firebase_admin.initialize_app(cred)
db = firestore.client()

# 3. FCM Setup
SCOPES = ["https://www.googleapis.com/auth/firebase.messaging"]
credentials_fc = service_account.Credentials.from_service_account_info(
    service_account_json, scopes=SCOPES
)
project_id = service_account_json["project_id"]
LAST_FILE = "last_timestamp.txt"

def get_last_timestamp():
    """
    Sirf wahi timestamp return karega jo ya toh file mein hai 
    ya current time hai (jo bhi latest ho).
    """
    current_utc = datetime.now(timezone.utc)
    
    if os.path.exists(LAST_FILE):
        with open(LAST_FILE, "r") as f:
            ts_str = f.read().strip()
            if ts_str:
                file_ts = datetime.fromisoformat(ts_str)
                # Agar file ka time purana hai, toh current time use karo
                # Isse purane notifications skip ho jayenge
                return max(file_ts, current_utc)
    
    return current_utc

def set_last_timestamp(ts: datetime):
    """Last processed timestamp ko file mein save karein."""
    with open(LAST_FILE, "w") as f:
        f.write(ts.isoformat())

def check_firestore_and_send_notifications():
    global credentials_fc
    
    # Token Refresh logic
    if not credentials_fc.valid or credentials_fc.expired:
        credentials_fc.refresh(Request())
    access_token = credentials_fc.token

    priority_order = ["news", "events", "files"]
    last_timestamp = get_last_timestamp()
    max_timestamp = last_timestamp
    current_now = datetime.now(timezone.utc)

    for collection in priority_order:
        # Firestore query: Sirf last_timestamp ke BAAD wale docs uthao
        docs = (
            db.collection(collection)
            .where("timestamp", ">", last_timestamp)
            .order_by("timestamp", direction=firestore.Query.ASCENDING)
            .stream()
        )

        for doc in docs:
            data = doc.to_dict()
            doc_ts = data.get("timestamp")
            
            if not doc_ts:
                continue
                
            # Timestamp conversion
            if hasattr(doc_ts, "to_datetime"):
                doc_dt = doc_ts.to_datetime().replace(tzinfo=timezone.utc)
            else:
                doc_dt = doc_ts

            # FILTER: Agar document galti se current time se pehle ka hai, toh skip karein
            if doc_dt < current_now:
                print(f"⏩ Skipping old entry: {doc.id} in {collection}")
                continue

            title = data.get("title", "Something New - Tap to Read")
            
            # FCM Message Payload
            message = {
                "message": {
                    "topic": "allUsers",
                    "notification": {
                        "title": "📢 Campus Update",
                        "body": title,
                    },
                    "android": {
                        "priority": "HIGH",
                        "notification": {
                            "channel_id": "high_importance_channel",
                            "default_sound": True,
                            "default_vibrate_timings": True,
                            "sound": "default",
                        },
                    },
                    "data": {
                        "click_action": "FLUTTER_NOTIFICATION_CLICK",
                        "collection": collection,
                        "doc_id": doc.id,
                    },
                }
            }

            url = f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; UTF-8",
            }

            # Sending Notification
            response = requests.post(url, headers=headers, data=json.dumps(message))
            
            if response.status_code == 200:
                print(f"✅ Sent: {collection} → {title}")
            else:
                print(f"❌ Failed: {collection} → {title} | Status: {response.status_code}")
            
            # Update max_timestamp for tracking
            if doc_dt > max_timestamp:
                max_timestamp = doc_dt

    set_last_timestamp(max_timestamp)

# --- Execution ---
CHECK_INTERVAL = 60 # Har 1 minute mein check karega

print(f"🚀 Watcher Started at {datetime.now(timezone.utc)}")
print(f"📅 Monitoring for documents created AFTER this moment.")

while True:
    try:
        check_firestore_and_send_notifications()
    except Exception as e:
        print(f"⚠️ Error occurred: {e}")
    
    time.sleep(CHECK_INTERVAL)
