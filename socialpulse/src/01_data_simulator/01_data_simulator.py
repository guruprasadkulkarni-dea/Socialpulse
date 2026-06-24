# Databricks notebook source
pip install faker

# COMMAND ----------

# MAGIC %run "../00_setup/common_variables"

# COMMAND ----------

from faker import Faker
import random
import json
import uuid
from datetime import datetime, timedelta
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd
import zoneinfo

fake = Faker()
random.seed(42)

# ── Config ──────────────────────────────────────────────
LANDING_PATH    = f"abfss://landing@{storage_account}.dfs.core.windows.net"
BRONZE_PATH     = f"abfss://bronze@{storage_account}.dfs.core.windows.net"

# Simulation config
NUM_USERS       = 500
NUM_ADS         = 50
NUM_CAMPAIGNS   = 10
EVENTS_PER_BATCH= 2000   # events per micro-batch
NUM_BATCHES     = 18    # total batches to simulate
MIN_USER_IN_1_BATCH = 100
MAX_USER_IN_1_BATCH = 200
MIN_AD_IN_1_BATCH = 20
MAX_AD_IN_1_BATCH = 30
MIN_ANAMOLY_IN_1_BATCH  = 0
MAX_ANAMOLY_IN_1_BATCH  = 0.15
ANAMOLY_ARR = ['spike','suspicious','bot']
BOT_ANAMOLY_ARR = ['null_ip','multiple_ip']

print("✅ Config ready")

# COMMAND ----------

PLATFORMS     = ["iOS", "Android", "Web", "Desktop"]
DEVICE_TYPES  = ["Mobile", "Tablet", "Laptop", "Desktop"]
EVENT_TYPES_WEIGHTED = [
    ("page_view", 30,40),
    ("story_view",30,40),
    ("video_play", 10,20),
    ("like", 5,15),
    ("click", 5,15),
    ("share", 0,10),
    ("comment", 0,10),
    ("follow", 0,10),
    ("video_complete", 0,10),
    ("ad_skip",5,10),
    ("ad_impression",5,10),
    ("ad_click",5,10),
    ("post_create",1,5),  ("profile_view",3,8),
    ("notification_click",5,10), ("app_open",1,8), ("app_close",1,8)
]
CONTENT_TYPES = ["post", "video", "story", "reel", "ad", "article"]
COUNTRIES     = ["IN", "US", "UK", "DE", "FR", "BR", "JP", "AU"]
TIERS         = ["free", "premium", "business"]

print(f"✅ {len(EVENT_TYPES_WEIGHTED)} event types ready")

# COMMAND ----------

def generate_users(n=NUM_USERS):
    users = []
    for i in range(1, n + 1):
        signup_date = fake.date_between(
            start_date="-3y", end_date="today"
        )
        users.append({
            "user_id"       : f"USR{str(i).zfill(5)}",
            "username"      : fake.user_name(),
            "email"         : fake.email(),
            "full_name"     : fake.name(),
            "age"           : random.randint(18, 65),
            "gender"        : random.choice(["M", "F", "Other"]),
            "country"       : random.choice(COUNTRIES),
            "city"          : fake.city(),
            "platform"      : random.choice(PLATFORMS),
            "tier"          : random.choice(TIERS),
            "signup_date"   : str(signup_date),
            "is_active"     : random.choice([True, True, True, False]),
            "followers"     : random.randint(0, 100000),
            "following"     : random.randint(0, 5000),
            "posts_count"   : random.randint(0, 1000),
            "updated_at": str(fake.date_time_between(start_date=signup_date,end_date="now"))
        })
    return users

users = generate_users()

# Save as CSV to ADLS landing/users/
users_df = spark.createDataFrame(users)
users_df.coalesce(1).write \
    .mode("append") \
    .option("header", "true") \
    .csv(f"{LANDING_PATH}/users/")

print(f"✅ {len(users)} users written to landing/users/")
users_df.show(3)

# COMMAND ----------

def generate_ads(n=NUM_ADS):
    ads = []
    for i in range(1, n + 1):
        start_date = fake.date_between(start_date="-6m", end_date="today")
        end_date   = start_date + timedelta(days=random.randint(7, 90))
        ads.append({
            "ad_id"          : f"AD{str(i).zfill(4)}",
            "campaign_id"    : f"CAMP{str(random.randint(1, NUM_CAMPAIGNS)).zfill(3)}",
            "ad_name"        : fake.catch_phrase(),
            "ad_type"        : random.choice([
                                   "banner", "video", 
                                   "carousel", "story"
                               ]),
            "target_country" : random.choice(COUNTRIES),
            "target_tier"    : random.choice(TIERS + ["all"]),
            "budget_usd"     : round(random.uniform(100, 50000), 2),
            "spend_usd"      : round(random.uniform(0, 100), 2),
            "start_date"     : str(start_date),
            "end_date"       : str(end_date),
            "is_active"      : random.choice([True, False]),
            "cpc"            : round(random.uniform(0.1, 5.0), 2),
            "cpm"            : round(random.uniform(1, 20), 2),
            "updated_at"     : str(fake.date_time_between(start_date=start_date,end_date=end_date))
        })
    return ads

ads = generate_ads()

# Save as CSV to ADLS landing/ads/
ads_df = spark.createDataFrame(ads)
ads_df.coalesce(1).write \
    .mode("append") \
    .option("header", "true") \
    .csv(f"{LANDING_PATH}/ads/")

print(f"✅ {len(ads)} ads written to landing/ads/")
ads_df.show(3)

# COMMAND ----------

def generate_event_batch(
    batch_id, 
    user_ids, 
    ad_ids
):
    
    raw_weights = {}
    for event, min_w, max_w in EVENT_TYPES_WEIGHTED:
        raw_weights[event] = random.uniform(min_w, max_w)
    
    total_raw_sum      = sum(raw_weights.values())
    normalized_weights = {event: (val / total_raw_sum) for event, val in raw_weights.items()}
    user_anamoly_perc  = random.uniform(MIN_ANAMOLY_IN_1_BATCH, MAX_ANAMOLY_IN_1_BATCH)
    if user_anamoly_perc > 0:
        user_anamoly_count = int(round(user_anamoly_perc * len(user_ids)))
        user_anamoly_ids   = random.sample(user_ids, user_anamoly_count)
    else:
        user_anamoly_ids = []
    
    #random.shuffle(event_pool)
    events = []
    event_num = 0
    
    while event_num < EVENTS_PER_BATCH:
        sessions_per_user = random.randint(1, 5)
        user_id      = random.choice(user_ids)
        anamoly_res  = []
        bot_case     = ''
        anamoly_flag = False
        multiplt_ip_flag = False
        if len(user_anamoly_ids) > 0:
            if user_id in user_anamoly_ids:
                anamoly_flag = True
                anamoly_res = random.sample(ANAMOLY_ARR,random.randint(1,3))
        if anamoly_flag and 'suspicious' in anamoly_res:
            country = random.sample(COUNTRIES,random.randint(1,3))
        else:
            country = random.choice(COUNTRIES)
        if anamoly_flag and 'bot' in anamoly_res:
            bot_case = random.choice(BOT_ANAMOLY_ARR)
            if bot_case == 'multiplte_ip':
                multiplt_ip_flag = True
            else:
                ip_address = None
        else:
            ip_address = fake.ipv4()

        city = fake.city()
        device_type = random.choice(DEVICE_TYPES)
        platform  = random.choice(PLATFORMS)
        user_agent = fake.user_agent()
        for _ in range(sessions_per_user):
            if event_num >= EVENTS_PER_BATCH:
                break
            
            session_id = str(uuid.uuid4())[:8]  # shared per session
            events_per_session = random.randint(10, 20)
            content_id = f"CONT{random.randint(1,10000):05d}"
            content_type = random.choice(CONTENT_TYPES)
            event_pool = []
            for event_type, weight_percent in normalized_weights.items():
                count = int(round(weight_percent * events_per_session))
                event_pool.extend([event_type] * count)
            while len(event_pool) < events_per_session:
                event_pool.append(random.choice(list(raw_weights.keys())))
            while len(event_pool) > events_per_session:
                event_pool.pop()
            random.shuffle(event_pool)
            for i in range(len(event_pool)):
                if event_num >= EVENTS_PER_BATCH:
                    break
                #print('event_num----> ',event_num)
                event_type = event_pool[i]
                if anamoly_flag and (('spike' in anamoly_res) or ('bot') in anamoly_res):
                    mod_start_date = datetime.now(zoneinfo.ZoneInfo("Asia/Kolkata")).replace(second=0, microsecond=0) + timedelta(seconds = 1)
                else:
                    mod_start_date = datetime.now(zoneinfo.ZoneInfo("Asia/Kolkata")).replace( microsecond=0) - timedelta(hours = random.randint(1,24),minutes=random.randint(1,60),seconds=random.randint(1,60))
                is_ad_event = event_type in ["ad_impression", "ad_click", "ad_skip"]
                if multiplt_ip_flag:
                    ip_address = fake.ipv4()
                event_obj = {
                    "event_id"      : str(uuid.uuid4()),
                    "user_id"       : user_id,
                    "session_id"    : session_id,
                    "event_type"    : event_type,
                    "content_id"    : content_id,
                    "content_type"  : content_type,
                    "ad_id"         : random.choice(ad_ids) if is_ad_event else None,
                    "platform"      : platform,
                    "device_type"   : device_type,
                    "country"       : random.choice(country) if anamoly_flag else country,
                    "city"          : city,
                    "duration_ms"   : random.randint(100, 30000) if event_type in ["video_play","video_complete","video_pause"] else None,
                    "scroll_depth"  : round(random.uniform(0, 1), 2) if event_type == "page_view" else None,
                    "timestamp"     : str(mod_start_date),
                    "batch_id"      : batch_id,
                    "user_agent"    : user_agent,
                    "ip_address"    : ip_address,
                    "updated_at"    : str(fake.date_time_between(start_date=mod_start_date,end_date="now"))
                }
                
                events.append(event_obj)
                event_num += 1
                #print(f'record {event_num} added---->')
    #print('events ----> ',events[100])
    random.shuffle(events)
    return events

print("✅ Event generator ready")

# COMMAND ----------

user_ids = [u["user_id"] for u in users]
ad_ids   = [a["ad_id"]   for a in ads]

print(f"🚀 Starting simulation: {NUM_BATCHES} batches × {EVENTS_PER_BATCH} events")
print(f"   Total events: {NUM_BATCHES * EVENTS_PER_BATCH:,}\n")

for batch_id in range(1, NUM_BATCHES + 1):
    user_ids_subset = random.sample(user_ids, random.randint(MIN_USER_IN_1_BATCH, MAX_USER_IN_1_BATCH))
    ad_ids_subset = random.sample(ad_ids, random.randint(MIN_AD_IN_1_BATCH, MAX_AD_IN_1_BATCH))
    # Generate batch
    events      = generate_event_batch(batch_id, user_ids_subset, ad_ids_subset)
    #print('events for 101 record ----> ',events[101])
    #print(f"event type: {type(events)}")
    #print(f"Events count: {len(events)}")
    #print(f"First event type: {type(events[0])}")
    #print(f"First event: {events[0]}")
    #print('events----> ',events)
    events_pdf  = pd.DataFrame(events)
    events_df   = spark.createDataFrame(events_pdf)
    # Write as JSON to landing/events/
    # Each batch = separate folder (simulates streaming arrival)
    events_df.coalesce(1).write.mode("append").json( \
    f"{LANDING_PATH}/events/batch_{str(batch_id).zfill(3)}/")


    print(
        f"  ✅ Batch {batch_id:02d}/{NUM_BATCHES} "
        f"→ {len(events)} events "
        f"→ landing/events/batch_{str(batch_id).zfill(3)}/"
    )

print(f"\n🎉 Simulation complete!")
print(f"   Total events : {NUM_BATCHES * EVENTS_PER_BATCH:,}")
print(f"   Total users  : {NUM_USERS:,}")
print(f"   Total ads    : {NUM_ADS:,}")

# COMMAND ----------

# Verify all data in landing zone
print("📁 Landing Zone Contents:\n")

for container in ["events", "users", "ads"]:
    path   = f"{LANDING_PATH}/{container}/"
    files  = dbutils.fs.ls(path)
    size   = sum([f.size for f in files])
    print(f"  landing/{container}/")
    print(f"  → {len(files)} file(s), {size/1024:.1f} KB")

# Quick count verification
print("\n📊 Record Counts:\n")

events_count = spark.read.json(f"{LANDING_PATH}/events/batch_*/*.json").count()
users_count  = spark.read \
    .option("header","true") \
    .csv(f"{LANDING_PATH}/users/*.csv") \
    .count()
ads_count    = spark.read \
    .option("header","true") \
    .csv(f"{LANDING_PATH}/ads/*.csv") \
    .count()

print(f"  Events : {events_count:,}")
print(f"  Users  : {users_count:,}")
print(f"  Ads    : {ads_count:,}")

# Sample event
print("\n📋 Sample Event:\n")
spark.read.json(f"{LANDING_PATH}/events/batch_*/*.json").show(1, truncate=False, vertical=True)