import discord
from discord import app_commands
from discord.ext import tasks
from flask import Flask
from threading import Thread
import os
import logging
import asyncpg
import asyncio
import random
import json
from dotenv import load_dotenv
from typing import Optional, List, Dict, Any
from datetime import datetime, date, time, timezone, timedelta # CRITICAL FIX 1: Imported timedelta
from collections import Counter

# --- START OF REPLIT 24-7 H.A.C.K ---
from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "PetQuest bot is alive!"

def run_web_server():
    app.run(host='0.0.0.0', port=8080)

def start_web_server_thread():
    t = Thread(target=run_web_server)
    t.start()

# --- Configuration & Logging ---
load_dotenv()
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

try:
    TOKEN = os.environ['DISCORD_TOKEN']
    DATABASE_URL = os.environ['DATABASE_URL']
except KeyError:
    log.critical("FATAL: DISCORD_TOKEN or DATABASE_URL not set.")
    exit(1)

# --- Game Design Constants ---

# Core Loop
NEEDS_FEED = 3
NEEDS_PLAY = 2
XP_PER_DAY = 50
XP_PENALTY = 10 # XP loss on failure

# Scalable evolution logic
STAGE_ORDER = ["egg", "baby", "teen", "adult", "elder"]
STAGES = {
    "egg": "🥚 Egg",
    "baby": "🐣 Baby",
    "teen": "🐤 Teen",
    "adult": "🦊 Adult",
    "elder": "⭐ Elder"
}
EVOLVE_THRESHOLDS = {
    "egg": 100,
    "baby": 500,
    "teen": 2000,
    "adult": 5000,
    "elder": float('inf')
}

# Pet Personalities
PERSONALITIES = {
    "curious": {
        "feed_response": ["Yum! What else is there to eat?", "Tasty! Can I have more?"],
        "play_response": ["That was fun! What's next?", "Woo! Let's do that again!"],
    },
    "lazy": {
        "feed_response": ["Thanks... *munches slowly*", "Food good... *falls asleep*"],
        "play_response": ["Do we have to?", "*halfhearted bounce*"],
    },
    "energetic": {
        "feed_response": ["YUM YUM YUM!", "MORE MORE MORE!"],
        "play_response": ["BEST. GAME. EVER!", "AGAIN AGAIN AGAIN!"],
    }
}

# POLISH 2: Item definitions
ITEM_CATALOG = {
    "shiny_pebble": {"name": "Shiny Pebble", "emoji": "🪨"},
    "apple": {"name": "Apple", "emoji": "🍎"},
    "ancient_coin": {"name": "Ancient Coin", "emoji": "🪙"},
    "crystal_shard": {"name": "Crystal Shard", "emoji": "💎"},
}

# CRITICAL BUG 4: Expanded adventure content
ADVENTURES = {
    "easy": {
        "name": "🌲 Peaceful Woods",
        "xp_reward": 15,
        "min_party": 1,
        "encounters": [
            {
                "text": "You find a friendly squirrel!",
                "choices": [
                    {"id": "share", "emoji": "🍎", "text": "Share food", "success_rate": 1.0, "reward": "shiny_pebble", "result": "The squirrel thanks you and drops a Shiny Pebble!"},
                    {"id": "wave", "emoji": "👋", "text": "Wave hello", "success_rate": 1.0, "reward": "xp_bonus", "result": "The squirrel waves back! (+5 XP)"}
                ]
            },
            {
                "text": "A patch of tasty-looking berries blocks your path.",
                "choices": [
                    {"id": "eat", "emoji": "😋", "text": "Eat them! (50% success)", "success_rate": 0.5, "reward": "apple", "result": "Delicious! You find an extra Apple!", "failure": "Yuck! You get a stomach ache and waste time."},
                    {"id": "pass", "emoji": "🚶", "text": "Walk around", "success_rate": 1.0, "reward": "nothing", "result": "You safely navigate around the patch."}
                ]
            },
            {
                "text": "You find a sparkling stream.",
                "choices": [
                    {"id": "drink", "emoji": "💧", "text": "Drink", "success_rate": 1.0, "reward": "nothing", "result": "Refreshing!"},
                    {"id": "pan", "emoji": "💎", "text": "Look for treasure", "success_rate": 0.3, "reward": "shiny_pebble", "result": "You found another Shiny Pebble!", "failure": "You just got your hands wet."}
                ]
            }
        ]
    },
    "medium": {
        "name": "⛰️ Rocky Mountain",
        "xp_reward": 30,
        "min_party": 2,
        "encounters": [
            {
                "text": "A rockslide blocks your path!",
                "choices": [
                    {"id": "push", "emoji": "💪", "text": "Push through (60% success)", "success_rate": 0.6, "reward": "ancient_coin", "result": "You cleared the path and found an Ancient Coin!", "failure": "The rocks were too heavy... you have to go around."},
                    {"id": "around", "emoji": "🚶", "text": "Go around (90% success)", "success_rate": 0.9, "reward": "shiny_pebble", "result": "You safely found a new path and spotted a Shiny Pebble.", "failure": "You got lost going around and found nothing."}
                ]
            },
            {
                "text": "A grumpy goat blocks the narrow bridge.",
                "choices": [
                    {"id": "bribe", "emoji": "🍎", "text": "Bribe with Apple (70% success)", "success_rate": 0.7, "reward": "nothing", "result": "The goat happily munches the apple and lets you pass.", "failure": "The goat eats the apple AND headbutts you."},
                    {"id": "sneak", "emoji": "🤫", "text": "Sneak past (30% success)", "success_rate": 0.3, "reward": "ancient_coin", "result": "You snuck by! You also found an Ancient Coin the goat was guarding!", "failure": "The goat saw you immediately and chased you back."}
                ]
            },
            {
                "text": "You find a mysterious cave entrance.",
                "choices": [
                    {"id": "enter", "emoji": "🔦", "text": "Enter (40% success)", "success_rate": 0.4, "reward": "crystal_shard", "result": "Inside, a faint light reveals a Crystal Shard!", "failure": "It's too dark! You stumble out and drop a Shiny Pebble."},
                    {"id": "skip", "emoji": "❌", "text": "Skip it", "success_rate": 1.0, "reward": "nothing", "result": "Too risky. You move on."}
                ]
            }
        ]
    },
    "hard": {
        "name": "🌋 Volcanic Lair",
        "xp_reward": 50,
        "min_party": 3,
        "encounters": [
            {
                "text": "A small dragon hisses at you!",
                "choices": [
                    {"id": "fight", "emoji": "⚔️", "text": "Fight! (50% success)", "success_rate": 0.5, "reward": "crystal_shard", "result": "You scared it off and found a Crystal Shard!", "failure": "It was too hot! You fled with nothing."},
                    {"id": "negotiate", "emoji": "🤝", "text": "Offer a Shiny Pebble (70% success)", "success_rate": 0.7, "reward": "ancient_coin", "result": "It likes shiny things! It let you pass and dropped an Ancient Coin.", "failure": "It hated your gift and scorched your pebble."},
                    {"id": "run", "emoji": "🏃", "text": "Run away (90% success)", "success_rate": 0.9, "reward": "nothing", "result": "You got away safely.", "failure": "You tripped and it singed your backpack."}
                ]
            },
            {
                "text": "A river of lava blocks your path.",
                "choices": [
                    {"id": "jump", "emoji": "🤸", "text": "Jump it (30% success)", "success_rate": 0.3, "reward": "crystal_shard", "result": "You made it! And spotted a Crystal Shard on the other side!", "failure": "Too wide! You barely avoid falling in."},
                    {"id": "bridge", "emoji": "🌉", "text": "Find a bridge (60% success)", "success_rate": 0.6, "reward": "ancient_coin", "result": "You found a rickety rock bridge and crossed safely, finding an Ancient Coin.", "failure": "The bridge collapsed! You made it back, but lost time."}
                ]
            },
            {
                "text": "You've reached the treasure horde!",
                "choices": [
                    {"id": "grab", "emoji": "✋", "text": "Grab and run (50% success)", "success_rate": 0.5, "reward": "crystal_shard", "result": "You grabbed a huge Crystal Shard and got out!", "failure": "The dragon returned! You dropped everything and ran."},
                    {"id": "sneak", "emoji": "🤫", "text": "Sneak one coin (90% success)", "success_rate": 0.9, "reward": "ancient_coin", "result": "Slow and steady. You nabbed an Ancient Coin without being noticed.", "failure": "You made too much noise and had to flee."}
                ]
            }
        ]
    }
}


# --- Database Setup ---
_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, max_size=20)
        if _pool is None:
            raise Exception("Failed to create database pool.")
    return _pool

async def init_db():
    """
    Initializes all database tables required for Beta v1.
    """
    pool = await get_pool()
    async with pool.acquire() as con:
        # --- Pet Table ---
        await con.execute("""
            CREATE TABLE IF NOT EXISTS pets (
                server_id BIGINT PRIMARY KEY,
                pet_channel_id BIGINT NOT NULL,
                pet_name TEXT DEFAULT 'Our Pet',
                stage TEXT DEFAULT 'egg',
                xp INTEGER DEFAULT 0,
                streak INTEGER DEFAULT 0,
                personality TEXT DEFAULT 'curious'
            );
        """)
        # --- Daily Needs Table ---
        await con.execute("""
            CREATE TABLE IF NOT EXISTS daily_needs (
                id SERIAL PRIMARY KEY,
                server_id BIGINT REFERENCES pets(server_id) ON DELETE CASCADE,
                need_date DATE NOT NULL,
                feed_users BIGINT[] DEFAULT ARRAY[]::BIGINT[],
                play_users BIGINT[] DEFAULT ARRAY[]::BIGINT[],
                UNIQUE(server_id, need_date)
            );
        """)
        # --- MVP Tracking Table ---
        await con.execute("""
            CREATE TABLE IF NOT EXISTS user_contributions (
                server_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                feeds_contributed INTEGER DEFAULT 0,
                plays_contributed INTEGER DEFAULT 0,
                PRIMARY KEY (server_id, user_id)
            );
        """)
        # --- Adventure Table ---
        await con.execute("""
            CREATE TABLE IF NOT EXISTS adventures (
                id SERIAL PRIMARY KEY,
                server_id BIGINT REFERENCES pets(server_id),
                status TEXT DEFAULT 'forming', -- 'forming', 'active', 'completed', 'failed'
                difficulty TEXT NOT NULL,
                party_members BIGINT[] DEFAULT ARRAY[]::BIGINT[],
                encounter_log JSONB DEFAULT '[]',
                encounter_index INTEGER DEFAULT 0,
                votes JSONB DEFAULT '{}',
                message_id BIGINT, -- CRITICAL BUG 2: Store message ID for reliable edits
                created_at TIMESTAMPTZ DEFAULT NOW(),
                expires_at TIMESTAMPTZ NOT NULL
            );
        """)
        # --- Analytics Table ---
        await con.execute("""
            CREATE TABLE IF NOT EXISTS analytics (
                id SERIAL PRIMARY KEY,
                event_type TEXT NOT NULL,
                server_id BIGINT,
                user_id BIGINT,
                metadata JSONB,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        # --- POLISH 2: Inventory Table ---
        await con.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                server_id BIGINT NOT NULL,
                item_id TEXT NOT NULL,
                quantity INTEGER DEFAULT 1,
                PRIMARY KEY (server_id, item_id)
            );
        """)
    log.info("All database tables initialized.")

# --- Helper Functions ---

async def track_event(event_type: str, server_id: int, user_id: int = None, metadata: dict = None):
    """Analytics tracker."""
    try:
        pool = await get_pool()
        await pool.execute(
            "INSERT INTO analytics (event_type, server_id, user_id, metadata) VALUES ($1, $2, $3, $4)",
            event_type, server_id, user_id, json.dumps(metadata) if metadata else None
        )
    except Exception as e:
        log.error(f"Analytics tracking failed: {e}")

async def add_item_to_inventory(con: asyncpg.Connection, server_id: int, item_id: str, quantity: int = 1) -> Optional[Dict]:
    """
    POLISH 2: Adds an item to a server's inventory.
    """
    if item_id not in ITEM_CATALOG:
        log.warning(f"Attempted to add invalid item: {item_id}")
        return None
    
    try:
        await con.execute("""
            INSERT INTO inventory (server_id, item_id, quantity)
            VALUES ($1, $2, $3)
            ON CONFLICT (server_id, item_id)
            DO UPDATE SET quantity = inventory.quantity + EXCLUDED.quantity
        """, server_id, item_id, quantity)
        
        return ITEM_CATALOG[item_id]
        
    except Exception as e:
        log.error(f"Failed to add item to inventory: {e}")
        return None

def get_time_until_reset() -> str:
    """Helper for the /status command UI."""
    now_utc = datetime.now(timezone.utc)
    reset_time = (now_utc + timedelta(days=1)).replace(hour=0, minute=1, second=0, microsecond=0)
    delta = reset_time - now_utc
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{hours}h {minutes}m"

async def get_or_create_needs(con: asyncpg.Connection, server_id: int, target_date: date) -> asyncpg.Record:
    """Atomically gets or creates the daily_needs row for a pet."""
    needs = await con.fetchrow("SELECT * FROM daily_needs WHERE server_id = $1 AND need_date = $2", server_id, target_date)
    if needs:
        return needs
    try:
        return await con.fetchrow(
            "INSERT INTO daily_needs (server_id, need_date) VALUES ($1, $2) RETURNING *",
            server_id, target_date
        )
    except asyncpg.exceptions.UniqueViolationError:
        return await con.fetchrow("SELECT * FROM daily_needs WHERE server_id = $1 AND need_date = $2", server_id, target_date)

def get_next_stage(current_stage: str) -> Optional[str]:
    """Gets the next stage in the predefined order."""
    try:
        current_index = STAGE_ORDER.index(current_stage)
        if current_index < len(STAGE_ORDER) - 1:
            return STAGE_ORDER[current_index + 1]
    except ValueError:
        pass
    return None

async def grant_xp_and_evolve(con: asyncpg.Connection, server_id: int, pet: asyncpg.Record, xp_to_grant: int) -> Optional[str]:
    """Grants XP and checks for evolution using the scalable logic."""
    new_xp = pet['xp'] + xp_to_grant
    new_stage = pet['stage']
    
    current_threshold = EVOLVE_THRESHOLDS.get(pet['stage'])
    
    if current_threshold and new_xp >= current_threshold:
        next_stage_key = get_next_stage(pet['stage'])
        if next_stage_key:
            new_stage = next_stage_key
    
    await con.execute(
        "UPDATE pets SET xp = $1, stage = $2 WHERE server_id = $3",
        new_xp, new_stage, server_id
    )
    
    if new_stage != pet['stage']:
        await track_event("evolution", server_id, metadata={"stage": new_stage})
        return new_stage # Return new stage name for announcement
    return None

# --- Bot Client Setup ---
intents = discord.Intents.default()
intents.guilds = True
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# --- Bot Events ---
@client.event
async def on_ready():
    log.info(f'Logged in as {client.user} (ID: {client.user.id})')
    await init_db()
    
    # Re-register persistent views
    pool = await get_pool()
    async with pool.acquire() as con:
        forming_adventures = await con.fetch("SELECT * FROM adventures WHERE status = 'forming'")
        for adv in forming_adventures:
            # Check if expired
            if adv['expires_at'] < datetime.now(timezone.utc):
                await con.execute("UPDATE adventures SET status = 'failed' WHERE id = $1", adv['id'])
                await track_event("adventure_expired", adv['server_id'], metadata={"adv_id": adv['id'], "reason": "startup_check"})
            else:
                view = AdventurePartyView(adv['id'], adv['difficulty'])
                client.add_view(view, message_id=adv['message_id'])

    log.info("Syncing command tree...")
    await tree.sync()
    log.info("Command tree synced.")
    
    # Start all background tasks
    daily_needs_reset_loop.start()
    check_adventure_timeouts.start()
    log.info("Bot ready and background loops started.")

# --- Phase 1 Commands (with Quick Wins) ---

@tree.command(name="setup", description="Set up the community pet in this channel.")
@app_commands.describe(channel="The channel where the pet will live.")
@app_commands.checks.has_permissions(administrator=True)
async def setup(interaction: discord.Interaction, channel: discord.TextChannel):
    if not interaction.guild:
        return
    pool = await get_pool()
    try:
        new_personality = random.choice(list(PERSONALITIES.keys()))
        await pool.execute(
            """
            INSERT INTO pets (server_id, pet_channel_id, personality) VALUES ($1, $2, $3)
            ON CONFLICT (server_id) DO UPDATE SET 
                pet_channel_id = EXCLUDED.pet_channel_id,
                personality = EXCLUDED.personality
            """,
            interaction.guild.id, channel.id, new_personality
        )
        await interaction.response.send_message(f"Success! 🦊 Our pet will now live in {channel.mention}.", ephemeral=True)
        await channel.send("Hello everyone! I'm your new community pet. Let's grow together! 🥚")
        await track_event("setup", interaction.guild.id, interaction.user.id)
    except Exception as e:
        log.error(f"Error in /setup: {e}")
        await interaction.response.send_message("An error occurred during setup.", ephemeral=True)

@tree.command(name="status", description="Check our pet's progress and today's needs.")
async def status(interaction: discord.Interaction):
    if not interaction.guild_id:
        return
    
    pool = await get_pool()
    async with pool.acquire() as con:
        pet = await con.fetchrow("SELECT * FROM pets WHERE server_id = $1", interaction.guild_id)
        if not pet:
            await interaction.response.send_message("Use `/setup` to get a pet first!", ephemeral=True)
            return

        today = datetime.now(timezone.utc).date()
        needs = await get_or_create_needs(con, interaction.guild_id, today)

        # --- Build the UI Embed ---
        stage_name = STAGES.get(pet['stage'], pet['stage'])
        xp_needed = EVOLVE_THRESHOLDS.get(pet['stage'], pet['xp'] + 1)
        progress_raw = (pet['xp'] / xp_needed) if xp_needed > 0 else 1.0
        xp_progress = min(progress_raw * 100, 100) # Cap at 100%
        
        title = f"{stage_name} {pet['pet_name']} | {pet['xp']} XP ({xp_progress:.0f}%)"
        if pet.get('streak', 0) > 1:
            title += f" | 🔥 {pet['streak']} Day Streak!"
        
        embed = discord.Embed(title=title, color=discord.Color.blue())
        embed.description = f"Today's Needs (Resets in {get_time_until_reset()})"
        
        # 1. Feed Need
        feeds_done = len(needs['feed_users'])
        feed_bar = "█" * feeds_done + "░" * (NEEDS_FEED - feeds_done)
        feed_helpers = ", ".join([f"<@{uid}>" for uid in needs['feed_users']]) or "None yet!"
        # POLISH 1: Better Embed Formatting
        embed.add_field(
            name="🍎 Daily Feeding",
            value=f"`{feed_bar}` {feeds_done}/{NEEDS_FEED}\n(Thanks: {feed_helpers})",
            inline=False
        )

        # 2. Play Need
        plays_done = len(needs['play_users'])
        play_bar = "█" * plays_done + "░" * (NEEDS_PLAY - plays_done)
        play_helpers = ", ".join([f"<@{uid}>" for uid in needs['play_users']]) or "None yet!"
        embed.add_field(
            name="🎾 Daily Playing",
            value=f"`{play_bar}` {plays_done}/{NEEDS_PLAY}\n(Thanks: {play_helpers})",
            inline=False
        )
        
        # 3. Adventure Need
        embed.add_field(
            name="🗺️ Daily Adventure",
            value=f"Use `/adventure_start`!",
            inline=False
        )

        # 4. Tip & MVP
        tips = []
        if feeds_done < NEEDS_FEED: tips.append(f"We need {NEEDS_FEED - feeds_done} more feed(s)!")
        if plays_done < NEEDS_PLAY: tips.append(f"We need {NEEDS_PLAY - plays_done} more play(s)!")
        if not tips: tips.append("All needs met! Great job, team!")
        
        top_contributor = await con.fetchrow("""
            SELECT user_id, (feeds_contributed + plays_contributed) as total
            FROM user_contributions
            WHERE server_id = $1
            ORDER BY total DESC
            LIMIT 1
        """, interaction.guild_id)

        footer_text = f"💡 Tip: {' '.join(tips)}"
        if top_contributor:
            user = client.get_user(top_contributor['user_id']) or f"<@{top_contributor['user_id']}>"
            footer_text += f" | 🏆 MVP: {user} ({top_contributor['total']} total)"

        embed.set_footer(text=footer_text)
        await interaction.response.send_message(embed=embed)

@tree.command(name="feed", description="Feed the pet. (Counts 1x per day, per user)")
async def feed(interaction: discord.Interaction):
    if not interaction.guild_id:
        return
    
    pool = await get_pool()
    async with pool.acquire() as con:
        pet = await con.fetchrow("SELECT personality FROM pets WHERE server_id = $1", interaction.guild_id)
        if not pet:
            await interaction.response.send_message("Use `/setup` first!", ephemeral=True)
            return

        today = datetime.now(timezone.utc).date()
        needs = await get_or_create_needs(con, interaction.guild_id, today)
        
        if interaction.user.id in needs['feed_users']:
            await interaction.response.send_message("Thanks! But we need someone *else* to feed the pet today.", ephemeral=True)
            return

        feeds_done = len(needs['feed_users'])
        if feeds_done >= NEEDS_FEED:
             await interaction.response.send_message("The pet is already full for today! Thanks!", ephemeral=True)
             return

        try:
            async with con.transaction():
                await con.execute(
                    "UPDATE daily_needs SET feed_users = array_append(feed_users, $1) WHERE id = $2",
                    interaction.user.id, needs['id']
                )
                await con.execute("""
                    INSERT INTO user_contributions (server_id, user_id, feeds_contributed)
                    VALUES ($1, $2, 1)
                    ON CONFLICT (server_id, user_id) 
                    DO UPDATE SET feeds_contributed = user_contributions.feeds_contributed + 1
                """, interaction.guild_id, interaction.user.id)
            
            pet_personality = PERSONALITIES.get(pet['personality'], PERSONALITIES['curious'])
            response = random.choice(pet_personality['feed_response'])
            
            await interaction.response.send_message(f"{response} Thanks, {interaction.user.mention}! 🍎\n*(Feed progress: {feeds_done + 1}/{NEEDS_FEED})*")
            await track_event("feed", interaction.guild_id, interaction.user.id)
            
        except Exception as e:
            log.error(f"Error in /feed transaction: {e}")
            await interaction.response.send_message("An error occurred while feeding.", ephemeral=True)

@tree.command(name="play", description="Play with the pet. (Counts 1x per day, per user)")
async def play(interaction: discord.Interaction):
    if not interaction.guild_id:
        return
    
    pool = await get_pool()
    async with pool.acquire() as con:
        pet = await con.fetchrow("SELECT personality FROM pets WHERE server_id = $1", interaction.guild_id)
        if not pet:
            await interaction.response.send_message("Use `/setup` first!", ephemeral=True)
            return

        today = datetime.now(timezone.utc).date()
        needs = await get_or_create_needs(con, interaction.guild_id, today)
        
        if interaction.user.id in needs['play_users']:
            await interaction.response.send_message("That was fun! But we need someone *else* to play with the pet.", ephemeral=True)
            return
            
        plays_done = len(needs['play_users'])
        if plays_done >= NEEDS_PLAY:
             await interaction.response.send_message("The pet is all played out for today! Thanks!", ephemeral=True)
             return

        try:
            async with con.transaction():
                await con.execute(
                    "UPDATE daily_needs SET play_users = array_append(play_users, $1) WHERE id = $2",
                    interaction.user.id, needs['id']
                )
                await con.execute("""
                    INSERT INTO user_contributions (server_id, user_id, plays_contributed)
                    VALUES ($1, $2, 1)
                    ON CONFLICT (server_id, user_id) 
                    DO UPDATE SET plays_contributed = user_contributions.plays_contributed + 1
                """, interaction.guild_id, interaction.user.id)
            
            pet_personality = PERSONALITIES.get(pet['personality'], PERSONALITIES['curious'])
            response = random.choice(pet_personality['play_response'])
            
            await interaction.response.send_message(f"{response} Thanks, {interaction.user.mention}! 🎾\n*(Play progress: {plays_done + 1}/{NEEDS_PLAY})*")
            await track_event("play", interaction.guild_id, interaction.user.id)

        except Exception as e:
            log.error(f"Error in /play transaction: {e}")
            await interaction.response.send_message("An error occurred while playing.", ephemeral=True)

# --- Phase 2: Adventure System ---

class AdventurePartyView(discord.ui.View):
    """
    This is the "Lobby" view. It collects party members.
    It has a 1-hour timeout, matching the adventure's expiry.
    CRITICAL BUG 2 FIX: This view no longer holds `self.message`
    """
    def __init__(self, adventure_id: int, difficulty: str):
        super().__init__(timeout=3600)
        self.adventure_id = adventure_id
        self.difficulty = difficulty
        self.min_party = ADVENTURES[difficulty]['min_party']

    async def on_timeout(self):
        # This is now primarily handled by the `check_adventure_timeouts` loop.
        # This view will simply stop listening. The loop cleans up the DB.
        self.stop()

    @discord.ui.button(label="Join Party", style=discord.ButtonStyle.green, emoji="🎒")
    async def join_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        pool = await get_pool()
        async with pool.acquire() as con:
            # FOR UPDATE locks the row to prevent race conditions on joining
            adventure = await con.fetchrow(
                "SELECT * FROM adventures WHERE id = $1 FOR UPDATE", self.adventure_id
            )
            
            if not adventure or adventure['status'] != 'forming':
                await interaction.response.send_message("This adventure is no longer available!", ephemeral=True)
                self.stop()
                return
            
            if interaction.user.id in adventure['party_members']:
                await interaction.response.send_message("You're already in the party!", ephemeral=True)
                return
            
            new_party = adventure['party_members'] + [interaction.user.id]
            await con.execute(
                "UPDATE adventures SET party_members = $1 WHERE id = $2",
                new_party, self.adventure_id
            )
            
            await track_event("adventure_join", interaction.guild_id, interaction.user.id, {"adv_id": self.adventure_id})
            
            new_size = len(new_party)
            
            if new_size >= self.min_party:
                # --- START THE ADVENTURE ---
                self.stop()
                await con.execute(
                    "UPDATE adventures SET status = 'active', votes = $1 WHERE id = $2",
                    json.dumps({}), self.adventure_id
                )
                
                party_mentions = " ".join([f"<@{uid}>" for uid in new_party])
                await interaction.response.send_message(
                    f"🎉 **Party formed!** {interaction.user.mention} joined.\n{party_mentions}, your adventure begins now!",
                )
                # Edit the original message to remove the view
                try:
                    await interaction.message.edit(view=None)
                except discord.NotFound:
                    pass
                
                await start_adventure_encounter(interaction.channel, self.adventure_id)
            
            else:
                # Update the party list
                party_mentions = "\n".join([f"<@{uid}>" for uid in new_party])
                
                original_message = interaction.message
                if original_message:
                    original_embed = original_message.embeds[0]
                    original_embed.description = (
                        f"**Difficulty:** {self.difficulty.capitalize()}\n"
                        f"**Min Party Size:** {self.min_party}\n"
                        f"**Reward:** {ADVENTURES[self.difficulty]['xp_reward']} XP\n\n"
                        f"**Current Party:**\n{party_mentions}\n\n"
                        f"Click below to join! (Expires in 1 hour)"
                    )
                    await interaction.response.edit_message(embed=original_embed)
                else:
                    await interaction.response.send_message(
                        f"✅ {interaction.user.mention} joined! ({new_size}/{self.min_party} ready)",
                    )

async def start_adventure_encounter(channel: discord.TextChannel, adventure_id: int):
    """
    Fetches the current encounter and sends the voting view.
    """
    pool = await get_pool()
    async with pool.acquire() as con:
        adv = await con.fetchrow("SELECT * FROM adventures WHERE id = $1", adventure_id)
        if not adv or adv['status'] != 'active':
            return
        
        adv_data = ADVENTURES.get(adv['difficulty'])
        if not adv_data:
            return await channel.send("Error: Unknown adventure difficulty.")
        
        encounter_index = adv['encounter_index']
        # CRITICAL BUG 4 FIX: Check for out of bounds
        if encounter_index >= len(adv_data['encounters']):
            await finish_adventure(channel, adv)
            return
            
        encounter = adv_data['encounters'][encounter_index]
        
        await con.execute(
            "UPDATE adventures SET votes = $1 WHERE id = $2",
            json.dumps({}), adventure_id
        )
        
        view = AdventureEncounterView(
            adventure_id=adventure_id,
            encounter_index=encounter_index,
            party_members=adv['party_members'],
            choices=encounter['choices']
        )
        
        party_mentions = " ".join([f"<@{uid}>" for uid in adv['party_members']])
        embed = discord.Embed(
            title=f"Encounter {encounter_index + 1}/{len(adv_data['encounters'])}",
            description=f"{encounter['text']}\n\nWhat will you do?",
            color=discord.Color.orange()
        )
        for choice in encounter['choices']:
            embed.add_field(name=f"{choice['emoji']} {choice['text']}", value=f"Vote for: `{choice['id']}`", inline=False)
        
        await channel.send(f"{party_mentions} - It's time to vote! (5 min timeout)", embed=embed, view=view)

class AdventureEncounterView(discord.ui.View):
    """
    This is the "Voting" view.
    """
    def __init__(self, adventure_id: int, encounter_index: int, party_members: List[int], choices: List[Dict]):
        super().__init__(timeout=300) # 5-minute timeout for voting
        self.adventure_id = adventure_id
        self.encounter_index = encounter_index
        self.party_members = party_members
        
        for choice in choices:
            button = discord.ui.Button(
                label=choice['text'],
                emoji=choice['emoji'],
                style=discord.ButtonStyle.secondary,
                custom_id=choice['id']
            )
            button.callback = self.handle_vote
            self.add_item(button)

    async def handle_vote(self, interaction: discord.Interaction):
        """Callback for any of the vote buttons."""
        
        if interaction.user.id not in self.party_members:
            await interaction.response.send_message("You are not in this adventure party!", ephemeral=True)
            return
        
        choice_id = interaction.data['custom_id']
        pool = await get_pool()
        
        async with pool.acquire() as con:
            async with con.transaction():
                adv = await con.fetchrow("SELECT * FROM adventures WHERE id = $1 FOR UPDATE", self.adventure_id)
                if not adv or adv['status'] != 'active':
                    await interaction.response.send_message("This adventure is no longer active.", ephemeral=True)
                    return
                
                votes = json.loads(adv['votes'])
                
                if str(interaction.user.id) in votes:
                    await interaction.response.send_message("You have already voted for this encounter.", ephemeral=True)
                    return
                
                votes[str(interaction.user.id)] = choice_id
                await con.execute(
                    "UPDATE adventures SET votes = $1 WHERE id = $2",
                    json.dumps(votes), self.adventure_id
                )
                
                await interaction.response.send_message(f"You voted for: **{choice_id}**", ephemeral=True)
                
                if len(votes) >= len(self.party_members):
                    self.stop()
                    await interaction.message.edit(view=None) # Remove buttons
                    await process_adventure_vote(interaction.channel, adv, votes)

    # CRITICAL BUG 1: Vote Timeout Resolution
    async def on_timeout(self):
        """Auto-vote for AFK players and resolve encounter."""
        self.stop()
        log.info(f"Adventure {self.adventure_id} encounter timed out.")
        
        pool = await get_pool()
        async with pool.acquire() as con:
            adv = await con.fetchrow("SELECT * FROM adventures WHERE id = $1", self.adventure_id)
            if not adv or adv['status'] != 'active':
                return
            
            votes = json.loads(adv['votes'])
            
            # Auto-vote for missing players (pick first choice)
            encounter = ADVENTURES[adv['difficulty']]['encounters'][adv['encounter_index']]
            default_choice = encounter['choices'][0]['id']
            
            missing_votes = False
            for user_id in self.party_members:
                if str(user_id) not in votes:
                    votes[str(user_id)] = default_choice
                    missing_votes = True
            
            await con.execute(
                "UPDATE adventures SET votes = $1 WHERE id = $2",
                json.dumps(votes), self.adventure_id
            )
            
            # Find the channel
            pet = await con.fetchrow("SELECT pet_channel_id FROM pets WHERE server_id = $1", adv['server_id'])
            if pet:
                channel = client.get_channel(pet['pet_channel_id'])
                if channel:
                    if missing_votes:
                        await channel.send("⏰ Vote timed out! Making automatic choices for AFK players...")
                    await process_adventure_vote(channel, adv, votes)

async def process_adventure_vote(channel: discord.TextChannel, adv: asyncpg.Record, votes: Dict[str, str]):
    """
    Tally votes, determine outcome, log it, and move to next step.
    """
    # 1. Tally votes
    vote_counts = Counter(votes.values())
    winning_choice_id = vote_counts.most_common(1)[0][0]
    
    # 2. Get choice data
    adv_data = ADVENTURES[adv['difficulty']]
    encounter = adv_data['encounters'][adv['encounter_index']]
    choice = next((c for c in encounter['choices'] if c['id'] == winning_choice_id), None)
    
    if not choice:
        return await channel.send("Error processing vote. Choice not found.")
        
    # 3. Determine outcome
    roll = random.random()
    success = roll <= choice['success_rate']
    
    result_text = ""
    item_reward = None
    xp_reward = 0
    
    pool = await get_pool()
    async with pool.acquire() as con:
        if success:
            result_text = choice['result']
            reward_type = choice.get('reward')
            
            if reward_type == "xp_bonus":
                xp_reward = 5 # Small XP bonus
                pet = await con.fetchrow("SELECT * FROM pets WHERE server_id = $1", adv['server_id'])
                if pet:
                    await grant_xp_and_evolve(con, pet['server_id'], pet, xp_reward)
            
            elif reward_type != "nothing" and reward_type is not None:
                # POLISH 2: Add item to inventory
                item = await add_item_to_inventory(con, adv['server_id'], reward_type, 1)
                if item:
                    result_text += f"\n\n**+1 {item['emoji']} {item['name']}** added to inventory!"

        else:
            result_text = choice.get('failure', "It didn't work...")

    # 4. Log the result
    log_entry = {
        "encounter": encounter['text'],
        "choice": choice['text'],
        "success": success,
        "result": result_text
    }
    
    new_log = json.loads(adv['encounter_log']) + [log_entry]
    next_index = adv['encounter_index'] + 1
    
    await pool.execute(
        "UPDATE adventures SET encounter_log = $1, encounter_index = $2 WHERE id = $3",
        json.dumps(new_log), next_index, adv['id']
    )
    
    # 5. Show the result
    embed = discord.Embed(
        title=f"Outcome: {choice['text']}",
        description=f"**{result_text}**",
        color=discord.Color.green() if success else discord.Color.red()
    )
    await channel.send(embed=embed)
    await asyncio.sleep(2)
    
    # 6. Move to next encounter (or finish)
    await start_adventure_encounter(channel, adv['id'])

async def finish_adventure(channel: discord.TextChannel, adv: asyncpg.Record):
    """
    Wraps up a completed adventure, gives XP, and updates status.
    """
    pool = await get_pool()
    xp_reward = ADVENTURES[adv['difficulty']]['xp_reward']
    
    async with pool.acquire() as con:
        # Grant XP to pet
        pet = await con.fetchrow("SELECT * FROM pets WHERE server_id = $1", adv['server_id'])
        if pet:
            await grant_xp_and_evolve(con, pet['server_id'], pet, xp_reward)
            
        # Set adventure as completed
        await con.execute(
            "UPDATE adventures SET status = 'completed' WHERE id = $1",
            adv['id']
        )
        
    embed = discord.Embed(
        title="Adventure Complete!",
        description=f"You survived the {ADVENTURES[adv['difficulty']]['name']}!",
        color=discord.Color.gold()
    )
    embed.add_field(name="Pet XP Gained", value=f"`{xp_reward} XP`")
    
    # Show summary
    log = json.loads(adv['encounter_log'])
    summary = "\n".join([f"• {e['result']}" for e in log])
    embed.add_field(name="Log Summary", value=summary or "No encounters.", inline=False)
    
    await channel.send(embed=embed)
    await track_event("adventure_completed", adv['server_id'], metadata={"adv_id": adv['id'], "difficulty": adv['difficulty']})

@tree.command(name="adventure_start", description="Start an adventure!")
@app_commands.describe(difficulty="Choose difficulty")
@app_commands.choices(difficulty=[
    app_commands.Choice(name="🌲 Easy (Solo OK)", value="easy"),
    app_commands.Choice(name="⛰️ Medium (2+ players)", value="medium"),
    app_commands.Choice(name="🌋 Hard (3+ players)", value="hard"),
])
async def adventure_start(interaction: discord.Interaction, difficulty: str):
    if not interaction.guild_id:
        return
    
    pool = await get_pool()
    async with pool.acquire() as con:
        # CRITICAL BUG 3: Transaction for race condition
        async with con.transaction():
            pet = await con.fetchrow("SELECT * FROM pets WHERE server_id = $1", interaction.guild_id)
            if not pet:
                await interaction.response.send_message("Use `/setup` first!", ephemeral=True)
                return
            
            existing = await con.fetchrow(
                "SELECT * FROM adventures WHERE server_id = $1 AND status IN ('forming', 'active') FOR UPDATE",
                interaction.guild_id
            )
            if existing:
                await interaction.response.send_message("There's already an adventure in progress!", ephemeral=True)
                return
            
            adv_data = ADVENTURES[difficulty]
            expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
            
            # We defer response to make sure we can send the message
            await interaction.response.defer()
            
            view = AdventurePartyView(adventure_id=0, difficulty=difficulty) # Dummy ID first
            embed = discord.Embed(
                title=f"⚔️ Adventure: {adv_data['name']}",
                description=(
                    f"**Difficulty:** {difficulty.capitalize()}\n"
                    f"**Min Party Size:** {adv_data['min_party']}\n"
                    f"**Reward:** {adv_data['xp_reward']} XP\n\n"
                    f"**Current Party:**\n{interaction.user.mention}\n\n"
                    f"Click below to join! (Expires in 1 hour)"
                ),
                color=discord.Color.green()
            )
            
            # Send the message first to get its ID
            message = await interaction.followup.send(embed=embed, view=view, wait=True)
            
            adventure_id = await con.fetchval(
                """
                INSERT INTO adventures (server_id, difficulty, expires_at, party_members, message_id)
                VALUES ($1, $2, $3, ARRAY[$4]::BIGINT[], $5)
                RETURNING id
                """,
                interaction.guild_id, difficulty, expires_at, interaction.user.id, message.id
            )
            
            # Update view with real adventure ID
            view.adventure_id = adventure_id
            await track_event("adventure_started", interaction.guild_id, interaction.user.id, {"difficulty": difficulty})

# --- POLISH 3: Adventure History Command ---
@tree.command(name="adventures", description="View recent adventure logs")
async def adventures_history(interaction: discord.Interaction):
    if not interaction.guild_id:
        return
    
    pool = await get_pool()
    recent = await pool.fetch(
        "SELECT * FROM adventures WHERE server_id = $1 AND status = 'completed' ORDER BY created_at DESC LIMIT 5",
        interaction.guild_id
    )
    
    if not recent:
        await interaction.response.send_message("No completed adventures yet!", ephemeral=True)
        return
    
    embed = discord.Embed(title="🗺️ Recent Adventures", color=discord.Color.blue())
    for adv in recent:
        log = json.loads(adv['encounter_log'])
        summary = ", ".join([f"{e['choice']} ({'✅' if e['success'] else '❌'})" for e in log])
        embed.add_field(
            name=f"{ADVENTURES[adv['difficulty']]['name']} - {discord.utils.format_dt(adv['created_at'], 'R')}",
            value=summary or "No encounters recorded",
            inline=False
        )
    
    await interaction.response.send_message(embed=embed)

# --- POLISH 4: Admin Cancel Command ---
@tree.command(name="adventure_cancel", description="(Admin) Cancel a stuck adventure")
@app_commands.checks.has_permissions(administrator=True)
async def adventure_cancel(interaction: discord.Interaction):
    if not interaction.guild_id:
        return
    
    pool = await get_pool()
    result = await pool.execute(
        "UPDATE adventures SET status = 'failed' WHERE server_id = $1 AND status IN ('forming', 'active')",
        interaction.guild_id
    )
    
    if result == "UPDATE 0":
        await interaction.response.send_message("No active adventures to cancel.", ephemeral=True)
    else:
        await interaction.response.send_message("Adventure cancelled.", ephemeral=True)
        await track_event("adventure_admin_cancel", interaction.guild_id, interaction.user.id)

# --- Background Loops ---

@tasks.loop(time=time(hour=0, minute=1, tzinfo=timezone.utc)) # Resets at 00:01 UTC
async def daily_needs_reset_loop():
    """
    Resets needs, grants XP/penalties, and handles streaks.
    """
    await client.wait_until_ready()
    log.info("[Loop] Running daily needs reset...")
    pool = await get_pool()
    
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    
    async with pool.acquire() as con:
        pets_and_needs = await con.fetch(
            """
            SELECT p.*, n.feed_users, n.play_users
            FROM pets p
            LEFT JOIN daily_needs n ON p.server_id = n.server_id AND n.need_date = $1
            """,
            yesterday
        )
        
        for record in pets_and_needs:
            pet = dict(record)
            channel = client.get_channel(pet['pet_channel_id'])
            if not channel:
                log.warning(f"Cannot find channel {pet['pet_channel_id']} for server {pet['server_id']}")
                continue
            
            feeds_met = len(pet.get('feed_users') or []) >= NEEDS_FEED
            plays_met = len(pet.get('play_users') or []) >= NEEDS_PLAY
            
            try:
                if feeds_met and plays_met:
                    # --- SUCCESS ---
                    new_streak = pet.get('streak', 0) + 1
                    evolved_to = await grant_xp_and_evolve(con, pet['server_id'], pet, XP_PER_DAY)
                    await con.execute("UPDATE pets SET streak = $1 WHERE server_id = $2", new_streak, pet['server_id'])
                    
                    streak_msg = f"\n🔥 **{new_streak} DAY STREAK!** Keep it up!" if new_streak > 1 else ""
                    await channel.send(
                        f"🎉 **Great job, team!** 🎉\n"
                        f"All needs met yesterday. `+{XP_PER_DAY} XP`!{streak_msg}"
                    )
                    await track_event("needs_completed", pet['server_id'], metadata={"streak": new_streak})

                    if evolved_to:
                        new_stage_name = STAGES.get(evolved_to, evolved_to)
                        await channel.send(f"🌟 **EVOLUTION!** 🌟\nOur pet has evolved into a {new_stage_name}!")
                
                else:
                    # --- FAILURE ---
                    old_streak = pet.get('streak', 0)
                    if old_streak > 0 or pet.get('xp', 0) > 0: # Only apply penalty if not a brand new server
                        await con.execute(
                            "UPDATE pets SET xp = GREATEST(xp - $1, 0), streak = 0 WHERE server_id = $2",
                            XP_PENALTY, pet['server_id']
                        )
                        streak_msg = f"😥 **Streak broken!** (Was {old_streak} days)\n" if old_streak > 0 else "😥 **Oh no...**\n"
                        await channel.send(
                            f"{streak_msg}"
                            f"We didn't meet all needs yesterday. `-{XP_PENALTY} XP` penalty.\nLet's try our best today!"
                        )
                        await track_event("needs_failed", pet['server_id'], metadata={"streak_broken": old_streak})
                    else:
                        await channel.send("😥 **Oh no...**\nWe didn't meet all needs yesterday. Let's try our best today!")


                # Announce the new day
                await channel.send(
                    "☀️ **A new day has begun!** All needs have been reset. Check `/status` to see what we need to do!"
                )
            except discord.errors.Forbidden:
                log.warning(f"Missing permissions for server {pet['server_id']}")
            except Exception as e:
                log.error(f"Error processing server {pet['server_id']} in daily loop: {e}")

@tasks.loop(minutes=10)
async def check_adventure_timeouts():
    """
    Cleans up 'forming' adventures that have expired.
    CRITICAL BUG 2 FIX: This is now the primary cleanup method.
    """
    await client.wait_until_ready()
    pool = await get_pool()
    now = datetime.now(timezone.utc)
    
    async with pool.acquire() as con:
        expired_adventures = await con.fetch(
            "SELECT * FROM adventures WHERE status = 'forming' AND expires_at < $1", now
        )
        
        for adv in expired_adventures:
            log.info(f"Adventure {adv['id']} for server {adv['server_id']} has expired.")
            await con.execute(
                "UPDATE adventures SET status = 'failed' WHERE id = $1", adv['id']
            )
            
            pet = await con.fetchrow("SELECT pet_channel_id FROM pets WHERE server_id = $1", adv['server_id'])
            if pet:
                channel = client.get_channel(pet['pet_channel_id'])
                if channel:
                    try:
                        # Edit the original message to show it expired
                        message = await channel.fetch_message(adv['message_id'])
                        if message:
                            await message.edit(content="This adventure has expired.", view=None)
                    except (discord.errors.NotFound, discord.errors.Forbidden):
                        # Message was deleted or permissions lost, send a new one
                        try:
                            await channel.send(f"The adventure to the {ADVENTURES[adv['difficulty']]['name']} failed to form in time and has expired. 😥")
                        except:
                            pass
            await track_event("adventure_expired", adv['server_id'], metadata={"adv_id": adv['id']})

# --- Run the Bot ---
if __name__ == "__main__":
    async def main():
        async with client:
            await client.start(TOKEN)

    # Start the web server in a separate thread
    start_web_server_thread()
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot shutdown requested.")