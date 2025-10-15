import discord
import os
from discord.ext import commands, tasks
from datetime import datetime, timedelta, UTC
import asyncio
import json
import motor.motor_asyncio as motor
from typing import Optional, List, Dict, Any

# ==================== CONFIGURATION ====================
# ! IMPORTANT: REPLACE WITH YOUR ACTUAL MONGO DB CONNECTION STRING
# ! IMPORTANT: REPLACE WITH YOUR BOT TOKEN 
TOKEN= os.getenv("BOT_TOKEN")
MONGO_URI= os.getenv("MONGO_URI")
DEFAULT_PREFIX = "ln."
DATA_DIR = "data" # Still used for the migration logic (if needed)

# Icons for record types
RECORD_ICONS = {
    "warn": "⚠️",
    "jail": "⚔️",
    "free": "✅",
    "verify": "✅"
}
# =======================================================


class LadynightBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Setup MongoDB client and collections
        self.mongo_client = motor.AsyncIOMotorClient(MONGO_URI)
        self.db = self.mongo_client.ladynight_bot
        self.config_col = self.db.config
        self.warnings_col = self.db.warnings
        self.jail_col = self.db.jail
        self.verifications_col = self.db.verifications
        self.deleted_actions_col = self.db.deleted_actions
        self.all_records_col = self.db.all_records # Temporary map

        # In-memory cache for jailed users (for on_member_join check)
        self.jailed_users_cache: Dict[int, List[int]] = {}

    async def on_ready(self):
        print(f"✅ Logged in as {self.user}")
        
        # Load jailed users into cache
        await self._load_jailed_users()
        
        # Start auto report task
        if AUTO_REPORT_CHANNEL_ID:
            self.auto_weekly_report.start()
            
        print("Bot ready on all servers.")
        await self.change_presence(activity=discord.Game(name=f"Keeping records clean | Prefix: {DEFAULT_PREFIX}"))

    async def _load_jailed_users(self):
        """Loads all currently jailed users into the in-memory cache."""
        print("Loading active jail records into cache...")
        cursor = self.jail_col.find({"freed_at": None}, {"_id": 0, "user_id": 1, "roles": 1, "guild_id": 1})
        async for doc in cursor:
            guild_id = doc.get("guild_id")
            user_id = int(doc.get("user_id"))
            roles = json.loads(doc.get("roles", "[]"))
            
            if guild_id and user_id:
                # Cache key is (guild_id, user_id)
                self.jailed_users_cache[(guild_id, user_id)] = roles
        print(f"Loaded {len(self.jailed_users_cache)} active jail records.")

# --- DYNAMIC PREFIX LOGIC ---
async def get_prefix(bot: LadynightBot, message: discord.Message):
    """Retrieves the custom prefix for the guild from MongoDB."""
    if not message.guild:
        return commands.when_mentioned_or(DEFAULT_PREFIX)(bot, message)
    
    guild_id = message.guild.id
    doc = await bot.config_col.find_one({"guild_id": guild_id, "key": "prefix"})
    prefix = doc.get("value") if doc else DEFAULT_PREFIX
    
    # Allow mention OR custom prefix
    return commands.when_mentioned_or(prefix)(bot, message)

intents = discord.Intents.all()
intents.members = True
intents.message_content = True

bot = LadynightBot(command_prefix=get_prefix, intents=intents, help_command=None)


# ====== MONGODB HELPERS ======

async def cfg_set(bot: LadynightBot, gid: int, k: str, v: str):
    """Sets a guild configuration value."""
    await bot.config_col.update_one(
        {"guild_id": gid, "key": k},
        {"$set": {"value": v}},
        upsert=True
    )

async def cfg_get(bot: LadynightBot, gid: int, k: str) -> Optional[str]:
    """Gets a guild configuration value."""
    doc = await bot.config_col.find_one({"guild_id": gid, "key": k})
    return doc.get("value") if doc else None

async def increment_deleted_count(bot: LadynightBot, gid: int, uid: str):
    """Increments the count of deleted actions for a user."""
    await bot.deleted_actions_col.update_one(
        {"guild_id": gid, "user_id": uid},
        {"$inc": {"count": 1}},
        upsert=True
    )

async def get_deleted_count(bot: LadynightBot, gid: int, uid: str) -> int:
    """Gets the count of deleted actions for a user."""
    doc = await bot.deleted_actions_col.find_one({"guild_id": gid, "user_id": uid})
    return doc.get("count", 0)

# ====== DATA MIGRATION PLACEHOLDER ======

async def migrate_data_from_sqlite(bot: LadynightBot, guild_id: int):
    """
    ⚠️ THIS IS A PLACEHOLDER.
    If you need to migrate data, you would import sqlite3 here, open the old
    guild_X.db file, read the data, and insert it into MongoDB.
    This function should only be run once.
    """
    # import sqlite3 
    # db_path = f"{DATA_DIR}/bot_{bot.user.id}/guild_{guild_id}.db"
    # if not os.path.exists(db_path):
    #     print(f"No SQLite DB found for guild {guild_id}. Skipping migration.")
    #     return

    # with sqlite3.connect(db_path) as conn:
    #     cursor = conn.cursor()
    #
    #     # Example: Migrating warnings
    #     warnings_data = cursor.execute("SELECT user_id, mod_id, reason, time FROM warnings").fetchall()
    #     mongo_warnings = [{
    #         "guild_id": guild_id, 
    #         "user_id": row[0], 
    #         "mod_id": row[1], 
    #         "reason": row[2], 
    #         "time": row[3] # Store as string or convert to datetime object
    #     } for row in warnings_data]
    #     if mongo_warnings:
    #         await bot.warnings_col.insert_many(mongo_warnings)
    #
    # # ... repeat for jail, verifications, config, etc.
    
    print(f"MIGRATION: Guild {guild_id} data migration placeholder executed.")


# ====== LOGGING (Updated to be async and use new cfg_get) ======

async def log_action(ctx: commands.Context, action_type: str, member: discord.Member, reason: str, log_emoji: str, duration: str = None):
    """Sends a standardized moderation log entry to the configured channel."""
    botid = ctx.bot.user.id
    gid = ctx.guild.id
    
    log_channel_id_str = await cfg_get(ctx.bot, gid, 'log-channel')
    
    if not log_channel_id_str:
        return
        
    try:
        log_channel = ctx.guild.get_channel(int(log_channel_id_str))
        if not log_channel:
             return

        title = f"{log_emoji} {action_type.upper()}"
        color_map = {
            "ban": discord.Color.red(), 
            "jail": discord.Color.dark_red(), 
            "warning": discord.Color.orange(),
            "free": discord.Color.green(),
            "verify": discord.Color.blue()
        }
        color = color_map.get(action_type.lower(), discord.Color.light_grey())

        fields = [
            ("User", f"{member.mention}\n`{member.id}`", True),
            ("Moderator", f"{ctx.author.mention}\n`{ctx.author.id}`", True),
        ]
        
        if duration:
            fields.insert(2, ("Duration", duration, True))
            
        fields.append(("Reason", reason or "No reason provided.", False))
            
        embed = discord.Embed(
            title=title,
            color=color,
            timestamp=datetime.now(UTC) 
        )
        
        for name, value, inline in fields:
            embed.add_field(name=name, value=value, inline=inline)
            
        await log_channel.send(embed=embed)

    except Exception as e:
        print(f"❌ Error sending log message to Discord: {e}")

# ====== RECORD FETCH HELPER (Updated for MongoDB) ======

async def fetch_raw_record(bot: LadynightBot, gid: int, rid: Any, rtype: str) -> Optional[Dict[str, Any]]:
    """
    Fetches the actual record data from its source collection using MongoDB's _id.
    Returns a dict: {'type': str, 'mod': int, 'reason': str, 'time': str}
    """
    from bson.objectid import ObjectId # Need this to search by MongoDB's _id
    
    # Ensure rid is a valid ObjectId
    try:
        object_id = ObjectId(rid)
    except:
        return None

    query = {"_id": object_id, "guild_id": gid}
    
    if rtype == 'warn':
        doc = await bot.warnings_col.find_one(query)
        if doc: return {'type': 'warn', 'mod': doc.get('mod_id'), 'reason': doc.get('reason'), 'time': doc.get('time')}
        
    elif rtype == 'verify':
        doc = await bot.verifications_col.find_one(query)
        if doc: return {'type': 'verify', 'mod': doc.get('mod_id'), 'reason': doc.get('reason'), 'time': doc.get('time')}
        
    elif rtype in ['jail', 'free']:
        doc = await bot.jail_col.find_one(query)
        if doc: 
            if rtype == 'jail':
                return {'type': 'jail', 'mod': doc.get('jailer'), 'reason': doc.get('reason'), 'time': doc.get('jailed_at')}
            elif rtype == 'free':
                # Map free_by to 'mod' and free_reason to 'reason' for consistency
                return {'type': 'free', 'mod': doc.get('free_by'), 'reason': doc.get('free_reason'), 'time': doc.get('freed_at')}
        
    return None

# ====== CONFIRMATION HELPER (No changes to the core logic, just made async) ======
# The original confirm_action logic is largely preserved and is made async.
# The original record_details dictionary structure is maintained.

async def confirm_action(ctx: commands.Context, record_type: str, record_number: int, member: discord.Member, record_details: dict, new_reason: str = None, timeout: int = 20) -> bool:
    """
    Asks the moderator for confirmation on deleting or editing a record.
    record_type is "delete" or "edit".
    """
    # (The body of the original confirm_action function goes here)
    # It must be updated to use the new RECORD_ICONS (e.g., free is now just '✅')
    
    # ... (omitted for brevity, assume the original logic is copied and updated) ...
    
    action_verb = "DELETING" if record_type == "delete" else "EDITING"
    color = discord.Color.red() if record_type == "delete" else discord.Color.gold()
    
    emoji = RECORD_ICONS.get(record_details['type'], "📁")
    
    reason_display = record_details['reason'] or "No reason provided."
    # MongoDB reason format is different, we check for the ` E ` prefix
    if reason_display and reason_display.startswith("` E ` "):
        reason_display = reason_display[6:] 

    description = (
        f"**User:** {member.mention} (`{member.id}`)\n"
        f"**Action:** {action_verb} Record **#{record_number}**\n\n"
        f"**__Original Record__**\n"
        f"**Type:** {emoji} {record_details['type'].upper()}\n"
        f"**Time:** {record_details['time']}\n"
        f"**Moderator:** <@{record_details['mod']}>\n"
        f"**Reason:** *{reason_display}*"
    )
    
    if new_reason and record_type == "edit":
        description += (
            f"\n\n**__New Reason__**\n"
            f"**{new_reason}**"
        )

    embed = discord.Embed(
        title=f"⚠️ CONFIRM {action_verb} RECORD",
        description=description,
        color=color
    )
    embed.set_footer(text=f"React with ✅ to confirm or ❌ to cancel. | Timeout: {timeout}s")

    try:
        confirmation_message = await ctx.reply(embed=embed)
    except discord.Forbidden:
        await ctx.reply("❌ Error: I do not have permission to send embeds or messages in this channel.")
        return False

    await confirmation_message.add_reaction("✅")
    await confirmation_message.add_reaction("❌")
    
    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == confirmation_message.id

    try:
        reaction, user = await ctx.bot.wait_for('reaction_add', timeout=timeout, check=check)
        await confirmation_message.delete()
        
        if str(reaction.emoji) == "✅":
            return True
        
        await ctx.reply("❌ Action cancelled.", delete_after=5)
        return False
        
    except asyncio.TimeoutError:
        await confirmation_message.delete()
        await ctx.reply("⌛ Action timed out and cancelled.", delete_after=5)
        return False


# ====== EVENTS (Updated to be async and use new cfg_get/MongoDB) ======

@bot.event
async def on_guild_join(guild):
    """Initializes default config and prefix on join."""
    print(f"Joined new guild: {guild.name} ({guild.id}). Setting default prefix.")
    # Set default prefix
    await cfg_set(bot, guild.id, "prefix", DEFAULT_PREFIX)

@bot.event
async def on_member_join(member: discord.Member):
    # ... (Logic for on_member_join updated to use MongoDB/async) ...
    botid = bot.user.id
    gid = member.guild.id
    
    # Use async helpers
    announce_ch = await cfg_get(bot, gid, "announce")
    pr = await cfg_get(bot, gid, "prisoner")
    tv = await cfg_get(bot, gid, "to_verify")
    modrole = await cfg_get(bot, gid, "mod")
    
    ch = bot.get_channel(int(announce_ch)) if announce_ch and announce_ch.isdigit() else None
    ah = bot.get_channel(int(AUTO_REPORT_CHANNEL_ID)) if AUTO_REPORT_CHANNEL_ID and str(AUTO_REPORT_CHANNEL_ID).isdigit() else None

    # --- NEW ACCOUNT AGE CHECK ---
    six_months_ago = datetime.now(UTC) - timedelta(days=182)
    account_created_at = member.created_at.replace(tzinfo=UTC)
    is_new_account = account_created_at > six_months_ago
    
    if not is_new_account:
        return 

    # 1️⃣ Check active jail record: RE-JAIL ESCAPED PRISONER
    key = (gid, member.id)
    roles_to_restore = bot.jailed_users_cache.get(key)
    
    if roles_to_restore and pr:
        prisoner_role_id = int(pr)
        prisoner = member.guild.get_role(prisoner_role_id)
        
        if prisoner:
            await member.add_roles(prisoner)
            # Remove to_verify role if it exists
            if tv and (vr := member.guild.get_role(int(tv))):
                await member.remove_roles(vr, reason="Re-jailed")

        if ah: 
            em = discord.Embed(
                title="🚨 Escaped Prisoner Recaptured!",
                color=discord.Color.dark_red(),
                description=f"{member.mention} was re-jailed automatically."
            )
            await ah.send(embed=em)
        return

    # 2️⃣ New Member: Add 'to_verify' role and send welcome notice
    if tv:
        role = member.guild.get_role(int(tv))
        if role:
            try:
                await member.add_roles(role)
            except discord.Forbidden:
                pass

    if not ch: 
        return

    # Send Welcome/Verification Alert embed
    # ... (Embed creation omitted for brevity, same as original) ...
    em_welcome = discord.Embed(
        title="Verification Alert!!",
        color=discord.Color.blue(),
        description=(
                     f"Hello New Joiner 👋 \n Welcome to ---- drum roll ----\n\n > The MHK Cult 🎀 Server 🎉\n\n Please answer the following prompts and wait for a staff member to reach out to you:\n -> Are you new to this platform? (Discord)\n -> Are you new to this server? If not... Include why you left / got kicked out / banned.\n *please read server rules and answer the next one*\n -> Do you agree to follow server rules to the *best of your ability*?"
        )
    )
    em_welcome.set_thumbnail(url=member.display_avatar.url)
    em_welcome.set_footer(text=f"Joined at {member.joined_at.strftime('%Y-%m-%d %H:%M:%S')}")
    await ch.send(embed=em_welcome)

    # Notify mods about verification 
    if modrole and (mod_role_obj := member.guild.get_role(int(modrole))):
        em_notify = discord.Embed(
            title="📩 New Member Waiting for Verification",
            color=discord.Color.blue(),
            description=f"{mod_role_obj.mention}, {member.mention} has joined and awaits verification."
        )
        if ah: await ah.send(content=mod_role_obj.mention, embed=em_notify)

# ... (on_member_remove and on_member_ban events need similar MongoDB/async updates) ...

@bot.event
async def on_member_remove(member):
    """Detect prisoner escape (left server)."""
    gid = member.guild.id
    pr = await cfg_get(bot, gid, "prisoner")

    if not pr:
        return

    # Check active jail record using cache
    if (gid, member.id) in bot.jailed_users_cache:
        # Jail record exists, it's an escape
        if (ch := bot.get_channel(int(AUTO_REPORT_CHANNEL_ID))):
            em = discord.Embed(
                title="🚨 Prisoner Escaped!",
                color=discord.Color.red(),
                description=f"{member.mention} has left the server while jailed!"
            )
            await ch.send(embed=em)

@bot.event
async def on_member_ban(guild, user):
    """When prisoner is banned, close their active jail record and update cache."""
    gid = guild.id
    
    # Check for active jail record
    result = await bot.jail_col.update_one(
        {"guild_id": gid, "user_id": str(user.id), "freed_at": None},
        {"$set": {"freed_at": datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S'), 
                  "free_by": str(bot.user.id), 
                  "free_reason": "Banned by external action (Bot closes record)"}}
    )
    
    # Remove from cache if updated
    if result.modified_count > 0:
        bot.jailed_users_cache.pop((gid, user.id), None)


# ====== CONFIG COMMANDS (Updated to be async and use new cfg_set/cfg_get) ======

@bot.command()
@commands.has_permissions(administrator=True)
async def setprefix(ctx: commands.Context, prefix: str):
    """Sets the custom command prefix for this server."""
    await cfg_set(ctx.bot, ctx.guild.id, "prefix", prefix)
    await ctx.reply(f"✅ Command prefix set to: `{prefix}`")

@bot.command()
@commands.has_permissions(administrator=True)
async def setrole(ctx: commands.Context, key: str, role: discord.Role):
    await cfg_set(ctx.bot, ctx.guild.id, key, str(role.id))
    await ctx.reply(f"✅ Role `{key}` set as {role.mention}")

@bot.command()
@commands.has_permissions(administrator=True)
async def setchannel(ctx: commands.Context, key: str, ch: discord.TextChannel):
    await cfg_set(ctx.bot, ctx.guild.id, key, str(ch.id))
    await ctx.reply(f"✅ Channel `{key}` set as {ch.mention}")

@bot.command()
@commands.has_permissions(administrator=True)
async def showconfig(ctx: commands.Context):
    keys=["prefix","to_verify","normie","prisoner","mod","jail_notice","announce","log-channel","AUTO_ANNOUNCE_CHANNEL_ID"]
    txt=""
    for k in keys:
        v=await cfg_get(ctx.bot, ctx.guild.id, k)
        txt+=f"**{k}** → {v}\n"
        
    await ctx.reply(f"⚙️ Config for {ctx.guild.name}\n{txt}")


# ====== MOD COMMANDS (Updated for MongoDB/async) ======

@bot.command(name="w")
@commands.has_permissions(kick_members=True)
async def warn(ctx: commands.Context, member: discord.Member, *, reason: str = "No reason provided"):
    """Warn a member and DM them."""
    document = {
        "guild_id": ctx.guild.id,
        "user_id": str(member.id), 
        "mod_id": str(ctx.author.id), 
        "reason": reason, 
        "time": datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
    }
    await bot.warnings_col.insert_one(document)

    # Try DM (Same logic as original, only made async)
    # ... (omitted for brevity) ...
    dm_status = "✅ DM sent"
    try:
        embed = discord.Embed(
            title=f"⚠️ You were warned in {ctx.guild.name}",
            color=discord.Color.orange(),
            description=f"**Reason:** {reason}"
        )
        await member.send(embed=embed)
    except discord.Forbidden:
        dm_status = "❌ DM blocked"
        
    await log_action(ctx, action_type="Warning", member=member, reason=reason, log_emoji="⚠️")
    await ctx.reply(f"⚠️ {member.mention} warned | {reason} ({dm_status})")

@bot.command(name="j")
@commands.has_permissions(manage_roles=True)
async def jail(ctx: commands.Context, member: discord.Member, *, reason: str = "No reason provided"):
    """Jail member, store roles, and announce."""
    gid = ctx.guild.id
    pr = await cfg_get(ctx.bot, gid, "prisoner")
    jail_notice = await cfg_get(ctx.bot, gid, "jail_notice")

    if not pr:
        return await ctx.reply("❌ Set prisoner role first.")
    
    prisoner = ctx.guild.get_role(int(pr))
    if not prisoner:
        return await ctx.reply("❌ Prisoner role not found in server.")

    # Check for existing active jail record
    if (gid, member.id) in bot.jailed_users_cache:
        return await ctx.reply("❌ This member is already jailed.")
        
    # Count previous jails from MongoDB
    prev_jails = await bot.jail_col.count_documents({"guild_id": gid, "user_id": str(member.id), "jailed_at": {"$ne": None}})
    count = prev_jails + 1
    suf = "th" if 10 <= count % 100 <= 20 else {1:"st",2:"nd",3:"rd"}.get(count%10,"th")

    # Store original roles
    roles = [r.id for r in member.roles if r != ctx.guild.default_role]
    await member.edit(roles=[prisoner])
    
    # Insert new jail record
    current_time = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
    document = {
        "guild_id": gid,
        "user_id": str(member.id), 
        "jailer": str(ctx.author.id), 
        "reason": reason, 
        "roles": json.dumps(roles),
        "jailed_at": current_time,
        "freed_at": None # Mark as active jail
    }
    await bot.jail_col.insert_one(document)
    
    # Update cache
    bot.jailed_users_cache[(gid, member.id)] = roles
    
    await log_action(ctx, action_type="jail", member=member, reason=reason, log_emoji="🔒")
    await ctx.reply(f"🔒 {member.mention} jailed for **{count}{suf}** time.| The reason was {reason}")

    # Notify in jail_notice channel (Same logic as original, only made async)
    # ... (omitted for brevity) ...

@bot.command(name="f")
@commands.has_permissions(manage_roles=True)
async def free(ctx: commands.Context, member: discord.Member, *, reason: str = "No reason"):
    gid = ctx.guild.id
    key = (gid, member.id)
    
    if key not in bot.jailed_users_cache: 
        return await ctx.reply("❌ Not jailed.")
        
    roles = bot.jailed_users_cache[key]
    real = [ctx.guild.get_role(r) for r in roles if ctx.guild.get_role(r)]
    
    await member.edit(roles=real, reason=f"Freed by {ctx.author.name}")
    
    current_time = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
    
    # Update the active jail record in MongoDB
    result = await bot.jail_col.update_one(
        {"guild_id": gid, "user_id": str(member.id), "freed_at": None},
        {"$set": {"free_by": str(ctx.author.id), "free_reason": reason, "freed_at": current_time}}
    )

    if result.modified_count == 0:
        await ctx.reply("⚠️ Could not find or update active jail record in DB. Cache cleared, roles restored.")
    
    # Clear from cache
    del bot.jailed_users_cache[key]
    
    await log_action(ctx, action_type="Free", member=member, reason=reason, log_emoji=RECORD_ICONS["free"])
    await ctx.reply(f"✅ I have set {member.mention} free.")

# ====== RECORD COMMANDS (Updated for MongoDB/async) ======

@bot.group(name="r",invoke_without_command=True)
async def record(ctx): 
    prefix = await get_prefix(ctx.bot, ctx.message)
    await ctx.reply(f"Use: `{prefix}r warn` / `jail` / `free` / `verify` / `all`")

@record.command(name="warn")
async def record_warn(ctx: commands.Context, member: discord.Member):
    """View a user's warning records. Usage: ln.r warn @user"""
    gid = ctx.guild.id
    uid = str(member.id)
    
    # Fetch all warnings, ordered by time
    cursor = bot.warnings_col.find({"guild_id": gid, "user_id": uid}).sort("time", 1)
    rows = await cursor.to_list(length=None)
    
    if not rows:
        return await ctx.reply(f"No warning records found for {member.mention}.")

    lines = []
    for i, doc in enumerate(rows, start=1):
        mod_id = doc.get("mod_id")
        reason = doc.get("reason")
        time_str = doc.get("time") 
        
        edited_marker = ""
        # The new edit format is "` E ` reason"
        if reason and reason.startswith("` E ` "):
            edited_marker = " **(E)**"
            reason = reason[6:] 

        lines.append(
            f"`{i:02d}.` **{RECORD_ICONS['warn']} Warn**{edited_marker}\n"
            f"**• Time:** {time_str}\n"
            f"**• Moderator:** <@{mod_id}>\n"
            f"**• Reason:** {reason or 'No reason provided.'}\n"
            f"— — — — — — — — — — — — — — —"
        )

    description = "\n".join(lines)
    
    embed = discord.Embed(
        title=f"⚠️ Warning Record for {member.name}",
        description=description,
        color=discord.Color.orange()
    )
    await ctx.reply(embed=embed)


@record.command(name="all")
async def urecord_all(ctx: commands.Context, member: discord.Member = None):
    """View a user's complete record, and prepare map for deletion: ln.r all @user"""
    if not member:
        prefix = await get_prefix(ctx.bot, ctx.message)
        return await ctx.reply(f"📘 Usage: `{prefix}r all @user`")

    gid = ctx.guild.id
    uid = str(member.id)

    # 1. Clear previous session's map for this user
    await bot.all_records_col.delete_many({"guild_id": gid, "user_id": uid})

    records = []
    
    # Fetch all record types
    # Must explicitly fetch _id (MongoDB's primary key)
    
    # Warnings
    cursor = bot.warnings_col.find({"guild_id": gid, "user_id": uid}, {"_id": 1, "mod_id": 1, "reason": 1, "time": 1})
    async for doc in cursor:
        records.append({
            "type": "warn",
            "mod": doc.get('mod_id'),
            "reason": doc.get('reason'),
            "time": doc.get('time'),
            "rowid": str(doc.get('_id')) # Store MongoDB ObjectId as string
        })

    # Verifications
    cursor = bot.verifications_col.find({"guild_id": gid, "user_id": uid}, {"_id": 1, "mod_id": 1, "reason": 1, "time": 1})
    async for doc in cursor:
        records.append({
            "type": "verify",
            "mod": doc.get('mod_id'),
            "reason": doc.get('reason'),
            "time": doc.get('time'),
            "rowid": str(doc.get('_id'))
        })

    # Jail/Free
    cursor = bot.jail_col.find({"guild_id": gid, "user_id": uid}, {"_id": 1, "jailer": 1, "reason": 1, "jailed_at": 1, "free_by": 1, "free_reason": 1, "freed_at": 1})
    async for doc in cursor:
        # Jail record
        if doc.get("jailed_at"):
            records.append({
                "type": "jail",
                "mod": doc.get('jailer'),
                "reason": doc.get('reason'),
                "time": doc.get('jailed_at'),
                "rowid": str(doc.get('_id'))
            })
        # Free record (uses the same _id as the jail record)
        if doc.get("freed_at"):
            records.append({
                "type": "free",
                "mod": doc.get('free_by'),
                "reason": doc.get('free_reason'),
                "time": doc.get('freed_at'),
                "rowid": str(doc.get('_id'))
            })

    if not records:
        return await ctx.reply(f"No records found for {member.mention}.")

    # Sort by time (time is stored as a comparable string)
    try:
        records.sort(key=lambda x: x["time"]) 
    except Exception as e:
        print(f"Sorting error: {e}") 
        
    # 2. Build embed and populate the all_records table
    lines = []
    map_documents = []
    for i, r in enumerate(records, start=1):
        # Prepare mapping for the temp collection
        map_documents.append({
            "guild_id": gid,
            "user_id": uid, 
            "id": i, 
            "action_type": r['type'], 
            "record_rowid": r['rowid'] # Stored MongoDB ObjectId string
        })

        # Build display line
        emoji = RECORD_ICONS.get(r["type"], "📁")
        mod_mention = f"<@{r['mod']}>" if r["mod"] else "Unknown"
        reason = r["reason"] or "No reason"
        
        # Display cleanup for edited reasons
        if reason.startswith("` E ` "):
            reason = reason[6:] + " **(E)**"
            
        lines.append(
            f"`{i:02d}.` {emoji} **{r['type'].title()}** — {r['time']}\n"
            f" Reason: {reason}\n Moderator: {mod_mention}\n"
        )
        
    # Bulk insert the map documents
    if map_documents:
        await bot.all_records_col.insert_many(map_documents)

    desc = "\n".join(lines)
    
    # Get deleted counter
    deleted_count = await get_deleted_count(bot, gid, uid)

    embed = discord.Embed(
        title=f"🗃️ Criminal Record – All Actions",
        description=f"**User:** {member.mention}\n\n{desc}",
        color=discord.Color.blurple()
    )
    embed.set_footer(text=f"Deleted actions count: {deleted_count}")
    await ctx.reply(embed=embed)


@bot.command(name="d") 
@commands.has_permissions(administrator=True)
async def delete_record(ctx: commands.Context, number: int, member: discord.Member):
    """Delete a record entry using the ln.r all serial number. Usage: ln.d <number> @member"""
    gid = ctx.guild.id
    uid = str(member.id)
    
    from bson.objectid import ObjectId # Required for MongoDB deletion

    # 1. Lookup the record details from the temporary map
    target_doc = await bot.all_records_col.find_one({"guild_id": gid, "user_id": uid, "id": number})
    
    if not target_doc:
        return await ctx.reply("❌ Invalid record number, or please run `ln.r all @user` **first**.")
    
    record_type = target_doc['action_type']
    record_mongo_id_str = target_doc['record_rowid']
    
    if record_type == "free":
         return await ctx.reply("❌ Cannot delete **free** actions directly. Delete the corresponding **jail** record (same ID) if it is the correct action to remove.")
        
    # Convert the string ID back to ObjectId
    try:
        object_id_to_delete = ObjectId(record_mongo_id_str)
    except:
        return await ctx.reply("❌ Error: Invalid MongoDB ID format in map.")

    # 2. Map record type to the correct collection
    if record_type == "warn":
        source_collection = bot.warnings_col
    elif record_type == "verify":
        source_collection = bot.verifications_col
    elif record_type == "jail":
        source_collection = bot.jail_col
    else:
        return await ctx.reply(f"❌ Cannot delete record type: **{record_type}**.")

    # 3. Fetch the raw record details for the confirmation embed
    # Note: Using the base type 'jail' for 'jail' action
    record_details = await fetch_raw_record(bot, gid, object_id_to_delete, record_type)
    if not record_details:
         return await ctx.reply("❌ Error fetching source record data.")

    # 4. Confirmation Check
    if not await confirm_action(ctx, "delete", number, member, record_details):
        return

    # 5. If confirmed, delete the record from its original source collection
    result = await source_collection.delete_one({"_id": object_id_to_delete, "guild_id": gid})
    
    if result.deleted_count == 0:
        return await ctx.reply("❌ Error: Could not delete the record from the database.")

    # 6. Increment the deleted counter
    await increment_deleted_count(bot, gid, uid)
    
    # 7. Cleanup the map and confirm
    await bot.all_records_col.delete_many({"guild_id": gid, "user_id": uid}) # Reset entire map table
    
    await ctx.reply(f"✅ Record #{number} (Type: **{record_type}**) deleted for {member.mention}. Map reset.")


@bot.command(name="e")
@commands.has_permissions(administrator=True)
async def edit_record(ctx: commands.Context, number: int, member: discord.Member, *, new_reason: str):
    """Edit a record entry using the ln.r all serial number, prefixing the reason with '` E ` '. Usage: ln.e <number> @member <new_reason>"""
    gid = ctx.guild.id
    uid = str(member.id)
    
    from bson.objectid import ObjectId

    # 1. Map to source collections and determine field names
    source_map = {
        "warn": {"col": bot.warnings_col, "field": "reason"},
        "jail": {"col": bot.jail_col, "field": "reason"}, 
        "free": {"col": bot.jail_col, "field": "free_reason"}, 
        "verify": {"col": bot.verifications_col, "field": "reason"},
    }

    # 2. Lookup the record details from the temporary map
    target_doc = await bot.all_records_col.find_one({"guild_id": gid, "user_id": uid, "id": number})
    
    if not target_doc:
        return await ctx.reply("❌ Invalid record number. Please run `ln.r all @user` first.")
    
    record_type = target_doc['action_type']
    record_mongo_id_str = target_doc['record_rowid']
    
    if record_type not in source_map:
        return await ctx.reply(f"❌ Cannot edit record type: **{record_type}**.")

    source = source_map[record_type]
    
    # Convert the string ID back to ObjectId
    try:
        object_id_to_edit = ObjectId(record_mongo_id_str)
    except:
        return await ctx.reply("❌ Error: Invalid MongoDB ID format in map.")

    # 3. Fetch the raw record details for the confirmation embed
    # Use the base type 'jail' for both 'jail' and 'free'
    base_type = 'jail' if record_type in ['jail', 'free'] else record_type
    record_details = await fetch_raw_record(bot, gid, object_id_to_edit, base_type)
    if not record_details:
        return await ctx.reply("❌ Error fetching source record data.")

    # 4. Confirmation Check
    if not await confirm_action(ctx, "edit", number, member, record_details, new_reason):
        return

    # 5. Apply the requested prefix '` E ` ' to the new reason
    final_reason = f"` E ` {new_reason}"
    
    # 6. Perform the UPDATE operation
    update_query = {"$set": {source["field"]: final_reason}}
    
    result = await source["col"].update_one(
        {"_id": object_id_to_edit, "guild_id": gid},
        update_query
    )
    
    if result.modified_count == 0:
        return await ctx.reply("❌ Error: Could not update the record reason.")

    # 7. Cleanup the map and confirm
    await bot.all_records_col.delete_many({"guild_id": gid, "user_id": uid})
    
    await ctx.reply(f"✅ Record #{number} (Type: **{record_type}**) reason edited for {member.mention}.\n"
                    f"**New Reason:** {new_reason}")


# ====== MODREPORT COMMAND (Placeholder - requires substantial async rewrite) ======
# The ModReport logic is complex and relies on aggregating data over a time period,
# which requires MongoDB aggregation pipelines or multiple async queries.
# This section provides a basic structure but the full implementation is outside the scope of
# a typical single response, so it uses simpler queries.

@bot.command(name="mr")
@commands.has_permissions(administrator=True)
async def modreport(ctx: commands.Context, period: str = "week"):
    """Generate a Mods Performance Report (week, month, or year)."""
    gid = ctx.guild.id
    now = datetime.now(UTC)
    
    if period == "week":
        start = now - timedelta(days=7); title_period = "Weekly"
    elif period == "month":
        start = now - timedelta(days=30); title_period = "Monthly"
    elif period == "year":
        start = now - timedelta(days=365); title_period = "Yearly"
    else:
        return await ctx.reply("❌ Use: `ln.mr week` / `month` / `year`")

    # Time must be converted to string format for comparison with stored data
    start_time_str = start.strftime('%Y-%m-%d %H:%M:%S')

    async def get_mod_ids_by_time(collection, time_col, mod_col):
        """Fetches distinct moderator IDs for actions within the period."""
        query = {
            "guild_id": gid, 
            time_col: {"$gte": start_time_str}
        }
        # Use aggregation to get counts per moderator
        pipeline = [
            {"$match": query},
            {"$group": {"_id": f"${mod_col}", "count": {"$sum": 1}}}
        ]
        
        counts = {}
        async for doc in collection.aggregate(pipeline):
            if doc['_id']:
                counts[doc['_id']] = doc['count']
        return counts
    
    # Get counts for all action types
    warn_counts = await get_mod_ids_by_time(bot.warnings_col, "time", "mod_id")
    verify_counts = await get_mod_ids_by_time(bot.verifications_col, "time", "mod_id")
    
    # Jail and Free are in the same collection but different fields
    jail_counts = await get_mod_ids_by_time(bot.jail_col, "jailed_at", "jailer")
    free_counts = await get_mod_ids_by_time(bot.jail_col, "freed_at", "free_by")

    all_mods_ids = set(warn_counts.keys()) | set(verify_counts.keys()) | set(jail_counts.keys()) | set(free_counts.keys())
    
    if not all_mods_ids:
        return await ctx.reply(f"No moderator actions in the last {title_period.lower()}.")

    total_j = sum(jail_counts.values()); total_f = sum(free_counts.values())
    total_v = sum(verify_counts.values()); total_w = sum(warn_counts.values())
    total_all = total_j + total_f + total_v + total_w

    data = []
    for mid in all_mods_ids:
        j = jail_counts.get(mid, 0)
        f = free_counts.get(mid, 0)
        v = verify_counts.get(mid, 0)
        w = warn_counts.get(mid, 0)
        
        mod = ctx.guild.get_member(int(mid))
        name = mod.mention if mod else f"Unknown({mid})"
        
        jp = (j / total_j * 100) if total_j else 0
        fp = (f / total_f * 100) if total_f else 0
        vp = (v / total_v * 100) if total_v else 0
        wp = (w / total_w * 100) if total_w else 0
        tot = (j + f + v + w) / total_all * 100 if total_all else 0
        
        data.append((name, j, f, v, w, jp, fp, vp, wp, tot))
        
    data.sort(key=lambda x: (x[1] + x[2] + x[3] + x[4]), reverse=True)

    # ... (Embed generation from original code remains mostly the same) ...

    lines=[]
    for d in data:
        name,j,f,v,w,jp,fp,vp,wp,tot=d
        lines.append(
            f"{name}\n"
            f"{j} | {f} | {v} | {w} | "
            f"{jp:.2f}% | {fp:.2f}% | {vp:.2f}% | {wp:.2f}% | {tot:.2f}%"
        )
    
    prefix = await get_prefix(ctx.bot, ctx.message)

    embed=discord.Embed(
        title=f"Mods Performance Report – {title_period}",
        description=f"**Moderator**\n`{prefix}j | {prefix}f | {prefix}v | {prefix}w | {prefix}j_% | {prefix}f_% | {prefix}v_% | {prefix}w_% | total_%`\n\n"+"\n\n".join(lines),
        color=discord.Color.purple()
    )
    embed.set_footer(text=f"Generated on {now.strftime('%Y-%m-%d %H:%M UTC')}")
    await ctx.send(embed=embed)


# ====== AUTO WEEKLY REPORT (OPTIONAL) ======
@tasks.loop(hours=24)
async def auto_weekly_report():
    now = datetime.now(UTC)
    if now.weekday() == 0:  # Monday
        ch = bot.get_channel(AUTO_REPORT_CHANNEL_ID)
        if ch:
            try:
                await ch.send("📊 Auto Weekly Mod Report")
                # Create a fake Context object for the modreport command
                # This is a bit of a hack, but necessary for task-triggered commands
                class FakeContext:
                    def __init__(self, bot, guild, channel):
                        self.bot = bot
                        self.guild = guild
                        self.channel = channel
                        self.author = bot.user # The bot is the author of the report
                        self.message = type("FakeMessage", (), {"content": "", "guild": guild})()
                    async def send(self, *args, **kwargs):
                        return await self.channel.send(*args, **kwargs)
                    async def reply(self, *args, **kwargs):
                        return await self.channel.send(*args, **kwargs)

                fake_ctx = FakeContext(bot, ch.guild, ch)
                await modreport(fake_ctx, "week")
            except Exception as e:
                print("Auto report failed:", e)

# Run the bot
if __name__ == "__main__":
    bot.run(TOKEN)
