from dotenv import load_dotenv
load_dotenv()
import discord
from discord import app_commands
from discord.ext import tasks
import os
import asyncpg
import asyncio
import logging
import traceback
import re
import time as _time
import datetime
import json

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nexusbot")

BOT_OWNER_ID = int(os.environ.get("BOT_OWNER_ID", "0"))
logger.info(f"BOT_OWNER_ID loaded: {BOT_OWNER_ID}")

salon_join_tracker = {}

DB_URL = os.environ.get("DATABASE_URL")
if DB_URL and DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

pool = None

async def init_db():
    global pool
    if not DB_URL:
        logger.error("DATABASE_URL is not set.")
        return
    try:
        try:
            pool = await asyncpg.create_pool(DB_URL, ssl='require')
            logger.info("Connected to database (SSL).")
        except Exception:
            pool = await asyncpg.create_pool(DB_URL)
            logger.info("Connected to database (no SSL).")
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS bot_logs (
                    id SERIAL PRIMARY KEY,
                    level VARCHAR(10) NOT NULL DEFAULT 'info',
                    message TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS outgoing_messages (
                    id SERIAL PRIMARY KEY,
                    channel_id VARCHAR(64) NOT NULL,
                    content TEXT NOT NULL,
                    status VARCHAR(16) NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT NOW(),
                    processed_at TIMESTAMP
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ownerlist (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    added_by TEXT,
                    added_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(guild_id, user_id)
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS whitelist (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    added_by TEXT,
                    added_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(guild_id, user_id)
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS blacklist (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL UNIQUE,
                    reason TEXT,
                    added_by TEXT,
                    added_at TIMESTAMP DEFAULT NOW()
                );
            """)
            try:
                await conn.execute("ALTER TABLE blacklist DROP COLUMN IF EXISTS guild_id CASCADE")
            except Exception:
                pass
            try:
                await conn.execute("ALTER TABLE blacklist ADD CONSTRAINT blacklist_user_id_unique UNIQUE (user_id)")
            except Exception:
                pass
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS protection_settings (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL,
                    module TEXT NOT NULL,
                    enabled BOOLEAN DEFAULT FALSE,
                    UNIQUE(guild_id, module)
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS gif_spam_targets (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    added_by TEXT,
                    added_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(guild_id, user_id)
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS guild_protections (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL,
                    protection_key TEXT NOT NULL,
                    enabled BOOLEAN DEFAULT FALSE,
                    log_channel_id TEXT,
                    punishment TEXT DEFAULT 'ban',
                    timeout_duration TEXT DEFAULT '1h',
                    whitelist_bypass BOOLEAN DEFAULT FALSE,
                    UNIQUE(guild_id, protection_key)
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS salon_access (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    added_by TEXT,
                    added_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(guild_id, channel_id, user_id)
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mention_spam_targets (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    added_by TEXT,
                    added_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(guild_id, user_id)
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS guild_blacklist (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL UNIQUE,
                    guild_name TEXT,
                    rejected_at TIMESTAMP DEFAULT NOW()
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS pending_guilds (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL UNIQUE,
                    guild_name TEXT,
                    inviter_id TEXT,
                    added_at TIMESTAMP DEFAULT NOW()
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS license_keys (
                    id SERIAL PRIMARY KEY,
                    key TEXT NOT NULL UNIQUE,
                    used_by_guild TEXT,
                    used_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS guild_licenses (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL UNIQUE,
                    license_key TEXT NOT NULL,
                    activated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS guild_join_leave (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL UNIQUE,
                    join_channel_id TEXT,
                    leave_channel_id TEXT
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS suggestions_log (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    nom TEXT NOT NULL,
                    faction TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    user_name TEXT,
                    suggestion TEXT,
                    objectif TEXT,
                    status TEXT DEFAULT 'en_attente',
                    submitted_at TIMESTAMP DEFAULT NOW()
                );
            """)
            await conn.execute("""
                ALTER TABLE suggestions_log ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'en_attente';
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS recensement (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL,
                    message_id TEXT,
                    channel_id TEXT,
                    user_id TEXT NOT NULL,
                    user_name TEXT,
                    date_event TEXT,
                    lieu TEXT,
                    victime TEXT,
                    agresseur TEXT,
                    action_resume TEXT,
                    echanger_contre TEXT,
                    capture_numero TEXT,
                    submitted_at TIMESTAMP DEFAULT NOW()
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS recensement_config (
                    guild_id TEXT PRIMARY KEY,
                    channel_id TEXT,
                    log_channel_id TEXT
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS recensement_pending (
                    id SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    channel_id TEXT,
                    user_id TEXT NOT NULL,
                    user_name TEXT,
                    date_event TEXT,
                    lieu TEXT,
                    victime TEXT,
                    agresseur TEXT,
                    action_resume TEXT,
                    echanger_contre TEXT,
                    capture_numero TEXT,
                    submitted_at TIMESTAMP DEFAULT NOW()
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS contrat_anon_relay (
                    channel_id TEXT PRIMARY KEY,
                    guild_id TEXT NOT NULL,
                    commanditaire_id TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            existing_keys = await conn.fetchval("SELECT COUNT(*) FROM license_keys")
            if existing_keys == 0:
                import secrets as sec
                for _ in range(10):
                    key = f"SHIELD-{sec.token_hex(4).upper()}-{sec.token_hex(4).upper()}"
                    await conn.execute("INSERT INTO license_keys (key) VALUES ($1) ON CONFLICT DO NOTHING", key)
                logger.info("Generated 10 license keys.")
            logger.info("All database tables verified/created.")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")

async def log_to_db(level, message):
    if pool:
        try:
            await pool.execute(
                "INSERT INTO bot_logs (level, message) VALUES ($1, $2)",
                level, str(message)
            )
        except Exception as e:
            logger.error(f"Failed to log to DB: {e}")


async def is_guild_licensed(guild_id):
    if not pool:
        return True
    row = await pool.fetchrow("SELECT id FROM guild_licenses WHERE guild_id = $1", str(guild_id))
    return row is not None

async def check_license(interaction):
    if interaction.user.id == BOT_OWNER_ID:
        return True
    if not interaction.guild:
        return True
    licensed = await is_guild_licensed(interaction.guild.id)
    if not licensed:
        embed = discord.Embed(
            description="⚠️ Ce serveur n'a pas de licence active.\nContactez le propriétaire du bot pour activer le bot.",
            color=0x2b2d31
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return False
    return True


SLASH_COMMANDS = [
    {"name": "/help", "params": "", "description": "Afficher la liste des commandes du bot."},
    {"name": "/panel", "params": "", "description": "Gérer les modules du serveur."},
    {"name": "/logs", "params": "", "description": "Créer automatiquement les salons de logs pour tous les modules."},
    {"name": "/supplogs", "params": "", "description": "Supprimer les salons de logs et réinitialiser la config."},
    {"name": "/blacklist", "params": "member | permission", "description": "Gérer la liste noire du serveur."},
    {"name": "/unblacklist", "params": "", "description": "Retirer un utilisateur de la blacklist."},
    {"name": "/whitelist", "params": "", "description": "Gérer la liste blanche du serveur."},
    {"name": "/ownerlist", "params": "", "description": "Gérer la liste des créateurs du serveur."},
    {"name": "/suggestions", "params": "", "description": "Affiche le panneau de suggestions du serveur."},
    {"name": "/logssuggestions", "params": "", "description": "Affiche les logs des suggestions dans un channel admin."},
    {"name": "/recpanel", "params": "", "description": "Créer le panneau de recensement de captures dans ce salon."},
    {"name": "/admincap voir", "params": "[membre]", "description": "Voir toutes les captures d'un membre (ownerlist/whitelist)."},
    {"name": "/admincap supprimer", "params": "[id]", "description": "Supprimer une capture par son ID (ownerlist/whitelist)."},
    {"name": "/admincap ajouter", "params": "[membre]", "description": "Ajouter une capture manuellement (ownerlist/whitelist)."},
]

TEXT_COMMANDS = []


async def get_command_ids(guild):
    command_ids = {}
    try:
        commands = await bot.tree.fetch_commands(guild=guild)
        for cmd in commands:
            command_ids[cmd.name] = cmd.id
        logger.info(f"Fetched {len(command_ids)} command IDs for guild {guild.name}: {command_ids}")
    except Exception as e:
        logger.warning(f"Failed to fetch guild commands: {e}")
        try:
            commands = await bot.tree.fetch_commands()
            for cmd in commands:
                command_ids[cmd.name] = cmd.id
            logger.info(f"Fetched {len(command_ids)} global command IDs: {command_ids}")
        except Exception as e2:
            logger.error(f"Failed to fetch global commands: {e2}")
    return command_ids


def build_help_embed(command_ids=None):
    if command_ids is None:
        command_ids = {}
    slash_lines = []
    for cmd in SLASH_COMMANDS:
        cmd_name = cmd['name'].lstrip('/')
        if cmd_name in command_ids:
            mention = f"</{cmd_name}:{command_ids[cmd_name]}>"
        else:
            mention = f"`{cmd['name']}`"
        if cmd["params"]:
            slash_lines.append(f"{mention} ({cmd['params']}) - {cmd['description']}")
        else:
            slash_lines.append(f"{mention} - {cmd['description']}")

    text_lines = []
    for cmd in TEXT_COMMANDS:
        if cmd["params"]:
            text_lines.append(f"`{cmd['name']}` {cmd['params']} - {cmd['description']}")
        else:
            text_lines.append(f"`{cmd['name']}` - {cmd['description']}")

    description = "# MssClick - Club\n"
    description += "MssClick-Club est un bot entièrement dédié à la protection du discord MssClick - Club. Il est là pour garantir la protection du serveur Discord avec les meilleures protections.\n\n"
    description += "## Commandes Slash\n"
    description += "\n".join(slash_lines)
    if text_lines:
        description += "\n\n## Commandes Textuelles\n"
        description += "\n".join(text_lines)

    embed = discord.Embed(
        description=description,
        color=0x2b2d31
    )

    banner_url = os.environ.get("HELP_BANNER_URL", "")
    if banner_url:
        embed.set_image(url=banner_url)

    embed.set_footer(text="© Ramzan")

    return embed


intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.moderation = True


class NexusCommandTree(app_commands.CommandTree):
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.type != discord.InteractionType.application_command:
            return True
        if interaction.guild is None:
            return True
        try:
            cmd = interaction.command
            is_admincap = (
                cmd is not None
                and hasattr(cmd, "parent")
                and cmd.parent is not None
                and getattr(cmd.parent, "name", None) == "admincap"
            )
            if is_admincap:
                check_coro = is_whitelisted(interaction.guild, interaction.user.id)
                error_msg = "❌ Seuls les membres de la ownerlist et whitelist peuvent utiliser cette commande."
            else:
                check_coro = is_owner_or_ownerlist(interaction.guild, interaction.user.id)
                error_msg = "❌ Seuls les membres de la ownerlist peuvent utiliser les commandes du bot."
            allowed = await asyncio.wait_for(check_coro, timeout=2.5)
        except asyncio.TimeoutError:
            logger.error("interaction_check: DB timeout")
            try:
                await interaction.response.send_message("❌ Délai dépassé lors de la vérification des droits.", ephemeral=True)
            except Exception:
                pass
            return False
        except Exception as e:
            logger.error(f"interaction_check error: {e}\n{traceback.format_exc()}")
            try:
                await interaction.response.send_message("❌ Erreur interne lors de la vérification des droits.", ephemeral=True)
            except Exception:
                pass
            return False
        if not allowed:
            try:
                await interaction.response.send_message(error_msg, ephemeral=True)
            except Exception:
                pass
            return False
        return True


class NexusBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = NexusCommandTree(self)
        self.synced = False

    async def on_ready(self):
        logger.info(f"Logged in as {self.user} (ID: {self.user.id})")
        await log_to_db('info', f'Bot logged in as {self.user}')

        streaming_activity = discord.Streaming(name="MssClick - Faction", url="https://twitch.tv/mssclick")
        await self.change_presence(activity=streaming_activity)

        if not self.synced:
            for guild in self.guilds:
                try:
                    self.tree.copy_global_to(guild=guild)
                    synced = await self.tree.sync(guild=guild)
                    logger.info(f"Synced {len(synced)} slash commands to {guild.name}")
                    await log_to_db('info', f'Synced {len(synced)} commands to {guild.name}')
                except Exception as e:
                    logger.error(f"Failed to sync to {guild.name}: {e}")
                    await log_to_db('error', f'Failed to sync to {guild.name}: {e}')
            self.synced = True

        if not process_outgoing_messages.is_running():
            process_outgoing_messages.start()

        self.add_view(SuggestionButtonView())
        await register_suggestion_views()
        self.add_view(RecensementButtonView())
        self.add_view(CaptureValidationView())
        try:
            self.add_dynamic_items(TicketCreateButton)
        except Exception as e:
            logger.error(f"Failed to register TicketCreateButton dynamic item: {e}")
        try:
            self.add_view(CombatPanelView())
            self.add_dynamic_items(CombatAcceptButton, CombatModifyButton, CombatReacceptButton, CombatRefuseButton, CombatRefuseModifyButton)
        except Exception as e:
            logger.error(f"Failed to register combat views: {e}")
        try:
            self.add_view(ContratPanelView())
            self.add_view(ContratCloseView())
            self.add_dynamic_items(ContratAcceptButton, ContratDoneButton, ContratCancelButton)
        except Exception as e:
            logger.error(f"Failed to register contrat views: {e}")
        try:
            self.add_view(WantedPanelView())
        except Exception as e:
            logger.error(f"Failed to register wanted views: {e}")

    async def on_guild_join(self, guild):
        try:
            if pool:
                bl = await pool.fetchrow("SELECT id FROM guild_blacklist WHERE guild_id = $1", str(guild.id))
                if bl:
                    await guild.leave()
                    await log_to_db('info', f'Auto-left blacklisted guild {guild.name} ({guild.id})')
                    return

            inviter_id = None
            try:
                async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.bot_add):
                    if entry.target.id == self.user.id:
                        inviter_id = entry.user.id
                        break
            except Exception:
                pass

            if pool:
                await pool.execute(
                    "INSERT INTO pending_guilds (guild_id, guild_name, inviter_id) VALUES ($1, $2, $3) ON CONFLICT (guild_id) DO UPDATE SET guild_name = $2, inviter_id = $3",
                    str(guild.id), guild.name, str(inviter_id) if inviter_id else None
                )

            owner = await self.fetch_user(BOT_OWNER_ID)
            if owner:
                invite_url = None
                for ch in guild.text_channels:
                    try:
                        invite = await ch.create_invite(max_age=0, max_uses=0)
                        invite_url = str(invite)
                        break
                    except Exception:
                        continue

                inviter_text = f"<@{inviter_id}>" if inviter_id else "Inconnu"
                embed = discord.Embed(
                    title="Nouveau serveur",
                    description=f"Le bot a été ajouté à un nouveau serveur.\n\n"
                                f"**Serveur :** {guild.name}\n"
                                f"**ID :** `{guild.id}`\n"
                                f"**Membres :** {guild.member_count}\n"
                                f"**Ajouté par :** {inviter_text}\n"
                                f"**Lien :** {invite_url or 'Impossible de créer un lien'}",
                    color=0x2b2d31
                )
                view = GuildApprovalView(guild.id, guild.name)
                await owner.send(embed=embed, view=view)
                await log_to_db('info', f'Approval request sent for guild {guild.name} ({guild.id})')

            try:
                self.tree.copy_global_to(guild=guild)
                await self.tree.sync(guild=guild)
            except Exception:
                pass

        except Exception as e:
            logger.error(f"Error in on_guild_join: {traceback.format_exc()}")

    async def on_guild_role_create(self, role):
        guild = role.guild
        try:
            await asyncio.sleep(0.5)
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.role_create):
                if entry.target.id == role.id:
                    await send_audit_log(guild, "role", "Rôle créé",
                        f"**Rôle:** {role.mention} (`{role.name}`)\n**ID:** `{role.id}`\n**Par:** {entry.user.mention} (`{entry.user}`)")
                    break
        except Exception:
            pass

        enabled = await is_protection_enabled(guild.id, "anti_role_create")
        if not enabled:
            return

        try:
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.role_create):
                if entry.target.id != role.id:
                    break
                user = entry.user
                if user.id == self.user.id:
                    return
                if user.id == guild.owner_id:
                    return

                is_allowed = await should_bypass_protection(guild, user.id, "anti_role_create")
                if is_allowed:
                    return

                try:
                    await role.delete(reason="Shield Protection: création de rôle non autorisée")
                except Exception as e:
                    logger.error(f"Failed to delete role {role.name}: {e}")
                    await log_to_db('error', f'Failed to delete role {role.name}: {e}')

                await apply_punishment(guild, user, "anti_role_create")
                await send_protection_log(guild, "anti_role_create", user, f"{user} a créé un rôle.", role=role)
                await log_to_db('warn', f'Role creation blocked: {user} created role {role.name} in {guild.name}')
                break
        except Exception as e:
            logger.error(f"Error in role create protection: {e}")

    async def on_guild_role_delete(self, role):
        guild = role.guild
        try:
            await asyncio.sleep(0.3)
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.role_delete):
                if entry.target.id == role.id:
                    await send_audit_log(guild, "role", "Rôle supprimé",
                        f"**Rôle:** `{role.name}`\n**ID:** `{role.id}`\n**Par:** {entry.user.mention} (`{entry.user}`)", color=0xe74c3c)
                    break
        except Exception:
            pass

        enabled = await is_protection_enabled(guild.id, "anti_role_delete")
        if not enabled:
            return

        try:
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.role_delete):
                if entry.target.id != role.id:
                    break
                user = entry.user
                if user.id == self.user.id:
                    return
                if user.id == guild.owner_id:
                    return

                is_allowed = await should_bypass_protection(guild, user.id, "anti_role_delete")
                if is_allowed:
                    return

                await apply_punishment(guild, user, "anti_role_delete")
                await send_protection_log(guild, "anti_role_delete", user, f"{user} a supprimé un rôle.", role=role)
                await log_to_db('warn', f'Role deletion blocked: {user} deleted role {role.name} in {guild.name}')
                break
        except Exception as e:
            logger.error(f"Error in role delete protection: {e}")

    async def on_guild_channel_create(self, channel):
        guild = channel.guild
        if not guild:
            return

        try:
            await asyncio.sleep(0.5)
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_create):
                if entry.target.id == channel.id:
                    await send_audit_log(guild, "channel", "Salon créé",
                        f"**Salon:** {channel.mention} (`{channel.name}`)\n**ID:** `{channel.id}`\n**Par:** {entry.user.mention} (`{entry.user}`)")
                    break
        except Exception:
            pass

        enabled = await is_protection_enabled(guild.id, "anti_channel_create")
        if not enabled:
            return

        try:
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_create):
                if entry.target.id != channel.id:
                    break
                user = entry.user
                if user.id == self.user.id:
                    return
                if user.id == guild.owner_id:
                    return

                is_allowed = await should_bypass_protection(guild, user.id, "anti_channel_create")
                if is_allowed:
                    return

                try:
                    await channel.delete(reason="Shield Protection: création de salon non autorisée")
                except Exception as e:
                    logger.error(f"Failed to delete channel {channel.name}: {e}")

                await apply_punishment(guild, user, "anti_channel_create")
                await send_protection_log(guild, "anti_channel_create", user, f"{user} a créé un salon.")
                await log_to_db('warn', f'Channel creation blocked: {user} created channel {channel.name} in {guild.name}')
                break
        except Exception as e:
            logger.error(f"Error in channel create protection: {e}")

    async def on_guild_channel_delete(self, channel):
        guild = channel.guild
        if not guild:
            return

        try:
            await asyncio.sleep(0.5)
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_delete):
                if entry.target.id == channel.id:
                    await send_audit_log(guild, "channel", "Salon supprimé",
                        f"**Salon:** `{channel.name}`\n**ID:** `{channel.id}`\n**Par:** {entry.user.mention} (`{entry.user}`)", color=0xe74c3c)
                    break
        except Exception:
            pass

        enabled = await is_protection_enabled(guild.id, "anti_channel_delete")
        if not enabled:
            return

        try:
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_delete):
                if entry.target.id != channel.id:
                    break
                user = entry.user
                if user.id == self.user.id:
                    return
                if user.id == guild.owner_id:
                    return

                is_allowed = await should_bypass_protection(guild, user.id, "anti_channel_delete")
                if is_allowed:
                    return

                await apply_punishment(guild, user, "anti_channel_delete")
                await send_protection_log(guild, "anti_channel_delete", user, f"{user} a supprimé un salon.")
                await log_to_db('warn', f'Channel deletion blocked: {user} deleted channel {channel.name} in {guild.name}')
                break
        except Exception as e:
            logger.error(f"Error in channel delete protection: {e}")

    async def on_guild_channel_update(self, before, after):
        guild = after.guild
        if not guild:
            return

        try:
            changes = []
            if before.name != after.name:
                changes.append(f"**Nom:** `{before.name}` → `{after.name}`")
            before_topic = before.topic or ""
            after_topic = after.topic or ""
            if hasattr(before, 'topic') and hasattr(after, 'topic') and before_topic != after_topic:
                changes.append(f"**Sujet:** `{before_topic or 'Aucun'}` → `{after_topic or 'Aucun'}`")
            before_nsfw = getattr(before, 'nsfw', None)
            after_nsfw = getattr(after, 'nsfw', None)
            if before_nsfw is not None and before_nsfw != after_nsfw:
                changes.append(f"**NSFW:** `{before_nsfw}` → `{after_nsfw}`")
            before_slowmode = getattr(before, 'slowmode_delay', None)
            after_slowmode = getattr(after, 'slowmode_delay', None)
            if before_slowmode is not None and before_slowmode != after_slowmode:
                changes.append(f"**Slowmode:** `{before_slowmode}s` → `{after_slowmode}s`")
            if changes:
                await asyncio.sleep(0.3)
                executor_str = ""
                try:
                    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_update):
                        if entry.target.id == after.id:
                            executor_str = f"\n**Par:** {entry.user.mention} (`{entry.user}`)"
                            break
                except Exception:
                    pass
                await send_audit_log(guild, "channel", "Salon modifié",
                    f"**Salon:** {after.mention} (`{after.name}`)\n**ID:** `{after.id}`\n" + "\n".join(changes) + executor_str, color=0xf39c12)
        except Exception:
            pass

        if before.overwrites != after.overwrites:
            enabled_perm = await is_protection_enabled(guild.id, "anti_channel_perm_update")
            if enabled_perm:
                try:
                    await asyncio.sleep(0.5)
                    action = discord.AuditLogAction.overwrite_update
                    async for entry in guild.audit_logs(limit=1, action=action):
                        user = entry.user
                        if user.id == self.user.id:
                            break
                        if user.id == guild.owner_id:
                            break
                        is_allowed = await should_bypass_protection(guild, user.id, "anti_channel_perm_update")
                        if is_allowed:
                            break

                        try:
                            await after.edit(overwrites=before.overwrites, reason="Shield Protection: modification de permissions non autorisée")
                        except Exception:
                            pass

                        await apply_punishment(guild, user, "anti_channel_perm_update")
                        await send_protection_log(guild, "anti_channel_perm_update", user, f"{user} a modifié les permissions d'un salon.")
                        await log_to_db('warn', f'Channel perm update blocked: {user} in {guild.name}')
                        break
                except Exception as e:
                    logger.error(f"Error in channel perm update protection: {e}")

        enabled = await is_protection_enabled(guild.id, "anti_channel_update")
        if not enabled:
            return

        try:
            await asyncio.sleep(0.5)
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_update):
                if entry.target.id != after.id:
                    break
                user = entry.user
                if user.id == self.user.id:
                    return
                if user.id == guild.owner_id:
                    return

                is_allowed = await should_bypass_protection(guild, user.id, "anti_channel_update")
                if is_allowed:
                    return

                await apply_punishment(guild, user, "anti_channel_update")
                await send_protection_log(guild, "anti_channel_update", user, f"{user} a modifié un salon.")
                await log_to_db('warn', f'Channel update blocked: {user} updated channel {after.name} in {guild.name}')
                break
        except Exception as e:
            logger.error(f"Error in channel update protection: {e}")

    async def on_guild_update(self, before, after):
        try:
            changes = []
            if before.name != after.name:
                changes.append(f"**Nom:** `{before.name}` → `{after.name}`")
            if before.icon != after.icon:
                changes.append("**Icône modifiée**")
            if before.banner != after.banner:
                changes.append("**Bannière modifiée**")
            if before.verification_level != after.verification_level:
                changes.append(f"**Vérification:** `{before.verification_level}` → `{after.verification_level}`")
            if changes:
                await asyncio.sleep(0.3)
                executor_str = ""
                try:
                    async for entry in after.audit_logs(limit=1, action=discord.AuditLogAction.guild_update):
                        executor_str = f"\n**Par:** {entry.user.mention} (`{entry.user}`)"
                        break
                except Exception:
                    pass
                await send_audit_log(after, "server", "Serveur modifié",
                    "\n".join(changes) + executor_str, color=0xf39c12)
        except Exception:
            pass

        enabled = await is_protection_enabled(after.id, "anti_server_update")
        if not enabled:
            return

        try:
            await asyncio.sleep(0.5)
            async for entry in after.audit_logs(limit=1, action=discord.AuditLogAction.guild_update):
                user = entry.user
                if user.id == self.user.id:
                    return
                if user.id == after.owner_id:
                    return

                is_allowed = await should_bypass_protection(after, user.id, "anti_server_update")
                if is_allowed:
                    return

                await apply_punishment(after, user, "anti_server_update")
                await send_protection_log(after, "anti_server_update", user, f"{user} a modifié le serveur.")
                await log_to_db('warn', f'Server update blocked: {user} updated server {after.name}')
                break
        except Exception as e:
            logger.error(f"Error in server update protection: {e}")

    async def on_member_ban(self, guild, user):
        try:
            await asyncio.sleep(0.3)
            executor_str = ""
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.ban):
                if entry.target.id == user.id:
                    executor_str = f"\n**Par:** {entry.user.mention} (`{entry.user}`)"
                    break
            await send_audit_log(guild, "member", "Membre banni",
                f"**Utilisateur:** `{user}` (`{user.id}`)" + executor_str, color=0xe74c3c,
                thumbnail_url=user.display_avatar.url if hasattr(user, 'display_avatar') else None)
        except Exception:
            pass

        enabled = await is_protection_enabled(guild.id, "anti_ban")
        if not enabled:
            return

        try:
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.ban):
                if entry.target.id != user.id:
                    break
                executor = entry.user
                if executor.id == self.user.id:
                    return
                if executor.id == guild.owner_id:
                    return

                is_allowed = await should_bypass_protection(guild, executor.id, "anti_ban")
                if is_allowed:
                    return

                try:
                    await guild.unban(user, reason="Shield Protection: bannissement non autorisé")
                except Exception:
                    pass

                await apply_punishment(guild, executor, "anti_ban")
                await send_protection_log(guild, "anti_ban", executor, f"{executor} a banni un utilisateur.", target=user)
                await log_to_db('warn', f'Ban blocked: {executor} banned {user} in {guild.name}')
                break
        except Exception as e:
            logger.error(f"Error in ban protection: {e}")

    async def on_member_unban(self, guild, user):
        try:
            await asyncio.sleep(0.3)
            executor_str = ""
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.unban):
                if entry.target.id == user.id:
                    executor_str = f"\n**Par:** {entry.user.mention} (`{entry.user}`)"
                    break
            await send_audit_log(guild, "member", "Membre débanni",
                f"**Utilisateur:** `{user}` (`{user.id}`)" + executor_str, color=0x2ecc71,
                thumbnail_url=user.display_avatar.url if hasattr(user, 'display_avatar') else None)
        except Exception:
            pass

        enabled = await is_protection_enabled(guild.id, "anti_unban")
        if not enabled:
            return

        try:
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.unban):
                if entry.target.id != user.id:
                    break
                executor = entry.user
                if executor.id == self.user.id:
                    return
                if executor.id == guild.owner_id:
                    return

                is_allowed = await should_bypass_protection(guild, executor.id, "anti_unban")
                if is_allowed:
                    return

                try:
                    await guild.ban(user, reason="Shield Protection: débannissement non autorisé")
                except Exception:
                    pass

                await apply_punishment(guild, executor, "anti_unban")
                await send_protection_log(guild, "anti_unban", executor, f"{executor} a débanni un utilisateur.", target=user)
                await log_to_db('warn', f'Unban blocked: {executor} unbanned {user} in {guild.name}')
                break
        except Exception as e:
            logger.error(f"Error in unban protection: {e}")

    async def on_member_remove(self, member):
        try:
            if pool:
                row = await pool.fetchrow(
                    "SELECT leave_channel_id FROM guild_join_leave WHERE guild_id = $1",
                    str(member.guild.id)
                )
                if row and row['leave_channel_id']:
                    channel = member.guild.get_channel(int(row['leave_channel_id']))
                    if channel:
                        created = int(member.created_at.timestamp())
                        member_count = member.guild.member_count or 0
                        roles = [r.mention for r in member.roles if r.name != "@everyone"]
                        roles_str = ", ".join(roles) if roles else "Aucun"
                        embed = discord.Embed(
                            title="Membre parti",
                            description=(
                                f"**Utilisateur:** {member.mention} (`{member}`)\n"
                                f"**ID:** `{member.id}`\n"
                                f"**Compte créé le:** <t:{created}:F> (<t:{created}:R>)\n"
                                f"**Rôles:** {roles_str}\n"
                                f"**Membres:** {member_count}"
                            ),
                            color=0xe74c3c
                        )
                        embed.set_thumbnail(url=member.display_avatar.url)
                        embed.set_footer(text=f"ID: {member.id}")
                        await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Error sending leave embed: {e}")

        guild = member.guild
        enabled = await is_protection_enabled(guild.id, "anti_kick")
        if not enabled:
            return

        try:
            await asyncio.sleep(0.5)
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.kick):
                if entry.target.id != member.id:
                    break
                user = entry.user
                if user.id == self.user.id:
                    return
                if user.id == guild.owner_id:
                    return

                is_allowed = await should_bypass_protection(guild, user.id, "anti_kick")
                if is_allowed:
                    return

                await apply_punishment(guild, user, "anti_kick")
                await send_protection_log(guild, "anti_kick", user, f"{user} a expulsé un utilisateur.", target=member)
                await log_to_db('warn', f'Kick blocked: {user} kicked {member} in {guild.name}')
                break
        except Exception as e:
            logger.error(f"Error in kick protection: {e}")

    async def on_webhooks_update(self, channel):
        guild = channel.guild
        if not guild:
            return
        enabled = await is_protection_enabled(guild.id, "anti_webhook_create")
        if not enabled:
            return

        try:
            await asyncio.sleep(0.5)
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.webhook_create):
                user = entry.user
                if user.id == self.user.id:
                    return
                if user.id == guild.owner_id:
                    return

                is_allowed = await should_bypass_protection(guild, user.id, "anti_webhook_create")
                if is_allowed:
                    return

                try:
                    webhooks = await channel.webhooks()
                    for wh in webhooks:
                        if wh.user and wh.user.id == user.id:
                            await wh.delete(reason="Shield Protection: création de webhook non autorisée")
                except Exception:
                    pass

                await apply_punishment(guild, user, "anti_webhook_create")
                await send_protection_log(guild, "anti_webhook_create", user, f"{user} a créé un webhook.")
                await log_to_db('warn', f'Webhook creation blocked: {user} in {guild.name}')
                break
        except Exception as e:
            logger.error(f"Error in webhook protection: {e}")

    async def on_member_join(self, member):
        try:
            if pool:
                row = await pool.fetchrow(
                    "SELECT join_channel_id FROM guild_join_leave WHERE guild_id = $1",
                    str(member.guild.id)
                )
                if row and row['join_channel_id']:
                    channel = member.guild.get_channel(int(row['join_channel_id']))
                    if channel:
                        created = int(member.created_at.timestamp())
                        joined = int(member.joined_at.timestamp()) if member.joined_at else 0
                        member_count = member.guild.member_count or 0
                        embed = discord.Embed(
                            title="Membre rejoint",
                            description=(
                                f"**Utilisateur:** {member.mention} (`{member}`)\n"
                                f"**ID:** `{member.id}`\n"
                                f"**Compte créé le:** <t:{created}:F> (<t:{created}:R>)\n"
                                f"**A rejoint le:** <t:{joined}:F> (<t:{joined}:R>)\n"
                                f"**Membres:** {member_count}"
                            ),
                            color=0x2ecc71
                        )
                        embed.set_thumbnail(url=member.display_avatar.url)
                        embed.set_footer(text=f"ID: {member.id}")
                        await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Error sending join embed: {e}")

        if member.id == BOT_OWNER_ID:
            return
        if not member.bot and pool:
            bl = await pool.fetchrow(
                "SELECT id FROM blacklist WHERE user_id = $1",
                str(member.id)
            )
            if bl:
                try:
                    await member.ban(reason="Shield Blacklist: utilisateur blacklisté globalement")
                    await log_to_db('warn', f'Blacklisted user {member} auto-banned from {member.guild.name}')
                except Exception as e:
                    logger.error(f"Failed to ban blacklisted user {member}: {e}")
                return

        if member.bot:
            guild = member.guild
            enabled = await is_protection_enabled(guild.id, "anti_bot_add")
            if not enabled:
                return

            try:
                await asyncio.sleep(0.5)
                async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.bot_add):
                    if entry.target.id != member.id:
                        break
                    user = entry.user
                    if user.id == self.user.id:
                        return
                    if user.id == guild.owner_id:
                        return

                    is_allowed = await should_bypass_protection(guild, user.id, "anti_bot_add")
                    if is_allowed:
                        return

                    try:
                        await member.kick(reason="Shield Protection: ajout de bot non autorisé")
                    except Exception:
                        pass

                    await apply_punishment(guild, user, "anti_bot_add")
                    await send_protection_log(guild, "anti_bot_add", user, f"{user} a ajouté un bot.")
                    await log_to_db('warn', f'Bot add blocked: {user} added bot {member} in {guild.name}')
                    break
            except Exception as e:
                logger.error(f"Error in bot add protection: {e}")

    async def on_thread_create(self, thread):
        guild = thread.guild
        if not guild:
            return
        enabled = await is_protection_enabled(guild.id, "anti_thread_create")
        if not enabled:
            return

        try:
            await asyncio.sleep(0.5)
            user = thread.owner
            if not user:
                try:
                    user = await guild.fetch_member(thread.owner_id)
                except Exception:
                    return
            if user.id == self.user.id:
                return
            if user.id == guild.owner_id:
                return

            is_allowed = await should_bypass_protection(guild, user.id, "anti_thread_create")
            if is_allowed:
                return

            try:
                await thread.delete()
            except Exception:
                pass

            await apply_punishment(guild, user, "anti_thread_create")
            await send_protection_log(guild, "anti_thread_create", user, f"{user} a créé un fil de discussion.")
            await log_to_db('warn', f'Thread creation blocked: {user} in {guild.name}')
        except Exception as e:
            logger.error(f"Error in thread create protection: {e}")

    async def on_voice_state_update(self, member, before, after):
        guild = member.guild

        try:
            if before.channel and not after.channel:
                await asyncio.sleep(0.3)
                try:
                    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.member_disconnect):
                        import time as _t2
                        if abs(_t2.time() - entry.created_at.timestamp()) < 5:
                            await send_audit_log(guild, "voice", "Membre déconnecté du vocal",
                                f"**Membre:** {member.mention} (`{member}`)\n**Salon:** `{before.channel.name}`\n**Par:** {entry.user.mention} (`{entry.user}`)", color=0xe74c3c,
                                thumbnail_url=member.display_avatar.url)
                        break
                except Exception:
                    pass
            if before.channel and after.channel and before.channel != after.channel:
                await asyncio.sleep(0.3)
                try:
                    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.member_move):
                        import time as _t2
                        if abs(_t2.time() - entry.created_at.timestamp()) < 5:
                            await send_audit_log(guild, "voice", "Membre déplacé",
                                f"**Membre:** {member.mention} (`{member}`)\n**De:** `{before.channel.name}`\n**Vers:** `{after.channel.name}`\n**Par:** {entry.user.mention} (`{entry.user}`)", color=0xf39c12,
                                thumbnail_url=member.display_avatar.url)
                        break
                except Exception:
                    pass
            if not before.mute and after.mute:
                await asyncio.sleep(0.3)
                try:
                    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.member_update):
                        import time as _t2
                        if entry.target.id == member.id and abs(_t2.time() - entry.created_at.timestamp()) < 5:
                            await send_audit_log(guild, "voice", "Membre mis en muet",
                                f"**Membre:** {member.mention} (`{member}`)\n**Par:** {entry.user.mention} (`{entry.user}`)", color=0xe74c3c,
                                thumbnail_url=member.display_avatar.url)
                        break
                except Exception:
                    pass
            if not before.deaf and after.deaf:
                await asyncio.sleep(0.3)
                try:
                    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.member_update):
                        import time as _t2
                        if entry.target.id == member.id and abs(_t2.time() - entry.created_at.timestamp()) < 5:
                            await send_audit_log(guild, "voice", "Membre mis en sourdine",
                                f"**Membre:** {member.mention} (`{member}`)\n**Par:** {entry.user.mention} (`{entry.user}`)", color=0xe74c3c,
                                thumbnail_url=member.display_avatar.url)
                        break
                except Exception:
                    pass
        except Exception:
            pass

        if after.channel and pool:
            try:
                if member.id == guild.owner_id or member.id == self.user.id:
                    pass
                elif BOT_OWNER_ID and member.id == BOT_OWNER_ID:
                    pass
                else:
                    restricted = await pool.fetchval(
                        "SELECT COUNT(*) FROM salon_access WHERE guild_id = $1 AND channel_id = $2",
                        str(guild.id), str(after.channel.id)
                    )
                    if restricted and restricted > 0:
                        allowed = await pool.fetchval(
                            "SELECT COUNT(*) FROM salon_access WHERE guild_id = $1 AND channel_id = $2 AND user_id = $3",
                            str(guild.id), str(after.channel.id), str(member.id)
                        )
                        if not allowed or allowed == 0:
                            is_ol = await is_owner_or_ownerlist(guild, member.id)
                            if not is_ol:
                                tracker_key = f"{guild.id}-{member.id}-{after.channel.id}"
                                import time as _time
                                now = _time.time()
                                if tracker_key not in salon_join_tracker:
                                    salon_join_tracker[tracker_key] = []
                                salon_join_tracker[tracker_key] = [t for t in salon_join_tracker[tracker_key] if now - t < 15]
                                salon_join_tracker[tracker_key].append(now)

                                timed_out = False
                                if len(salon_join_tracker[tracker_key]) >= 3:
                                    try:
                                        from datetime import timedelta as td
                                        await member.timeout(td(seconds=60), reason="Shield: spam de rejoindre un salon vocal restreint")
                                        await log_to_db('warn', f'{member} timed out 60s for spamming voice channel {after.channel.name} in {guild.name}')
                                        timed_out = True
                                    except Exception as te:
                                        logger.error(f"Failed to timeout {member}: {te}")
                                    salon_join_tracker[tracker_key] = []

                                try:
                                    await member.move_to(None, reason="Accès non autorisé au salon vocal (Shield)")
                                except Exception:
                                    pass
                                await log_to_db('info', f'{member} kicked from voice channel {after.channel.name} in {guild.name} (not in salon_access)')

                                try:
                                    log_ch = None
                                    prot = await get_protection(str(guild.id), "salon_access")
                                    if prot and prot['log_channel_id']:
                                        log_ch = guild.get_channel(int(prot['log_channel_id']))
                                    if not log_ch:
                                        category = discord.utils.get(guild.categories, name="RShield - Logs")
                                        if category:
                                            log_ch = discord.utils.get(category.text_channels, name="logs・salon-access")
                                    if log_ch:
                                        log_embed = discord.Embed(
                                            title="Tentative d'accès à un salon restreint",
                                            description=(
                                                f"**Utilisateur:** {member.mention} (`{member}`)\n"
                                                f"**ID:** `{member.id}`\n"
                                                f"**Salon:** {after.channel.mention}\n"
                                                f"**Action:** Déconnecté du salon"
                                                + ("\n**⚠️ Timeout 60s appliqué (spam)**" if timed_out else "")
                                            ),
                                            color=0xe74c3c
                                        )
                                        log_embed.set_thumbnail(url=member.display_avatar.url)
                                        log_embed.set_footer(text=f"ID: {member.id}")
                                        await log_ch.send(embed=log_embed)
                                except Exception as le:
                                    logger.error(f"Error sending salon access log: {le}")
            except Exception as e:
                logger.error(f"Error in salon access check: {e}")

        if before.channel and not after.channel and before.channel != after.channel:
            enabled = await is_protection_enabled(guild.id, "anti_disconnect")
            if enabled:
                try:
                    await asyncio.sleep(0.5)
                    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.member_disconnect):
                        user = entry.user
                        if user.id == self.user.id:
                            return
                        if user.id == guild.owner_id:
                            return
                        is_allowed = await should_bypass_protection(guild, user.id, "anti_disconnect")
                        if is_allowed:
                            return

                        await apply_punishment(guild, user, "anti_disconnect")
                        await send_protection_log(guild, "anti_disconnect", user, f"{user} a déconnecté un utilisateur.", target=member)
                        await log_to_db('warn', f'Disconnect blocked: {user} disconnected {member} in {guild.name}')
                        break
                except Exception as e:
                    logger.error(f"Error in disconnect protection: {e}")

        if before.channel and after.channel and before.channel != after.channel:
            enabled = await is_protection_enabled(guild.id, "anti_member_move")
            if enabled:
                try:
                    await asyncio.sleep(0.5)
                    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.member_move):
                        user = entry.user
                        if user.id == self.user.id:
                            return
                        if user.id == guild.owner_id:
                            return
                        is_allowed = await should_bypass_protection(guild, user.id, "anti_member_move")
                        if is_allowed:
                            return

                        await apply_punishment(guild, user, "anti_member_move")
                        await send_protection_log(guild, "anti_member_move", user, f"{user} a déplacé un utilisateur.", target=member)
                        await log_to_db('warn', f'Member move blocked: {user} moved {member} in {guild.name}')
                        break
                except Exception as e:
                    logger.error(f"Error in member move protection: {e}")

        if not before.mute and after.mute:
            enabled = await is_protection_enabled(guild.id, "anti_mute")
            if enabled:
                try:
                    await asyncio.sleep(0.5)
                    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.member_update):
                        if entry.target.id != member.id:
                            break
                        user = entry.user
                        if user.id == self.user.id:
                            return
                        if user.id == guild.owner_id:
                            return
                        is_allowed = await should_bypass_protection(guild, user.id, "anti_mute")
                        if is_allowed:
                            return

                        try:
                            await member.edit(mute=False, reason="Shield Protection: mise en muet non autorisée")
                        except Exception:
                            pass

                        await apply_punishment(guild, user, "anti_mute")
                        await send_protection_log(guild, "anti_mute", user, f"{user} a mis en muet un utilisateur.", target=member)
                        await log_to_db('warn', f'Mute blocked: {user} muted {member} in {guild.name}')
                        break
                except Exception as e:
                    logger.error(f"Error in mute protection: {e}")

    async def on_guild_emojis_update(self, guild, before, after):
        enabled = await is_protection_enabled(guild.id, "anti_emoji_update")
        if not enabled:
            return

        try:
            await asyncio.sleep(0.5)
            added = set(after) - set(before)
            removed = set(before) - set(after)

            if added:
                action = discord.AuditLogAction.emoji_create
            elif removed:
                action = discord.AuditLogAction.emoji_delete
            else:
                action = discord.AuditLogAction.emoji_update

            async for entry in guild.audit_logs(limit=1, action=action):
                user = entry.user
                if user.id == self.user.id:
                    return
                if user.id == guild.owner_id:
                    return
                is_allowed = await should_bypass_protection(guild, user.id, "anti_emoji_update")
                if is_allowed:
                    return

                if added:
                    for emoji in added:
                        try:
                            await emoji.delete(reason="Shield Protection: modification d'emoji non autorisée")
                        except Exception:
                            pass

                await apply_punishment(guild, user, "anti_emoji_update")
                await send_protection_log(guild, "anti_emoji_update", user, f"{user} a modifié les emojis du serveur.")
                await log_to_db('warn', f'Emoji update blocked: {user} in {guild.name}')
                break
        except Exception as e:
            logger.error(f"Error in emoji update protection: {e}")

    async def on_guild_role_update(self, before, after):
        guild = after.guild

        try:
            changes = []
            if before.name != after.name:
                changes.append(f"**Nom:** `{before.name}` → `{after.name}`")
            if before.color != after.color:
                changes.append(f"**Couleur:** `{before.color}` → `{after.color}`")
            if before.permissions != after.permissions:
                changes.append("**Permissions modifiées**")
            if before.hoist != after.hoist:
                changes.append(f"**Affiché séparément:** `{before.hoist}` → `{after.hoist}`")
            if changes:
                await asyncio.sleep(0.3)
                executor_str = ""
                try:
                    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.role_update):
                        if entry.target.id == after.id:
                            executor_str = f"\n**Par:** {entry.user.mention} (`{entry.user}`)"
                            break
                except Exception:
                    pass
                await send_audit_log(guild, "role", "Rôle modifié",
                    f"**Rôle:** {after.mention} (`{after.name}`)\n**ID:** `{after.id}`\n" + "\n".join(changes) + executor_str, color=0xf39c12)
        except Exception:
            pass

        if before.position != after.position and before.permissions == after.permissions and before.name == after.name:
            enabled_pos = await is_protection_enabled(guild.id, "anti_role_position")
            if enabled_pos:
                try:
                    await asyncio.sleep(0.5)
                    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.role_update):
                        if entry.target.id != after.id:
                            break
                        user = entry.user
                        if user.id == self.user.id:
                            break
                        if user.id == guild.owner_id:
                            break
                        is_allowed = await should_bypass_protection(guild, user.id, "anti_role_position")
                        if is_allowed:
                            break

                        await apply_punishment(guild, user, "anti_role_position")
                        await send_protection_log(guild, "anti_role_position", user, f"{user} a modifié la position des rôles.", role=after)
                        await log_to_db('warn', f'Role position change blocked: {user} in {guild.name}')
                        break
                except Exception as e:
                    logger.error(f"Error in role position protection: {e}")

        dangerous_perms = [
            'administrator', 'ban_members', 'kick_members', 'manage_guild',
            'manage_roles', 'manage_channels', 'mention_everyone', 'manage_webhooks'
        ]
        if before.permissions != after.permissions:
            new_dangerous = []
            for perm_name in dangerous_perms:
                had = getattr(before.permissions, perm_name, False)
                has = getattr(after.permissions, perm_name, False)
                if not had and has:
                    new_dangerous.append(perm_name)

            if new_dangerous:
                enabled_danger = await is_protection_enabled(guild.id, "anti_role_dangerous_perm")
                if enabled_danger:
                    try:
                        await asyncio.sleep(0.3)
                        async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.role_update):
                            if entry.target.id != after.id:
                                break
                            user = entry.user
                            if user.id == self.user.id:
                                break
                            if user.id == guild.owner_id:
                                break
                            is_allowed = await should_bypass_protection(guild, user.id, "anti_role_dangerous_perm")
                            if is_allowed:
                                break

                            try:
                                await after.edit(permissions=before.permissions, reason="Shield Protection: permission dangereuse bloquée")
                            except Exception:
                                pass

                            perm_list = ", ".join(new_dangerous)
                            await apply_punishment(guild, user, "anti_role_dangerous_perm")
                            await send_protection_log(guild, "anti_role_dangerous_perm", user, f"{user} a ajouté des permissions dangereuses ({perm_list}).", role=after)
                            await log_to_db('warn', f'Dangerous perm blocked: {user} added {perm_list} to {after.name} in {guild.name}')
                            break
                    except Exception as e:
                        logger.error(f"Error in dangerous perm protection: {e}")
                    return

        if before.permissions == after.permissions and before.name == after.name and before.color == after.color:
            return

        enabled = await is_protection_enabled(guild.id, "anti_role_update")
        if not enabled:
            return

        try:
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.role_update):
                if entry.target.id != after.id:
                    break
                user = entry.user
                if user.id == self.user.id:
                    return
                if user.id == guild.owner_id:
                    return

                is_allowed = await should_bypass_protection(guild, user.id, "anti_role_update")
                if is_allowed:
                    return

                try:
                    await after.edit(
                        permissions=before.permissions,
                        name=before.name,
                        color=before.color,
                        reason="Shield Protection: modification non autorisée"
                    )
                except Exception as e:
                    logger.error(f"Failed to restore role {after.name}: {e}")
                    await log_to_db('error', f'Failed to restore role {after.name}: {e}')

                await apply_punishment(guild, user, "anti_role_update")
                await send_protection_log(guild, "anti_role_update", user, f"{user} a modifié un rôle.", role=after)
                await log_to_db('warn', f'Role modification blocked: {user} modified role {after.name} in {guild.name}')
                break
        except Exception as e:
            logger.error(f"Error in role update protection: {e}")

    async def on_member_update(self, before, after):
        guild = after.guild

        try:
            if before.roles != after.roles:
                added = [r for r in after.roles if r not in before.roles]
                removed = [r for r in before.roles if r not in after.roles]
                if added or removed:
                    await asyncio.sleep(0.3)
                    executor_str = ""
                    try:
                        async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.member_role_update):
                            if entry.target.id == after.id:
                                executor_str = f"\n**Par:** {entry.user.mention} (`{entry.user}`)"
                                break
                    except Exception:
                        pass
                    desc = f"**Membre:** {after.mention} (`{after}`)\n**ID:** `{after.id}`"
                    if added:
                        desc += f"\n**Rôle(s) ajouté(s):** {', '.join(r.mention for r in added)}"
                    if removed:
                        desc += f"\n**Rôle(s) retiré(s):** {', '.join(r.mention for r in removed)}"
                    desc += executor_str
                    color = 0x2ecc71 if added and not removed else 0xe74c3c if removed and not added else 0xf39c12
                    await send_audit_log(guild, "role", "Rôle(s) modifié(s) sur un membre", desc, color=color,
                        thumbnail_url=after.display_avatar.url)

            if before.timed_out_until != after.timed_out_until and after.timed_out_until is not None:
                await asyncio.sleep(0.3)
                executor_str = ""
                try:
                    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.member_update):
                        if entry.target.id == after.id:
                            executor_str = f"\n**Par:** {entry.user.mention} (`{entry.user}`)"
                            break
                except Exception:
                    pass
                await send_audit_log(guild, "member", "Membre exclu temporairement",
                    f"**Membre:** {after.mention} (`{after}`)\n**ID:** `{after.id}`" + executor_str, color=0xe74c3c,
                    thumbnail_url=after.display_avatar.url)
        except Exception:
            pass

        if before.timed_out_until != after.timed_out_until and after.timed_out_until is not None:
            enabled = await is_protection_enabled(guild.id, "anti_timeout")
            if enabled:
                try:
                    await asyncio.sleep(0.5)
                    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.member_update):
                        if entry.target.id != after.id:
                            break
                        user = entry.user
                        if user.id == self.user.id:
                            break
                        if user.id == guild.owner_id:
                            break
                        is_allowed = await should_bypass_protection(guild, user.id, "anti_timeout")
                        if is_allowed:
                            break

                        try:
                            await after.timeout(None, reason="Shield Protection: exclusion temporaire non autorisée")
                        except Exception:
                            pass

                        await apply_punishment(guild, user, "anti_timeout")
                        await send_protection_log(guild, "anti_timeout", user, f"{user} a exclu temporairement un utilisateur.", target=after)
                        await log_to_db('warn', f'Timeout blocked: {user} timed out {after} in {guild.name}')
                        break
                except Exception as e:
                    logger.error(f"Error in timeout protection: {e}")

        if before.roles == after.roles:
            return

        added_roles = set(after.roles) - set(before.roles)
        removed_roles = set(before.roles) - set(after.roles)

        if added_roles:
            prot_key = "anti_role_add"
        elif removed_roles:
            prot_key = "anti_role_remove"
        else:
            return

        enabled = await is_protection_enabled(guild.id, prot_key)
        if not enabled:
            return

        try:
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.member_role_update):
                if entry.target.id != after.id:
                    break
                user = entry.user
                if user.id == self.user.id:
                    return
                if user.id == guild.owner_id:
                    return

                is_allowed = await should_bypass_protection(guild, user.id, prot_key)
                if is_allowed:
                    return

                try:
                    if added_roles:
                        safe_to_remove = [r for r in added_roles if r < guild.me.top_role]
                        if safe_to_remove:
                            await after.remove_roles(*safe_to_remove, reason="Shield Protection: ajout de rôle non autorisé")
                    if removed_roles:
                        safe_to_add = [r for r in removed_roles if r < guild.me.top_role]
                        if safe_to_add:
                            await after.add_roles(*safe_to_add, reason="Shield Protection: retrait de rôle non autorisé")
                except Exception as e:
                    logger.error(f"Failed to restore member roles for {after}: {e}")
                    await log_to_db('error', f'Failed to restore member roles for {after}: {e}')

                await apply_punishment(guild, user, prot_key)
                if added_roles:
                    for r in added_roles:
                        await send_protection_log(guild, prot_key, user, f"{user} a ajouté un rôle à un utilisateur.", role=r, target=after)
                elif removed_roles:
                    for r in removed_roles:
                        await send_protection_log(guild, prot_key, user, f"{user} a retiré un rôle à un utilisateur.", role=r, target=after)
                await log_to_db('warn', f'Member role change blocked: {user} modified roles of {after} in {guild.name}')
                break
        except Exception as e:
            logger.error(f"Error in member role protection: {e}")

    async def on_message(self, message):
        if message.author == self.user:
            return
        if message.author.bot:
            return

        # ── Relais anonyme des contrats ──
        if message.guild is None:
            relays = await contrat_relay_list_by_commanditaire(message.author.id)
            if relays:
                target_relay = None
                if message.reference and message.reference.message_id:
                    routes = getattr(self, '_contrat_dm_routes', {})
                    ch_id = routes.get(message.reference.message_id)
                    if ch_id is not None:
                        for r in relays:
                            if int(r['channel_id']) == ch_id:
                                target_relay = r
                                break
                if target_relay is None:
                    if len(relays) == 1:
                        target_relay = relays[0]
                    else:
                        try:
                            await message.channel.send(
                                "⚠️ Tu as plusieurs contrats anonymes actifs. "
                                "**Réponds** (clic droit → Répondre) à un message que je t'ai "
                                "relayé pour choisir le bon salon.")
                        except Exception:
                            pass
                        return
                channel = self.get_channel(int(target_relay['channel_id']))
                if channel is None:
                    await contrat_relay_remove(int(target_relay['channel_id']))
                    try:
                        await message.channel.send(
                            "❌ Le salon de ce contrat n'existe plus, le relais est fermé.")
                    except Exception:
                        pass
                    return
                content = (message.content or "").strip()
                if message.attachments:
                    extra = "\n".join(a.url for a in message.attachments)
                    content = (content + "\n" + extra).strip()
                if not content:
                    return
                relay_embed = discord.Embed(
                    description=f"# Missive reçue du Commanditaire\n\n{_format_missive(content)}",
                    color=CONTRAT_MISSIVE_COLOR)
                relay_embed.set_footer(text="Expéditeur anonyme")
                try:
                    await channel.send(embed=relay_embed)
                    await message.channel.send(
                        "✅ Message transmis **anonymement** dans le salon du contrat.")
                except Exception:
                    try:
                        await message.channel.send(
                            "❌ Impossible de transmettre ton message (le bot n'a peut-être "
                            "plus accès au salon).")
                    except Exception:
                        pass
                return

        elif getattr(message.channel, "category_id", None) == CONTRAT_CATEGORY_ID:
            relay = await contrat_relay_get_by_channel(message.channel.id)
            if relay:
                commanditaire_id = int(relay['commanditaire_id'])
                if message.author.id != commanditaire_id:
                    content = (message.content or "").strip()
                    if message.attachments:
                        extra = "\n".join(a.url for a in message.attachments)
                        content = (content + "\n" + extra).strip()
                    if content:
                        target = self.get_user(commanditaire_id)
                        if target is None:
                            try:
                                target = await self.fetch_user(commanditaire_id)
                            except Exception:
                                target = None
                        if target is not None:
                            dm_relay = discord.Embed(
                                description=f"# Missive reçue du Mercenaire\n\n{_format_missive(content)}",
                                color=CONTRAT_MISSIVE_COLOR)
                            dm_relay.set_footer(
                                text=f"De {message.author.display_name} — réponds à ce message pour répondre")
                            try:
                                sent = await target.send(embed=dm_relay)
                                if not hasattr(self, '_contrat_dm_routes'):
                                    self._contrat_dm_routes = {}
                                self._contrat_dm_routes[sent.id] = message.channel.id
                                if len(self._contrat_dm_routes) > 2000:
                                    for k in list(self._contrat_dm_routes)[:1000]:
                                        self._contrat_dm_routes.pop(k, None)
                            except Exception:
                                pass
                return

        is_bot_ping = message.guild and (self.user in message.mentions or (message.reference and message.reference.resolved and hasattr(message.reference.resolved, 'author') and message.reference.resolved.author == self.user))
        if is_bot_ping:
            user = message.author
            if not await can_use_bot(message.guild, user.id):
                if not hasattr(self, '_ping_tracker'):
                    self._ping_tracker = {}
                now = asyncio.get_event_loop().time()
                key = (message.guild.id, user.id)
                if key not in self._ping_tracker:
                    self._ping_tracker[key] = []
                self._ping_tracker[key] = [t for t in self._ping_tracker[key] if now - t < 15]
                self._ping_tracker[key].append(now)
                if len(self._ping_tracker[key]) >= 3:
                    self._ping_tracker[key] = []
                    member = message.guild.get_member(user.id)
                    if member:
                        try:
                            from datetime import timedelta
                            await member.timeout(timedelta(minutes=5), reason="Shield Protection: spam ping du bot")
                            embed = discord.Embed(description=f"{user.mention} a été timeout 5 minutes pour spam ping du bot.", color=0x2b2d31)
                            await message.channel.send(embed=embed)
                            await log_to_db('warn', f'{user} timed out for bot ping spam in {message.guild.name}')
                        except Exception as e:
                            logger.error(f"Failed to timeout ping spammer {user}: {e}")

        if message.guild:
            user = message.author

            link_pattern = re.compile(r'https?://\S+|discord\.gg/\S+|discord\.com/invite/\S+')
            if link_pattern.search(message.content):
                enabled = await is_protection_enabled(message.guild.id, "anti_link")
                if enabled:
                    if not await should_bypass_protection(message.guild, user.id, "anti_link"):
                        try:
                            await message.delete()
                        except Exception:
                            pass
                        await apply_punishment(message.guild, user, "anti_link")
                        await send_protection_log(message.guild, "anti_link", user, f"{user} a envoyé un lien.")
                        await log_to_db('warn', f'Link blocked: {user} in {message.guild.name}')
                        return

            if len(message.mentions) >= 5:
                enabled = await is_protection_enabled(message.guild.id, "anti_mass_mention")
                if enabled:
                    if not await should_bypass_protection(message.guild, user.id, "anti_mass_mention"):
                        try:
                            await message.delete()
                        except Exception:
                            pass
                        await apply_punishment(message.guild, user, "anti_mass_mention")
                        await send_protection_log(message.guild, "anti_mass_mention", user, f"{user} a mentionné massivement ({len(message.mentions)} mentions).")
                        await log_to_db('warn', f'Mass mention blocked: {user} in {message.guild.name}')
                        return

            if not hasattr(self, '_spam_tracker'):
                self._spam_tracker = {}
            now = _time.time()
            uid = user.id
            if uid not in self._spam_tracker:
                self._spam_tracker[uid] = []
            self._spam_tracker[uid] = [t for t in self._spam_tracker[uid] if now - t < 5]
            self._spam_tracker[uid].append(now)
            if len(self._spam_tracker[uid]) >= 5:
                enabled = await is_protection_enabled(message.guild.id, "anti_spam")
                if enabled:
                    if not await should_bypass_protection(message.guild, user.id, "anti_spam"):
                        try:
                            await message.delete()
                        except Exception:
                            pass
                        self._spam_tracker[uid] = []
                        await apply_punishment(message.guild, user, "anti_spam")
                        await send_protection_log(message.guild, "anti_spam", user, f"{user} a envoyé du spam.")
                        await log_to_db('warn', f'Spam blocked: {user} in {message.guild.name}')
                        return

            has_gif = False
            if message.attachments:
                for att in message.attachments:
                    if att.filename and att.filename.lower().endswith('.gif'):
                        has_gif = True
                        break
            if not has_gif and message.content:
                gif_pattern = re.compile(r'https?://(?:tenor\.com|giphy\.com|media\.discordapp\.net|cdn\.discordapp\.com)\S*\.gif\S*', re.IGNORECASE)
                if gif_pattern.search(message.content):
                    has_gif = True
            if not has_gif and message.embeds:
                for emb in message.embeds:
                    if emb.type in ('gifv', 'image'):
                        has_gif = True
                        break
                    if emb.url and '.gif' in emb.url.lower():
                        has_gif = True
                        break
                    if emb.thumbnail and emb.thumbnail.url and '.gif' in emb.thumbnail.url.lower():
                        has_gif = True
                        break

            if has_gif:
                enabled = await is_protection_enabled(message.guild.id, "anti_gif_spam")
                if enabled:
                    if not await should_bypass_protection(message.guild, user.id, "anti_gif_spam"):
                        is_target = False
                        if pool:
                            target_row = await pool.fetchrow(
                                "SELECT id FROM gif_spam_targets WHERE guild_id = $1 AND user_id = $2",
                                str(message.guild.id), str(user.id)
                            )
                            if target_row:
                                is_target = True
                        if is_target:
                            if not hasattr(self, '_gif_spam_tracker'):
                                self._gif_spam_tracker = {}
                            now = _time.time()
                            tracker_key = f"{message.guild.id}_{user.id}"
                            if tracker_key not in self._gif_spam_tracker:
                                self._gif_spam_tracker[tracker_key] = []
                            self._gif_spam_tracker[tracker_key] = [t for t in self._gif_spam_tracker[tracker_key] if now - t < 40]
                            self._gif_spam_tracker[tracker_key].append(now)
                            if len(self._gif_spam_tracker[tracker_key]) >= 5:
                                try:
                                    await message.delete()
                                except Exception:
                                    pass
                                self._gif_spam_tracker[tracker_key] = []
                                await apply_punishment(message.guild, user, "anti_gif_spam")
                                await send_protection_log(message.guild, "anti_gif_spam", user, f"{user} a spammé des GIFs (5 en 40s).")
                                await log_to_db('warn', f'GIF spam blocked: {user} in {message.guild.name}')
                                return

            if len(message.mentions) >= 3:
                enabled = await is_protection_enabled(message.guild.id, "anti_mention_spam")
                if enabled:
                    if not await should_bypass_protection(message.guild, user.id, "anti_mention_spam"):
                        is_target = False
                        if pool:
                            target_row = await pool.fetchrow(
                                "SELECT id FROM mention_spam_targets WHERE guild_id = $1 AND user_id = $2",
                                str(message.guild.id), str(user.id)
                            )
                            if target_row:
                                is_target = True
                        if is_target:
                            if not hasattr(self, '_mention_spam_tracker'):
                                self._mention_spam_tracker = {}
                            now = _time.time()
                            tracker_key = f"{message.guild.id}_{user.id}"
                            if tracker_key not in self._mention_spam_tracker:
                                self._mention_spam_tracker[tracker_key] = []
                            self._mention_spam_tracker[tracker_key] = [t for t in self._mention_spam_tracker[tracker_key] if now - t < 8]
                            self._mention_spam_tracker[tracker_key].append(now)
                            if len(self._mention_spam_tracker[tracker_key]) >= 3:
                                try:
                                    await message.delete()
                                except Exception:
                                    pass
                                self._mention_spam_tracker[tracker_key] = []
                                await apply_punishment(message.guild, user, "anti_mention_spam")
                                await send_protection_log(message.guild, "anti_mention_spam", user, f"{user} a spammé des mentions (3+ en 8s).")
                                await log_to_db('warn', f'Mention spam blocked: {user} in {message.guild.name}')
                                return

            toxicity_words = [
                'fdp', 'ntm', 'nique', 'pute', 'connard', 'connasse',
                'enculé', 'batard', 'salope', 'merde', 'tg', 'ferme ta gueule',
                'fils de pute', 'va te faire', 'pd', 'tapette'
            ]
            msg_lower = message.content.lower()
            if any(w in msg_lower for w in toxicity_words):
                enabled = await is_protection_enabled(message.guild.id, "anti_toxicity")
                if enabled:
                    if not await should_bypass_protection(message.guild, user.id, "anti_toxicity"):
                        try:
                            await message.delete()
                        except Exception:
                            pass
                        await apply_punishment(message.guild, user, "anti_toxicity")
                        await send_protection_log(message.guild, "anti_toxicity", user, f"{user} a envoyé un message toxique.")
                        await log_to_db('warn', f'Toxicity blocked: {user} in {message.guild.name}')
                        return

    async def on_message_delete(self, message):
        if not message.guild or message.author.bot:
            return
        try:
            log_ch = await get_general_log_channel(message.guild)
            if not log_ch:
                return
            content = message.content or "*[pas de texte]*"
            if len(content) > 1024:
                content = content[:1021] + "…"
            embed = discord.Embed(
                title="🗑️ Message supprimé",
                color=0xe74c3c,
                timestamp=datetime.datetime.utcnow()
            )
            embed.add_field(name="Auteur", value=f"{message.author.mention} (`{message.author}`)", inline=True)
            embed.add_field(name="Salon", value=message.channel.mention, inline=True)
            embed.add_field(name="Contenu", value=content, inline=False)
            if message.attachments:
                embed.add_field(name="Pièces jointes", value="\n".join(a.filename for a in message.attachments), inline=False)
            await log_ch.send(embed=embed)
        except Exception as e:
            logger.error(f"on_message_delete log error: {e}")

    async def on_message_edit(self, before, after):
        if not after.guild or after.author.bot:
            return
        if before.content == after.content:
            return
        try:
            log_ch = await get_general_log_channel(after.guild)
            if not log_ch:
                return
            before_content = before.content or "*[pas de texte]*"
            after_content = after.content or "*[pas de texte]*"
            if len(before_content) > 512:
                before_content = before_content[:509] + "…"
            if len(after_content) > 512:
                after_content = after_content[:509] + "…"
            embed = discord.Embed(
                title="✏️ Message modifié",
                color=0xf39c12,
                timestamp=datetime.datetime.utcnow()
            )
            embed.add_field(name="Auteur", value=f"{after.author.mention} (`{after.author}`)", inline=True)
            embed.add_field(name="Salon", value=after.channel.mention, inline=True)
            embed.add_field(name="Avant", value=before_content, inline=False)
            embed.add_field(name="Après", value=after_content, inline=False)
            embed.add_field(name="Lien", value=f"[Voir le message]({after.jump_url})", inline=False)
            await log_ch.send(embed=embed)
        except Exception as e:
            logger.error(f"on_message_edit log error: {e}")

    async def on_invite_create(self, invite):
        if not invite.guild:
            return
        try:
            log_ch = await get_general_log_channel(invite.guild)
            if not log_ch:
                return
            embed = discord.Embed(
                title="🔗 Invitation créée",
                color=0x3498db,
                timestamp=datetime.datetime.utcnow()
            )
            inviter = invite.inviter
            embed.add_field(name="Créateur", value=f"{inviter.mention} (`{inviter}`)" if inviter else "Inconnu", inline=True)
            embed.add_field(name="Code", value=f"`{invite.code}`", inline=True)
            embed.add_field(name="Salon", value=invite.channel.mention if invite.channel else "Inconnu", inline=True)
            uses_max = str(invite.max_uses) if invite.max_uses else "∞"
            expires = f"<t:{int(invite.expires_at.timestamp())}:R>" if invite.expires_at else "Jamais"
            embed.add_field(name="Utilisations max", value=uses_max, inline=True)
            embed.add_field(name="Expire", value=expires, inline=True)
            await log_ch.send(embed=embed)
        except Exception as e:
            logger.error(f"on_invite_create log error: {e}")

    async def on_guild_stickers_update(self, guild, before, after):
        try:
            log_ch = await get_general_log_channel(guild)
            if not log_ch:
                return
            added = set(s.id for s in after) - set(s.id for s in before)
            removed = set(s.id for s in before) - set(s.id for s in after)
            if not added and not removed:
                return
            embed = discord.Embed(title="🎨 Stickers mis à jour", color=0x9b59b6, timestamp=datetime.datetime.utcnow())
            if added:
                names = [s.name for s in after if s.id in added]
                embed.add_field(name="Ajoutés", value=", ".join(names), inline=False)
            if removed:
                names = [s.name for s in before if s.id in removed]
                embed.add_field(name="Supprimés", value=", ".join(names), inline=False)
            await log_ch.send(embed=embed)
        except Exception as e:
            logger.error(f"on_guild_stickers_update log error: {e}")


bot = NexusBot()




async def is_bot_owner_or_server_owner(guild, user_id):
    if BOT_OWNER_ID and user_id == BOT_OWNER_ID:
        return True
    if guild.owner_id == user_id:
        return True
    return False


async def is_owner_or_ownerlist(guild, user_id):
    if await is_bot_owner_or_server_owner(guild, user_id):
        return True
    if pool:
        row = await pool.fetchrow(
            "SELECT id FROM ownerlist WHERE guild_id = $1 AND user_id = $2",
            str(guild.id), str(user_id)
        )
        return row is not None
    return False


async def can_use_bot(guild, user_id):
    """Peut utiliser les commandes du bot : Bot Owner, Server Owner ou Ownerlist uniquement."""
    return await is_owner_or_ownerlist(guild, user_id)


async def is_whitelisted(guild, user_id):
    if await is_owner_or_ownerlist(guild, user_id):
        return True
    if pool:
        row = await pool.fetchrow(
            "SELECT id FROM whitelist WHERE guild_id = $1 AND user_id = $2",
            str(guild.id), str(user_id)
        )
        return row is not None
    return False


async def should_bypass_protection(guild, user_id, protection_key):
    if user_id == BOT_OWNER_ID:
        return True
    if bot.user and user_id == bot.user.id:
        return True
    if await is_owner_or_ownerlist(guild, user_id):
        return True
    prot = await get_protection(guild.id, protection_key)
    if prot and prot.get('whitelist_bypass', False):
        if await is_whitelisted(guild, user_id):
            return True
    return False


async def apply_punishment(guild, user, protection_key):
    if user.id == BOT_OWNER_ID:
        return
    if bot.user and user.id == bot.user.id:
        return
    prot = await get_protection(guild.id, protection_key)
    punishment = prot['punishment'] if prot and prot['punishment'] else 'ban'
    member = guild.get_member(user.id)
    if not member:
        return

    try:
        if punishment == 'ban':
            await guild.ban(member, reason=f"Shield Protection: {protection_key}")
        elif punishment == 'kick':
            await guild.kick(member, reason=f"Shield Protection: {protection_key}")
        elif punishment == 'derank':
            roles_to_remove = [r for r in member.roles if r != guild.default_role and r < guild.me.top_role]
            if roles_to_remove:
                await member.remove_roles(*roles_to_remove, reason=f"Shield Protection: {protection_key}")
        elif punishment == 'timeout':
            from datetime import timedelta
            duration_str = prot['timeout_duration'] if prot and prot.get('timeout_duration') else '1h'
            duration_map = {
                '60s': timedelta(seconds=60),
                '5m': timedelta(minutes=5),
                '10m': timedelta(minutes=10),
                '1h': timedelta(hours=1),
                '1d': timedelta(days=1),
                '1w': timedelta(weeks=1),
            }
            duration = duration_map.get(duration_str, timedelta(hours=1))
            await member.timeout(duration, reason=f"Shield Protection: {protection_key}")
    except Exception as e:
        logger.error(f"Failed to apply punishment {punishment} to {user}: {e}")
        await log_to_db('error', f'Failed to apply punishment {punishment} to {user}: {e}')


GENERAL_LOG_CHANNEL = "logs・général"

AUDIT_LOG_CHANNELS = {k: GENERAL_LOG_CHANNEL for k in [
    "role", "channel", "member", "voice", "message", "server", "salon_access"
]}


async def get_general_log_channel(guild):
    """Retourne le salon logs・général dans la catégorie RShield - Logs, ou None."""
    try:
        cat = discord.utils.get(guild.categories, name="RShield - Logs")
        if cat:
            ch = discord.utils.get(cat.text_channels, name=GENERAL_LOG_CHANNEL)
            if ch:
                return ch
        # Fallback: chercher n'importe quel log_channel_id configuré
        prot = await get_protection(str(guild.id), "anti_ban")
        if prot and prot.get('log_channel_id'):
            ch = guild.get_channel(int(prot['log_channel_id']))
            if ch:
                return ch
    except Exception:
        pass
    return None


async def send_audit_log(guild, category_key, title, description, color=0x2b2d31, thumbnail_url=None):
    try:
        log_ch = await get_general_log_channel(guild)
        if not log_ch:
            return
        embed = discord.Embed(title=title, description=description, color=color)
        embed.timestamp = datetime.datetime.utcnow()
        if thumbnail_url:
            embed.set_thumbnail(url=thumbnail_url)
        await log_ch.send(embed=embed)
    except Exception as e:
        logger.error(f"Failed to send audit log: {e}")


async def send_protection_log(guild, protection_key, user, detail_text, role=None, target=None):
    try:
        channel = await get_general_log_channel(guild)
        if not channel:
            return
        prot = await get_protection(guild.id, protection_key)

        mod = next((m for m in PROTECTION_MODULES if m['key'] == protection_key), None)

        punishment_str = "Bannissement."
        if prot and prot.get('punishment'):
            for p in PUNISHMENT_OPTIONS:
                if p['value'] == prot['punishment']:
                    punishment_str = f"{p['label']}."
                    break

        perm_str = "Activé." if (prot and prot.get('enabled')) else "Désactivé."

        mention_lines = [f"**Auteur:** <@{user.id}>"]
        if target:
            mention_lines.append(f"**Cible:** <@{target.id}>")

        code_lines = [f"+ {detail_text}", f"Utilisateur: {user} (ID: {user.id})"]
        if target:
            code_lines.append(f"Cible: {target} (ID: {target.id})")
        if role:
            code_lines.append(f"Rôle: {role.name} (ID: {role.id})")
        code_lines.append(f"Punition: {punishment_str}")
        code_lines.append(f"Permission: {perm_str}")

        description = "\n".join(mention_lines) + "\n```diff\n" + "\n".join(code_lines) + "\n```"

        embed = discord.Embed(description=description, color=0x2b2d31)
        embed.timestamp = datetime.datetime.utcnow()
        await channel.send(embed=embed)
    except Exception as e:
        logger.error(f"Failed to send protection log: {e}")
        await log_to_db('error', f'Failed to send protection log: {e}')


async def build_ownerlist_embed(guild_id):
    if pool:
        rows = await pool.fetch(
            "SELECT user_id FROM ownerlist WHERE guild_id = $1",
            str(guild_id)
        )
        if not rows:
            embed = discord.Embed(
                description="La ownerlist est actuellement vide.\nUtilisez les boutons ci-dessous pour gérer la liste.",
                color=0x2b2d31
            )
        else:
            lines = [f"<@{row['user_id']}>" for row in rows]
            embed = discord.Embed(
                description="**Membres dans la ownerlist :**\n" + "\n".join(lines),
                color=0x2b2d31
            )
    else:
        embed = discord.Embed(description="Erreur de connexion à la base de données.", color=0x2b2d31)
    embed.set_author(name="Ownerlist")
    return embed


class GuildApprovalView(discord.ui.View):
    def __init__(self, guild_id, guild_name):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.guild_name = guild_name

    @discord.ui.button(label="Accepter", style=discord.ButtonStyle.green, custom_id="guild_accept")
    async def accept_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != BOT_OWNER_ID:
            return
        if pool:
            await pool.execute("DELETE FROM pending_guilds WHERE guild_id = $1", str(self.guild_id))
        embed = discord.Embed(
            description=f"✅ Le serveur **{self.guild_name}** (`{self.guild_id}`) a été accepté.",
            color=0x2b2d31
        )
        self.accept_button.disabled = True
        self.reject_button.disabled = True
        await interaction.response.edit_message(embed=embed, view=self)
        await log_to_db('info', f'Guild {self.guild_name} ({self.guild_id}) accepted by bot owner')

    @discord.ui.button(label="Refuser", style=discord.ButtonStyle.red, custom_id="guild_reject")
    async def reject_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != BOT_OWNER_ID:
            return
        if pool:
            await pool.execute("DELETE FROM pending_guilds WHERE guild_id = $1", str(self.guild_id))
            await pool.execute(
                "INSERT INTO guild_blacklist (guild_id, guild_name) VALUES ($1, $2) ON CONFLICT (guild_id) DO NOTHING",
                str(self.guild_id), self.guild_name
            )
        guild = bot.get_guild(self.guild_id)
        if guild:
            try:
                await guild.leave()
            except Exception:
                pass
        embed = discord.Embed(
            description=f"❌ Le serveur **{self.guild_name}** (`{self.guild_id}`) a été refusé et blacklisté.",
            color=0x2b2d31
        )
        self.accept_button.disabled = True
        self.reject_button.disabled = True
        await interaction.response.edit_message(embed=embed, view=self)
        await log_to_db('info', f'Guild {self.guild_name} ({self.guild_id}) rejected and blacklisted by bot owner')


class OwnerlistView(discord.ui.View):
    def __init__(self, guild_id, owner_id):
        super().__init__(timeout=120)
        self.guild_id = guild_id
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction):
        if not await is_bot_owner_or_server_owner(interaction.guild, interaction.user.id):
            await interaction.response.send_message("Seul le créateur du serveur peut utiliser ce menu.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Ajouter", style=discord.ButtonStyle.green, custom_id="ownerlist_add")
    async def add_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = OwnerlistAddModal(self.guild_id, self.owner_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Retirer", style=discord.ButtonStyle.red, custom_id="ownerlist_remove")
    async def remove_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not pool:
            await interaction.response.send_message("Erreur de connexion.", ephemeral=True)
            return
        rows = await pool.fetch(
            "SELECT user_id FROM ownerlist WHERE guild_id = $1",
            str(self.guild_id)
        )
        if not rows:
            await interaction.response.send_message("La ownerlist est vide, rien à retirer.", ephemeral=True)
            return
        view = OwnerlistRemoveSelect(self.guild_id, self.owner_id, rows, interaction.guild)
        await interaction.response.send_message("Sélectionnez le membre à retirer :", view=view, ephemeral=True)

    @discord.ui.button(label="Liste", style=discord.ButtonStyle.blurple, custom_id="ownerlist_list")
    async def list_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await build_ownerlist_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self)


class OwnerlistAddModal(discord.ui.Modal, title="Ajouter à la ownerlist"):
    user_id_input = discord.ui.TextInput(
        label="ID du membre",
        placeholder="Ex: 123456789012345678",
        required=True,
        min_length=17,
        max_length=20
    )

    def __init__(self, guild_id, owner_id):
        super().__init__()
        self.guild_id = guild_id
        self.owner_id = owner_id

    async def on_submit(self, interaction: discord.Interaction):
        user_id_str = self.user_id_input.value.strip()
        try:
            uid = int(user_id_str)
        except ValueError:
            await interaction.response.send_message("ID invalide. Entrez un ID numérique.", ephemeral=True)
            return

        if uid == self.owner_id:
            await interaction.response.send_message("Le créateur du serveur est déjà protégé.", ephemeral=True)
            return

        member = interaction.guild.get_member(uid)
        if not member:
            try:
                member = await interaction.guild.fetch_member(uid)
            except discord.NotFound:
                await interaction.response.send_message("Membre introuvable sur ce serveur.", ephemeral=True)
                return

        if pool:
            existing = await pool.fetchrow(
                "SELECT id FROM ownerlist WHERE guild_id = $1 AND user_id = $2",
                str(self.guild_id), str(uid)
            )
            if existing:
                await interaction.response.send_message(f"{member.mention} est déjà dans la ownerlist.", ephemeral=True)
                return

            await pool.execute(
                "INSERT INTO ownerlist (guild_id, user_id) VALUES ($1, $2)",
                str(self.guild_id), str(uid)
            )
            await log_to_db('info', f'{interaction.user} added {member} to ownerlist in {interaction.guild.name}')

            embed = await build_ownerlist_embed(self.guild_id)
            view = OwnerlistView(self.guild_id, self.owner_id)
            await interaction.response.edit_message(embed=embed, view=view)


class OwnerlistRemoveSelect(discord.ui.View):
    def __init__(self, guild_id, owner_id, rows, guild):
        super().__init__(timeout=60)
        self.guild_id = guild_id
        self.owner_id = owner_id
        options = []
        for row in rows[:25]:
            uid = row['user_id']
            member = guild.get_member(int(uid))
            label = str(member) if member else f"ID: {uid}"
            options.append(discord.SelectOption(label=label, value=uid))
        self.select = discord.ui.Select(placeholder="Choisir un membre à retirer...", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Seul le créateur du serveur peut utiliser ce menu.", ephemeral=True)
            return False
        return True

    async def select_callback(self, interaction: discord.Interaction):
        uid = self.select.values[0]
        if pool:
            await pool.execute(
                "DELETE FROM ownerlist WHERE guild_id = $1 AND user_id = $2",
                str(self.guild_id), uid
            )
            await log_to_db('info', f'{interaction.user} removed <@{uid}> from ownerlist in {interaction.guild.name}')

            embed = discord.Embed(
                description=f"<@{uid}> a été retiré de la ownerlist.",
                color=0x2b2d31
            )
            await interaction.response.edit_message(embed=embed, view=None)


@bot.tree.command(name="ownerlist", description="Gérer la liste des créateurs du serveur.")
@app_commands.default_permissions(administrator=True)
async def ownerlist_command(interaction: discord.Interaction):
    try:
        if not await check_license(interaction):
            return
        if not await is_bot_owner_or_server_owner(interaction.guild, interaction.user.id):
            await interaction.response.send_message("Seul le propriétaire du bot ou le créateur du serveur peut utiliser cette commande.", ephemeral=True)
            return

        embed = await build_ownerlist_embed(interaction.guild.id)
        view = OwnerlistView(interaction.guild.id, interaction.user.id)
        await interaction.response.send_message(embed=embed, view=view)
        await log_to_db('info', f'/ownerlist used by {interaction.user} in #{interaction.channel}')
    except Exception as e:
        logger.error(f"Error in /ownerlist command: {traceback.format_exc()}")
        try:
            await log_to_db('error', f'Error in /ownerlist: {e}')
        except Exception:
            pass
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


async def build_whitelist_embed(guild_id):
    if pool:
        rows = await pool.fetch(
            "SELECT user_id FROM whitelist WHERE guild_id = $1",
            str(guild_id)
        )
        if not rows:
            embed = discord.Embed(
                description="La whitelist est actuellement vide.\nUtilisez les boutons ci-dessous pour gérer la liste.",
                color=0x2b2d31
            )
        else:
            lines = [f"<@{row['user_id']}>" for row in rows]
            embed = discord.Embed(
                description="**Membres dans la whitelist :**\n" + "\n".join(lines),
                color=0x2b2d31
            )
    else:
        embed = discord.Embed(description="Erreur de connexion à la base de données.", color=0x2b2d31)
    embed.set_author(name="Whitelist")
    return embed


class WhitelistView(discord.ui.View):
    def __init__(self, guild_id, owner_id):
        super().__init__(timeout=120)
        self.guild_id = guild_id
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction):
        is_allowed = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
        if not is_allowed:
            await interaction.response.send_message("Seul le créateur ou un membre de la ownerlist peut utiliser ce menu.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Ajouter", style=discord.ButtonStyle.green, custom_id="whitelist_add")
    async def add_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = WhitelistAddModal(self.guild_id, self.owner_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Retirer", style=discord.ButtonStyle.red, custom_id="whitelist_remove")
    async def remove_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not pool:
            await interaction.response.send_message("Erreur de connexion.", ephemeral=True)
            return
        rows = await pool.fetch(
            "SELECT user_id FROM whitelist WHERE guild_id = $1",
            str(self.guild_id)
        )
        if not rows:
            await interaction.response.send_message("La whitelist est vide, rien à retirer.", ephemeral=True)
            return
        view = WhitelistRemoveSelect(self.guild_id, self.owner_id, rows, interaction.guild)
        await interaction.response.send_message("Sélectionnez le membre à retirer :", view=view, ephemeral=True)

    @discord.ui.button(label="Liste", style=discord.ButtonStyle.blurple, custom_id="whitelist_list")
    async def list_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await build_whitelist_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self)


class WhitelistAddModal(discord.ui.Modal, title="Ajouter à la whitelist"):
    user_id_input = discord.ui.TextInput(
        label="ID du membre",
        placeholder="Ex: 123456789012345678",
        required=True,
        min_length=17,
        max_length=20
    )

    def __init__(self, guild_id, owner_id):
        super().__init__()
        self.guild_id = guild_id
        self.owner_id = owner_id

    async def on_submit(self, interaction: discord.Interaction):
        user_id_str = self.user_id_input.value.strip()
        try:
            uid = int(user_id_str)
        except ValueError:
            await interaction.response.send_message("ID invalide. Entrez un ID numérique.", ephemeral=True)
            return

        member = interaction.guild.get_member(uid)
        if not member:
            try:
                member = await interaction.guild.fetch_member(uid)
            except discord.NotFound:
                await interaction.response.send_message("Membre introuvable sur ce serveur.", ephemeral=True)
                return

        if pool:
            existing = await pool.fetchrow(
                "SELECT id FROM whitelist WHERE guild_id = $1 AND user_id = $2",
                str(self.guild_id), str(uid)
            )
            if existing:
                await interaction.response.send_message(f"{member.mention} est déjà dans la whitelist.", ephemeral=True)
                return

            await pool.execute(
                "INSERT INTO whitelist (guild_id, user_id) VALUES ($1, $2)",
                str(self.guild_id), str(uid)
            )
            await log_to_db('info', f'{interaction.user} added {member} to whitelist in {interaction.guild.name}')

            embed = await build_whitelist_embed(self.guild_id)
            view = WhitelistView(self.guild_id, self.owner_id)
            await interaction.response.edit_message(embed=embed, view=view)


class WhitelistRemoveSelect(discord.ui.View):
    def __init__(self, guild_id, owner_id, rows, guild):
        super().__init__(timeout=60)
        self.guild_id = guild_id
        self.owner_id = owner_id
        options = []
        for row in rows[:25]:
            uid = row['user_id']
            member = guild.get_member(int(uid))
            label = str(member) if member else f"ID: {uid}"
            options.append(discord.SelectOption(label=label, value=uid))
        self.select = discord.ui.Select(placeholder="Choisir un membre à retirer...", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def interaction_check(self, interaction: discord.Interaction):
        is_allowed = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
        if not is_allowed:
            await interaction.response.send_message("Seul le créateur ou un membre de la ownerlist peut utiliser ce menu.", ephemeral=True)
            return False
        return True

    async def select_callback(self, interaction: discord.Interaction):
        uid = self.select.values[0]
        if pool:
            await pool.execute(
                "DELETE FROM whitelist WHERE guild_id = $1 AND user_id = $2",
                str(self.guild_id), uid
            )
            await log_to_db('info', f'{interaction.user} removed <@{uid}> from whitelist in {interaction.guild.name}')

            embed = discord.Embed(
                description=f"<@{uid}> a été retiré de la whitelist.",
                color=0x2b2d31
            )
            await interaction.response.edit_message(embed=embed, view=None)


@bot.tree.command(name="whitelist", description="Gérer la liste blanche du serveur.")
@app_commands.default_permissions(administrator=True)
async def whitelist_command(interaction: discord.Interaction):
    try:
        if not await check_license(interaction):
            return
        is_allowed = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
        if not is_allowed:
            await interaction.response.send_message("Seul le créateur ou un membre de la ownerlist peut utiliser cette commande.", ephemeral=True)
            return

        embed = await build_whitelist_embed(interaction.guild.id)
        view = WhitelistView(interaction.guild.id, interaction.user.id)
        await interaction.response.send_message(embed=embed, view=view)
        await log_to_db('info', f'/whitelist used by {interaction.user} in #{interaction.channel}')
    except Exception as e:
        logger.error(f"Error in /whitelist command: {traceback.format_exc()}")
        try:
            await log_to_db('error', f'Error in /whitelist: {e}')
        except Exception:
            pass
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


async def is_blacklisted(user_id):
    if not pool:
        return False
    row = await pool.fetchrow(
        "SELECT id FROM blacklist WHERE user_id = $1",
        str(user_id)
    )
    return row is not None


async def build_blacklist_embed():
    embed = discord.Embed(
        title="Blacklist",
        description="Les utilisateurs blacklistés sont bannis automatiquement de tous les serveurs où le bot est présent.",
        color=0x2b2d31
    )
    if pool:
        rows = await pool.fetch("SELECT user_id, reason FROM blacklist ORDER BY added_at DESC")
        if rows:
            lines = []
            for i, row in enumerate(rows, 1):
                reason = row['reason'] or "Aucune raison"
                lines.append(f"`{i}.` <@{row['user_id']}> — {reason}")
            embed.add_field(name="Utilisateurs blacklistés", value="\n".join(lines[:20]), inline=False)
            embed.set_footer(text=f"{len(rows)} utilisateur(s) blacklisté(s)")
        else:
            embed.add_field(name="Liste vide", value="Aucun utilisateur blacklisté.", inline=False)
    return embed


class BlacklistView(discord.ui.View):
    def __init__(self, owner_id):
        super().__init__(timeout=120)
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction):
        if not await can_use_bot(interaction.guild, interaction.user.id):
            await interaction.response.send_message("Vous ne pouvez pas utiliser le bot.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Ajouter", style=discord.ButtonStyle.green, custom_id="blacklist_add")
    async def add_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = BlacklistAddModal(self.owner_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Retirer", style=discord.ButtonStyle.red, custom_id="blacklist_remove")
    async def remove_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not pool:
            await interaction.response.send_message("Erreur de connexion.", ephemeral=True)
            return
        rows = await pool.fetch("SELECT user_id, added_by FROM blacklist")
        if not rows:
            await interaction.response.send_message("La blacklist est vide, rien à retirer.", ephemeral=True)
            return
        view = BlacklistRemoveSelect(self.owner_id, rows, interaction.guild)
        await interaction.response.send_message("Sélectionnez l'utilisateur à retirer :", view=view, ephemeral=True)

    @discord.ui.button(label="Liste", style=discord.ButtonStyle.blurple, custom_id="blacklist_list")
    async def list_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await build_blacklist_embed()
        await interaction.response.edit_message(embed=embed, view=self)


class BlacklistAddModal(discord.ui.Modal, title="Ajouter à la blacklist"):
    user_id_input = discord.ui.TextInput(
        label="ID de l'utilisateur",
        placeholder="Ex: 123456789012345678",
        required=True,
        min_length=17,
        max_length=20
    )
    reason_input = discord.ui.TextInput(
        label="Raison",
        placeholder="Raison du blacklist (optionnel)",
        required=False,
        max_length=200
    )

    def __init__(self, owner_id):
        super().__init__()
        self.owner_id = owner_id

    async def on_submit(self, interaction: discord.Interaction):
        user_id_str = self.user_id_input.value.strip()
        reason = self.reason_input.value.strip() or None
        try:
            uid = int(user_id_str)
        except ValueError:
            await interaction.response.send_message("ID invalide. Entrez un ID numérique.", ephemeral=True)
            return

        if uid == interaction.user.id:
            await interaction.response.send_message("Vous ne pouvez pas vous blacklister vous-même.", ephemeral=True)
            return

        if uid == bot.user.id:
            await interaction.response.send_message("Vous ne pouvez pas blacklister le bot.", ephemeral=True)
            return

        if uid == BOT_OWNER_ID:
            await interaction.response.send_message("Vous ne pouvez pas blacklister le propriétaire du bot.", ephemeral=True)
            return

        if pool:
            existing = await pool.fetchrow(
                "SELECT id FROM blacklist WHERE user_id = $1",
                str(uid)
            )
            if existing:
                await interaction.response.send_message(f"<@{uid}> est déjà dans la blacklist.", ephemeral=True)
                return

            await pool.execute(
                "INSERT INTO blacklist (user_id, reason, added_by) VALUES ($1, $2, $3)",
                str(uid), reason, str(interaction.user.id)
            )
            await log_to_db('info', f'{interaction.user} added <@{uid}> to blacklist')

            banned_servers = []
            for guild in bot.guilds:
                try:
                    member = guild.get_member(uid)
                    if not member:
                        try:
                            member = await guild.fetch_member(uid)
                        except discord.NotFound:
                            continue
                    await guild.ban(discord.Object(id=uid), reason=f"Shield Blacklist: ajouté par {interaction.user} — {reason or 'Aucune raison'}")
                    banned_servers.append(guild.name)
                except Exception as e:
                    logger.error(f"Failed to ban {uid} from {guild.name}: {e}")

            embed = discord.Embed(
                description=f"<@{uid}> a bien été banni de **{len(banned_servers)}** serveur(s) avec succès.",
                color=0x2b2d31
            )
            view = BlacklistView(self.owner_id)
            await interaction.response.edit_message(embed=embed, view=view)


class BlacklistRemoveSelect(discord.ui.View):
    def __init__(self, owner_id, rows, guild):
        super().__init__(timeout=60)
        self.owner_id = owner_id
        self.added_by_map = {row['user_id']: row['added_by'] for row in rows}
        options = []
        for row in rows[:25]:
            uid = row['user_id']
            member = guild.get_member(int(uid)) if guild else None
            label = str(member) if member else f"ID: {uid}"
            options.append(discord.SelectOption(label=label, value=uid))
        self.select = discord.ui.Select(placeholder="Choisir un utilisateur à retirer...", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def interaction_check(self, interaction: discord.Interaction):
        if not await can_use_bot(interaction.guild, interaction.user.id):
            await interaction.response.send_message("Vous ne pouvez pas utiliser le bot.", ephemeral=True)
            return False
        return True

    async def select_callback(self, interaction: discord.Interaction):
        uid = self.select.values[0]
        added_by = self.added_by_map.get(uid)
        is_bot_owner = interaction.user.id == BOT_OWNER_ID
        is_guild_owner = interaction.guild and interaction.guild.owner_id == interaction.user.id
        is_adder = added_by == str(interaction.user.id)
        if not (is_bot_owner or is_guild_owner or is_adder):
            await interaction.response.send_message(
                "❌ Seul la personne qui a blacklisté cet utilisateur, le propriétaire du bot ou le créateur du serveur peut l'unblacklist.",
                ephemeral=True
            )
            return
        if pool:
            await pool.execute(
                "DELETE FROM blacklist WHERE user_id = $1",
                uid
            )
            await log_to_db('info', f'{interaction.user} removed <@{uid}> from blacklist')

            for guild in bot.guilds:
                try:
                    await guild.unban(discord.Object(id=int(uid)), reason="Shield Blacklist: retiré de la blacklist")
                except Exception:
                    pass

            embed = discord.Embed(
                description=f"<@{uid}> a été retiré de la blacklist et débanni de tous les serveurs.",
                color=0x2b2d31
            )
            await interaction.response.edit_message(embed=embed, view=None)


@bot.tree.command(name="blacklist", description="Gérer la blacklist globale du bot.")
@app_commands.default_permissions(administrator=True)
async def blacklist_command(interaction: discord.Interaction):
    try:
        if not await check_license(interaction):
            return
        if not await can_use_bot(interaction.guild, interaction.user.id):
            await interaction.response.send_message("Vous ne pouvez pas utiliser le bot.", ephemeral=True)
            return

        embed = await build_blacklist_embed()
        view = BlacklistView(interaction.user.id)
        await interaction.response.send_message(embed=embed, view=view)
        await log_to_db('info', f'/blacklist used by {interaction.user} in #{interaction.channel}')
    except Exception as e:
        logger.error(f"Error in /blacklist command: {traceback.format_exc()}")
        try:
            await log_to_db('error', f'Error in /blacklist: {e}')
        except Exception:
            pass
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


@bot.tree.command(name="unblacklist", description="Retirer un utilisateur de la blacklist.")
@app_commands.default_permissions(administrator=True)
async def unblacklist_command(interaction: discord.Interaction):
    try:
        if not await check_license(interaction):
            return
        if not await can_use_bot(interaction.guild, interaction.user.id):
            await interaction.response.send_message("Vous ne pouvez pas utiliser le bot.", ephemeral=True)
            return

        if not pool:
            await interaction.response.send_message("Erreur de connexion à la base de données.", ephemeral=True)
            return

        rows = await pool.fetch("SELECT user_id, added_by FROM blacklist")
        if not rows:
            await interaction.response.send_message("La blacklist est vide, rien à retirer.", ephemeral=True)
            return

        view = UnblacklistSelect(interaction.user.id, rows, interaction.guild)
        await interaction.response.send_message("Sélectionnez l'utilisateur à retirer de la blacklist :", view=view, ephemeral=True)
        await log_to_db('info', f'/unblacklist used by {interaction.user} in #{interaction.channel}')
    except Exception as e:
        logger.error(f"Error in /unblacklist command: {traceback.format_exc()}")
        try:
            await log_to_db('error', f'Error in /unblacklist: {e}')
        except Exception:
            pass
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


class UnblacklistSelect(discord.ui.View):
    def __init__(self, owner_id, rows, guild):
        super().__init__(timeout=60)
        self.owner_id = owner_id
        self.added_by_map = {row['user_id']: row['added_by'] for row in rows}
        options = []
        for row in rows[:25]:
            uid = row['user_id']
            member = guild.get_member(int(uid)) if guild else None
            label = str(member) if member else f"ID: {uid}"
            options.append(discord.SelectOption(label=label, value=uid))
        self.select = discord.ui.Select(placeholder="Choisir un utilisateur à retirer...", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def interaction_check(self, interaction: discord.Interaction):
        if not await can_use_bot(interaction.guild, interaction.user.id):
            await interaction.response.send_message("Vous ne pouvez pas utiliser le bot.", ephemeral=True)
            return False
        return True

    async def select_callback(self, interaction: discord.Interaction):
        uid = self.select.values[0]
        added_by = self.added_by_map.get(uid)
        is_bot_owner = interaction.user.id == BOT_OWNER_ID
        is_guild_owner = interaction.guild and interaction.guild.owner_id == interaction.user.id
        is_adder = added_by == str(interaction.user.id)
        if not (is_bot_owner or is_guild_owner or is_adder):
            await interaction.response.send_message(
                "❌ Seul la personne qui a blacklisté cet utilisateur, le propriétaire du bot ou le créateur du serveur peut l'unblacklist.",
                ephemeral=True
            )
            return
        if pool:
            await pool.execute(
                "DELETE FROM blacklist WHERE user_id = $1",
                uid
            )
            await log_to_db('info', f'{interaction.user} removed <@{uid}> from blacklist via /unblacklist')

            for guild in bot.guilds:
                try:
                    await guild.unban(discord.Object(id=int(uid)), reason="Shield Blacklist: retiré de la blacklist")
                except Exception:
                    pass

            embed = discord.Embed(
                description=f"<@{uid}> a été retiré de la blacklist et débanni de tous les serveurs.",
                color=0x2b2d31
            )
            await interaction.response.edit_message(content=None, embed=embed, view=None)


DANGEROUS_PERMISSIONS = [
    'administrator',
    'ban_members',
    'kick_members',
    'manage_guild',
    'manage_roles',
    'manage_channels',
    'mention_everyone',
    'manage_webhooks',
    'manage_messages',
    'manage_nicknames',
    'manage_emojis_and_stickers',
    'moderate_members',
    'mute_members',
    'deafen_members',
    'move_members',
]


@bot.tree.command(name="secure", description="Désactive les permissions et clear les whitelists.")
@app_commands.default_permissions(administrator=True)
async def secure_command(interaction: discord.Interaction):
    try:
        if not await check_license(interaction):
            return
        if not await is_bot_owner_or_server_owner(interaction.guild, interaction.user.id):
            await interaction.response.send_message("Seul le cr\u00e9ateur du serveur peut utiliser cette commande.", ephemeral=True)
            return

        embed = discord.Embed(
            title="\u26a0\ufe0f Mode Secure",
            description=(
                "**Attention !** Cette action est irr\u00e9versible et va :\n\n"
                "\u2022 Supprimer **tous** les membres de la whitelist\n"
                "\u2022 Retirer **toutes** les permissions administratives de tous les r\u00f4les\n"
                "\u2022 Retirer **toutes** les permissions dangereuses de tous les r\u00f4les\n\n"
                "\u00cates-vous s\u00fbr de vouloir activer le mode secure ?"
            ),
            color=0xff0000
        )
        view = SecureConfirmView(interaction.user.id)
        await interaction.response.send_message(embed=embed, view=view)
        await log_to_db('info', f'/secure used by {interaction.user} in #{interaction.channel}')
    except Exception as e:
        logger.error(f"Error in /secure command: {traceback.format_exc()}")
        try:
            await log_to_db('error', f'Error in /secure: {e}')
        except Exception:
            pass
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


class SecureConfirmView(discord.ui.View):
    def __init__(self, owner_id):
        super().__init__(timeout=30)
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Seul le cr\u00e9ateur du serveur peut utiliser ce menu.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirmer", style=discord.ButtonStyle.danger, custom_id="secure_confirm")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        guild = interaction.guild
        results = []

        if pool:
            await pool.execute(
                "DELETE FROM ownerlist WHERE guild_id = $1",
                str(guild.id)
            )
            await pool.execute(
                "DELETE FROM whitelist WHERE guild_id = $1",
                str(guild.id)
            )
            results.append("Whitelist/Ownerlist vid\u00e9e")
            await log_to_db('warn', f'Secure: ownerlist and whitelist cleared in {guild.name}')

        roles_modified = 0
        bot_top_role = guild.me.top_role
        for role in guild.roles:
            if role.is_default() or role.managed or role >= bot_top_role:
                continue
            perms = role.permissions
            new_perms = discord.Permissions(perms.value)
            changed = False
            for perm_name in DANGEROUS_PERMISSIONS:
                if getattr(new_perms, perm_name, False):
                    setattr(new_perms, perm_name, False)
                    changed = True
            if changed:
                try:
                    await role.edit(permissions=new_perms, reason="Shield Secure: retrait des permissions dangereuses")
                    roles_modified += 1
                except Exception as e:
                    logger.error(f"Secure: failed to edit role {role.name}: {e}")

        results.append(f"{roles_modified} r\u00f4le(s) modifi\u00e9(s)")
        await log_to_db('warn', f'Secure: {roles_modified} roles stripped of dangerous permissions in {guild.name}')

        embed = discord.Embed(
            title="\u2705 Mode Secure Activ\u00e9",
            description=(
                "Le mode secure a \u00e9t\u00e9 activ\u00e9 avec succ\u00e8s.\n\n"
                f"\u2022 {results[0]}\n"
                f"\u2022 {results[1]}\n"
            ),
            color=0x00ff00
        )
        await interaction.followup.edit_message(interaction.message.id, embed=embed, view=None)

    @discord.ui.button(label="Annuler", style=discord.ButtonStyle.secondary, custom_id="secure_cancel")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            description="Mode secure annul\u00e9.",
            color=0x2b2d31
        )
        await interaction.response.edit_message(embed=embed, view=None)


PROTECTION_MODULES = [
    {"key": "anti_bot_add", "label": "Ajout de bot"},
    {"key": "anti_role_add", "label": "Ajout de rôle"},
    {"key": "anti_ban", "label": "Bannissement d'utilisateur"},
    {"key": "anti_thread_create", "label": "Création de fil"},
    {"key": "anti_role_create", "label": "Création de rôle"},
    {"key": "anti_channel_create", "label": "Création de salon"},
    {"key": "anti_webhook_create", "label": "Création de webhook"},
    {"key": "anti_disconnect", "label": "Déconnexion d'utilisateur"},
    {"key": "anti_member_move", "label": "Déplacement d'un utilisateur"},
    {"key": "anti_role_remove", "label": "Enlever un rôle"},
    {"key": "anti_timeout", "label": "Exclure temporairement"},
    {"key": "anti_kick", "label": "Expulsion d'utilisateur"},
    {"key": "anti_link", "label": "Message contenant des liens"},
    {"key": "anti_spam", "label": "Message contenant du spam"},
    {"key": "anti_toxicity", "label": "Message contenant un taux de toxicité"},
    {"key": "anti_role_update", "label": "Mise à jour de rôle"},
    {"key": "anti_channel_update", "label": "Mise à jour de salon"},
    {"key": "anti_server_update", "label": "Mise à jour de serveur"},
    {"key": "anti_role_position", "label": "Mise a jour massive de la position des rôles"},
    {"key": "anti_mute", "label": "Mise en muet d'un utilisateur"},
    {"key": "anti_deafen", "label": "Mise en sourdine d'un utilisateur"},
    {"key": "anti_embed_delete", "label": "Suppression de message contenant une embed"},
    {"key": "anti_role_delete", "label": "Suppression de rôle"},
    {"key": "anti_channel_delete", "label": "Suppression de salon"},
    {"key": "anti_unban", "label": "Débannissement d'utilisateur"},
    {"key": "anti_gif_spam", "label": "Spam de GIF"},
    {"key": "anti_mention_spam", "label": "Spam de mentions"},
]

PUNISHMENT_OPTIONS = [
    {"label": "Bannissement", "value": "ban"},
    {"label": "Expulsion", "value": "kick"},
    {"label": "Retirer les rôles", "value": "derank"},
    {"label": "Exclure temporairement", "value": "timeout"},
]

TIMEOUT_DURATION_OPTIONS = [
    {"label": "60 secondes", "value": "60s"},
    {"label": "5 minutes", "value": "5m"},
    {"label": "10 minutes", "value": "10m"},
    {"label": "1 heure", "value": "1h"},
    {"label": "1 jour", "value": "1d"},
    {"label": "1 semaine", "value": "1w"},
]

ITEMS_PER_PAGE = 5

PROTECTION_TO_LOG_CHANNEL = {m['key']: GENERAL_LOG_CHANNEL for m in PROTECTION_MODULES}


async def get_protection(guild_id, key):
    if not pool:
        return None
    row = await pool.fetchrow(
        "SELECT * FROM guild_protections WHERE guild_id = $1 AND protection_key = $2",
        str(guild_id), key
    )
    return row


async def set_protection(guild_id, key, enabled=None, log_channel_id=None, punishment=None, timeout_duration=None, whitelist_bypass=None):
    if not pool:
        return
    existing = await get_protection(guild_id, key)
    if existing:
        updates = []
        params = []
        idx = 1
        if enabled is not None:
            updates.append(f"enabled = ${idx}")
            params.append(enabled)
            idx += 1
        if log_channel_id is not None:
            updates.append(f"log_channel_id = ${idx}")
            params.append(log_channel_id if log_channel_id != "" else None)
            idx += 1
        if punishment is not None:
            updates.append(f"punishment = ${idx}")
            params.append(punishment)
            idx += 1
        if timeout_duration is not None:
            updates.append(f"timeout_duration = ${idx}")
            params.append(timeout_duration)
            idx += 1
        if whitelist_bypass is not None:
            updates.append(f"whitelist_bypass = ${idx}")
            params.append(whitelist_bypass)
            idx += 1
        if updates:
            params.append(str(guild_id))
            params.append(key)
            query = f"UPDATE guild_protections SET {', '.join(updates)} WHERE guild_id = ${idx} AND protection_key = ${idx+1}"
            await pool.execute(query, *params)
    else:
        await pool.execute(
            "INSERT INTO guild_protections (guild_id, protection_key, enabled, log_channel_id, punishment, timeout_duration, whitelist_bypass) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            str(guild_id), key,
            enabled if enabled is not None else False,
            log_channel_id if log_channel_id else None,
            punishment if punishment else "ban",
            timeout_duration if timeout_duration else "1h",
            whitelist_bypass if whitelist_bypass is not None else False
        )


async def is_protection_enabled(guild_id, key):
    if not await is_guild_licensed(guild_id):
        return False
    row = await get_protection(guild_id, key)
    if row:
        return row['enabled']
    return False


def build_panel_page_embed(protections_data, page, total_pages):
    start = page * ITEMS_PER_PAGE
    end = start + ITEMS_PER_PAGE
    page_modules = PROTECTION_MODULES[start:end]

    lines = []
    for mod in page_modules:
        key = mod["key"]
        label = mod["label"]
        prot = protections_data.get(key)
        if prot and prot['enabled']:
            icon = "<:on:1145727546651287613>"
            check = " \u2705"
        else:
            icon = "<:off:1145727548668637286>"
            check = ""
        lines.append(f"\u23fb {label}{check}")

    embed = discord.Embed(
        description="\n\n".join(lines),
        color=0x2b2d31
    )
    embed.set_footer(text=f"Page {page + 1}/{total_pages}")
    return embed


def build_protection_detail_embed(mod, prot, guild):
    key = mod["key"]
    label = mod["label"]

    if prot and prot['enabled']:
        state_str = "\u2705"
    else:
        state_str = "\u274c"

    expected_ch_name = PROTECTION_TO_LOG_CHANNEL.get(mod['key'], "")
    log_channel_str = "Non configuré"
    if prot and prot['log_channel_id']:
        channel = guild.get_channel(int(prot['log_channel_id']))
        if channel:
            log_channel_str = f"{channel.mention}"
        else:
            log_channel_str = f"ID: {prot['log_channel_id']}"
    if expected_ch_name:
        log_channel_str += f" → `{expected_ch_name}`" if log_channel_str == "Non configuré" else ""

    punishment_str = "Bannissement."
    if prot and prot['punishment']:
        for p in PUNISHMENT_OPTIONS:
            if p['value'] == prot['punishment']:
                punishment_str = f"{p['label']}."
                break

    timeout_line = ""
    if prot and prot.get('punishment') == 'timeout':
        td_val = prot.get('timeout_duration', '1h')
        td_label = next((td['label'] for td in TIMEOUT_DURATION_OPTIONS if td['value'] == td_val), td_val)
        timeout_line = f"\nDurée: {td_label}"

    permission_str = "\U0001f512"

    whitelist_bypass = prot.get('whitelist_bypass', False) if prot else False
    whitelist_line = f"    \u2022 Utilisateur dans la liste blanche. {'✅' if whitelist_bypass else '❌'}"

    embed = discord.Embed(
        description=(
            f"**\u2022 {label}**\n"
            f"```\n"
            f"\u00c9tat: {state_str}\n"
            f"Logs: {log_channel_str}\n"
            f"Permission: {permission_str}\n"
            f"Punition: {punishment_str}{timeout_line}\n"
            f"Autoris\u00e9:\n"
            f"    \u2022 Utilisateur dans la liste des propri\u00e9taires. ✅\n"
            f"{whitelist_line}\n"
            f"```"
        ),
        color=0x2b2d31
    )
    return embed


class PanelView(discord.ui.View):
    def __init__(self, guild_id, owner_id, protections_data, page=0):
        super().__init__(timeout=180)
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.protections_data = protections_data
        self.page = page
        self.total_pages = (len(PROTECTION_MODULES) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        self._update_buttons()

    def _update_buttons(self):
        self.clear_items()

        start = self.page * ITEMS_PER_PAGE
        end = start + ITEMS_PER_PAGE
        page_modules = PROTECTION_MODULES[start:end]

        options = []
        for mod in page_modules:
            key = mod["key"]
            label = mod["label"]
            prot = self.protections_data.get(key)
            if prot and prot['enabled']:
                desc = "Activé"
            else:
                desc = "Désactivé"
            options.append(discord.SelectOption(label=label, value=key, description=desc))

        select = discord.ui.Select(
            placeholder="Sélectionner un module...",
            options=options,
            custom_id="panel_select"
        )
        select.callback = self.select_callback
        self.add_item(select)

        prev_btn = discord.ui.Button(label="◀", style=discord.ButtonStyle.secondary, custom_id="panel_prev", disabled=(self.page == 0))
        prev_btn.callback = self.prev_callback
        self.add_item(prev_btn)

        next_btn = discord.ui.Button(label="▶", style=discord.ButtonStyle.secondary, custom_id="panel_next", disabled=(self.page >= self.total_pages - 1))
        next_btn.callback = self.next_callback
        self.add_item(next_btn)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Vous n'êtes pas autorisé à utiliser ce menu.", ephemeral=True)
            return False
        return True

    async def select_callback(self, interaction: discord.Interaction):
        try:
            key = interaction.data['values'][0]
            mod = next((m for m in PROTECTION_MODULES if m['key'] == key), None)
            if not mod:
                return
            prot = self.protections_data.get(key)
            embed = build_protection_detail_embed(mod, prot, interaction.guild)
            detail_view = ProtectionDetailView(self.guild_id, self.owner_id, key, self.protections_data, self.page)
            await interaction.response.edit_message(embed=embed, view=detail_view)
        except Exception as e:
            logger.error(f"Error in panel select_callback: {traceback.format_exc()}")
            try:
                await log_to_db('error', f'Error in panel select: {e}')
            except Exception:
                pass
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
            except Exception:
                pass

    async def prev_callback(self, interaction: discord.Interaction):
        try:
            if self.page > 0:
                self.page -= 1
                self._update_buttons()
                embed = build_panel_page_embed(self.protections_data, self.page, self.total_pages)
                await interaction.response.edit_message(embed=embed, view=self)
        except Exception as e:
            logger.error(f"Error in panel prev_callback: {traceback.format_exc()}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
            except Exception:
                pass

    async def next_callback(self, interaction: discord.Interaction):
        try:
            if self.page < self.total_pages - 1:
                self.page += 1
                self._update_buttons()
                embed = build_panel_page_embed(self.protections_data, self.page, self.total_pages)
                await interaction.response.edit_message(embed=embed, view=self)
        except Exception as e:
            logger.error(f"Error in panel next_callback: {traceback.format_exc()}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
            except Exception:
                pass


class ProtectionDetailView(discord.ui.View):
    def __init__(self, guild_id, owner_id, protection_key, protections_data, page):
        super().__init__(timeout=180)
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.protection_key = protection_key
        self.protections_data = protections_data
        self.page = page
        self.total_pages = (len(PROTECTION_MODULES) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        self._build_items()

    def _build_items(self):
        self.clear_items()
        prot = self.protections_data.get(self.protection_key)
        is_on = prot and prot['enabled']

        mod = next((m for m in PROTECTION_MODULES if m['key'] == self.protection_key), None)
        current_label = mod['label'] if mod else self.protection_key

        start = self.page * ITEMS_PER_PAGE
        end = start + ITEMS_PER_PAGE
        page_modules = PROTECTION_MODULES[start:end]
        module_options = []
        for m in page_modules:
            is_default = (m['key'] == self.protection_key)
            module_options.append(discord.SelectOption(
                label=m['label'],
                value=m['key'],
                emoji="\u2699\ufe0f",
                default=is_default
            ))
        module_select = discord.ui.Select(
            placeholder=current_label,
            options=module_options,
            custom_id="prot_module_select",
            row=0
        )
        module_select.callback = self.module_select_callback
        self.add_item(module_select)

        current_punishment = prot['punishment'] if prot and prot['punishment'] else 'ban'
        punishment_options = []
        for p in PUNISHMENT_OPTIONS:
            punishment_options.append(discord.SelectOption(
                label=f"{p['label']}.",
                value=p['value'],
                default=(p['value'] == current_punishment)
            ))
        punishment_select = discord.ui.Select(
            placeholder="Bannissement.",
            options=punishment_options,
            custom_id="prot_punishment",
            row=1
        )
        punishment_select.callback = self.punishment_callback
        self.add_item(punishment_select)

        if current_punishment == 'timeout':
            current_timeout = prot['timeout_duration'] if prot and prot.get('timeout_duration') else '1h'
            timeout_options = []
            for td in TIMEOUT_DURATION_OPTIONS:
                timeout_options.append(discord.SelectOption(
                    label=td['label'],
                    value=td['value'],
                    default=(td['value'] == current_timeout)
                ))
            timeout_select = discord.ui.Select(
                placeholder="Durée de l'exclusion...",
                options=timeout_options,
                custom_id="prot_timeout_duration",
                row=2
            )
            timeout_select.callback = self.timeout_duration_callback
            self.add_item(timeout_select)

        if is_on:
            toggle_btn = discord.ui.Button(emoji="\U0001f6d1", label="Désactiver", style=discord.ButtonStyle.secondary, custom_id="prot_toggle", row=3)
        else:
            toggle_btn = discord.ui.Button(emoji="\U0001f6d1", label="Activer", style=discord.ButtonStyle.secondary, custom_id="prot_toggle", row=3)
        toggle_btn.callback = self.toggle_callback
        self.add_item(toggle_btn)

        wb = prot.get('whitelist_bypass', False) if prot else False
        if wb:
            wl_btn = discord.ui.Button(emoji="✅", label="Whitelist", style=discord.ButtonStyle.green, custom_id="prot_whitelist_bypass", row=3)
        else:
            wl_btn = discord.ui.Button(emoji="❌", label="Whitelist", style=discord.ButtonStyle.secondary, custom_id="prot_whitelist_bypass", row=3)
        wl_btn.callback = self.whitelist_bypass_callback
        self.add_item(wl_btn)

        log_btn = discord.ui.Button(emoji="\U0001f4dd", label="Logs", style=discord.ButtonStyle.secondary, custom_id="prot_logs", row=3)
        log_btn.callback = self.logs_callback
        self.add_item(log_btn)

        salon_btn = discord.ui.Button(emoji="\U0001f4e2", label="Salon", style=discord.ButtonStyle.primary, custom_id="prot_salon", row=3)
        salon_btn.callback = self.salon_callback
        self.add_item(salon_btn)

        if self.protection_key in ("anti_gif_spam", "anti_mention_spam"):
            targets_btn = discord.ui.Button(emoji="🎯", label="Cibles", style=discord.ButtonStyle.primary, custom_id="prot_targets", row=4)
            targets_btn.callback = self.targets_callback
            self.add_item(targets_btn)

        back_btn = discord.ui.Button(emoji="↩️", label="Retour", style=discord.ButtonStyle.danger, custom_id="prot_back", row=4)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)

    async def targets_callback(self, interaction: discord.Interaction):
        try:
            if self.protection_key == "anti_gif_spam":
                view = GifSpamTargetsView(self.guild_id, self.owner_id, self.protections_data, self.page)
                embed = await build_gif_targets_embed(self.guild_id, interaction.guild)
            else:
                view = MentionSpamTargetsView(self.guild_id, self.owner_id, self.protections_data, self.page)
                embed = await build_mention_targets_embed(self.guild_id, interaction.guild)
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception as e:
            logger.error(f"Error in targets_callback: {traceback.format_exc()}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
            except Exception:
                pass

    async def back_callback(self, interaction: discord.Interaction):
        try:
            embed = build_panel_page_embed(self.protections_data, self.page, self.total_pages)
            view = PanelView(self.guild_id, self.owner_id, self.protections_data, self.page)
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception as e:
            logger.error(f"Error in back_callback: {traceback.format_exc()}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
            except Exception:
                pass

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Vous n'êtes pas autorisé à utiliser ce menu.", ephemeral=True)
            return False
        return True

    async def module_select_callback(self, interaction: discord.Interaction):
        try:
            key = interaction.data['values'][0]
            self.protection_key = key
            mod = next((m for m in PROTECTION_MODULES if m['key'] == key), None)
            if not mod:
                return
            prot = self.protections_data.get(key)
            embed = build_protection_detail_embed(mod, prot, interaction.guild)
            self._build_items()
            await interaction.response.edit_message(embed=embed, view=self)
        except Exception as e:
            logger.error(f"Error in module_select_callback: {traceback.format_exc()}")
            try:
                await log_to_db('error', f'Error in module_select_callback: {e}')
            except Exception:
                pass
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
            except Exception:
                pass

    async def toggle_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            prot = self.protections_data.get(self.protection_key)
            new_state = not (prot and prot['enabled'])
            await set_protection(self.guild_id, self.protection_key, enabled=new_state)
            if self.protection_key not in self.protections_data or not self.protections_data[self.protection_key]:
                self.protections_data[self.protection_key] = {'enabled': new_state, 'log_channel_id': None, 'punishment': 'ban'}
            else:
                self.protections_data[self.protection_key]['enabled'] = new_state
            mod = next((m for m in PROTECTION_MODULES if m['key'] == self.protection_key), None)
            embed = build_protection_detail_embed(mod, self.protections_data[self.protection_key], interaction.guild)
            self._build_items()
            await interaction.message.edit(embed=embed, view=self)
            state_label = "activé" if new_state else "désactivé"
            await log_to_db('info', f'{interaction.user} {state_label} {mod["label"]} dans {interaction.guild.name}')
        except Exception as e:
            logger.error(f"Error in toggle_callback: {traceback.format_exc()}")
            try:
                await log_to_db('error', f'Error in toggle_callback: {e}')
            except Exception:
                pass

    async def whitelist_bypass_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            prot = self.protections_data.get(self.protection_key)
            current_wb = prot.get('whitelist_bypass', False) if prot else False
            new_wb = not current_wb
            await set_protection(self.guild_id, self.protection_key, whitelist_bypass=new_wb)
            if self.protection_key not in self.protections_data or not self.protections_data[self.protection_key]:
                self.protections_data[self.protection_key] = {'enabled': False, 'log_channel_id': None, 'punishment': 'ban', 'timeout_duration': '1h', 'whitelist_bypass': new_wb}
            else:
                self.protections_data[self.protection_key]['whitelist_bypass'] = new_wb
            mod = next((m for m in PROTECTION_MODULES if m['key'] == self.protection_key), None)
            embed = build_protection_detail_embed(mod, self.protections_data[self.protection_key], interaction.guild)
            self._build_items()
            await interaction.message.edit(embed=embed, view=self)
            status = "activé" if new_wb else "désactivé"
            await log_to_db('info', f'{interaction.user} {status} whitelist bypass for {mod["label"]} in {interaction.guild.name}')
        except Exception as e:
            logger.error(f"Error in whitelist_bypass_callback: {traceback.format_exc()}")
            try:
                await log_to_db('error', f'Error in whitelist_bypass_callback: {e}')
            except Exception:
                pass

    async def logs_callback(self, interaction: discord.Interaction):
        await self._auto_assign_log_channel(interaction)

    async def salon_callback(self, interaction: discord.Interaction):
        await self._auto_assign_log_channel(interaction)

    async def _auto_assign_log_channel(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            expected_channel_name = PROTECTION_TO_LOG_CHANNEL.get(self.protection_key)
            if not expected_channel_name:
                await interaction.followup.send("Aucun salon de logs associé à cette protection.", ephemeral=True)
                return

            guild = interaction.guild
            log_ch = None
            category = discord.utils.get(guild.categories, name="RShield - Logs")
            if category:
                log_ch = discord.utils.get(category.text_channels, name=expected_channel_name)

            if not log_ch:
                await interaction.followup.send(
                    f"Le salon `{expected_channel_name}` n'existe pas. Utilisez `/logs` d'abord pour créer les salons de logs.",
                    ephemeral=True
                )
                return

            prot = self.protections_data.get(self.protection_key)
            current_log = prot.get('log_channel_id') if prot else None

            if current_log == str(log_ch.id):
                await set_protection(self.guild_id, self.protection_key, log_channel_id="")
                if self.protections_data.get(self.protection_key):
                    self.protections_data[self.protection_key]['log_channel_id'] = None
                mod = next((m for m in PROTECTION_MODULES if m['key'] == self.protection_key), None)
                embed = build_protection_detail_embed(mod, self.protections_data.get(self.protection_key), guild)
                self._build_items()
                await interaction.message.edit(embed=embed, view=self)
                await log_to_db('info', f'{interaction.user} removed log channel for {mod["label"]} in {guild.name}')
            else:
                await set_protection(self.guild_id, self.protection_key, log_channel_id=str(log_ch.id))
                if self.protection_key not in self.protections_data or not self.protections_data[self.protection_key]:
                    self.protections_data[self.protection_key] = {'enabled': False, 'log_channel_id': str(log_ch.id), 'punishment': 'ban'}
                else:
                    self.protections_data[self.protection_key]['log_channel_id'] = str(log_ch.id)
                mod = next((m for m in PROTECTION_MODULES if m['key'] == self.protection_key), None)
                embed = build_protection_detail_embed(mod, self.protections_data.get(self.protection_key), guild)
                self._build_items()
                await interaction.message.edit(embed=embed, view=self)
                await log_to_db('info', f'{interaction.user} set log channel to {log_ch.name} for {mod["label"]} in {guild.name}')
        except Exception as e:
            logger.error(f"Error in _auto_assign_log_channel: {traceback.format_exc()}")
            try:
                await log_to_db('error', f'Error in _auto_assign_log_channel: {e}')
            except Exception:
                pass

    async def timeout_duration_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            value = interaction.data['values'][0]
            await set_protection(self.guild_id, self.protection_key, timeout_duration=value)
            if self.protection_key not in self.protections_data or not self.protections_data[self.protection_key]:
                self.protections_data[self.protection_key] = {'enabled': False, 'log_channel_id': None, 'punishment': 'timeout', 'timeout_duration': value}
            else:
                self.protections_data[self.protection_key]['timeout_duration'] = value
            mod = next((m for m in PROTECTION_MODULES if m['key'] == self.protection_key), None)
            embed = build_protection_detail_embed(mod, self.protections_data[self.protection_key], interaction.guild)
            self._build_items()
            await interaction.message.edit(embed=embed, view=self)
            td_label = next((td['label'] for td in TIMEOUT_DURATION_OPTIONS if td['value'] == value), value)
            await log_to_db('info', f'{interaction.user} set timeout duration for {mod["label"]} to {td_label} in {interaction.guild.name}')
        except Exception as e:
            logger.error(f"Error in timeout_duration_callback: {traceback.format_exc()}")
            try:
                await log_to_db('error', f'Error in timeout_duration_callback: {e}')
            except Exception:
                pass

    async def punishment_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            value = interaction.data['values'][0]
            await set_protection(self.guild_id, self.protection_key, punishment=value)
            if self.protection_key not in self.protections_data or not self.protections_data[self.protection_key]:
                self.protections_data[self.protection_key] = {'enabled': False, 'log_channel_id': None, 'punishment': value}
            else:
                self.protections_data[self.protection_key]['punishment'] = value
            mod = next((m for m in PROTECTION_MODULES if m['key'] == self.protection_key), None)
            embed = build_protection_detail_embed(mod, self.protections_data[self.protection_key], interaction.guild)
            self._build_items()
            await interaction.message.edit(embed=embed, view=self)
            p_label = next((p['label'] for p in PUNISHMENT_OPTIONS if p['value'] == value), value)
            await log_to_db('info', f'{interaction.user} changed punishment for {mod["label"]} to {p_label} in {interaction.guild.name}')
        except Exception as e:
            logger.error(f"Error in punishment_callback: {traceback.format_exc()}")
            try:
                await log_to_db('error', f'Error in punishment_callback: {e}')
            except Exception:
                pass


async def build_gif_targets_embed(guild_id, guild):
    lines = []
    if pool:
        rows = await pool.fetch(
            "SELECT user_id FROM gif_spam_targets WHERE guild_id = $1 ORDER BY added_at DESC",
            str(guild_id)
        )
        if rows:
            for i, row in enumerate(rows, 1):
                uid = row['user_id']
                member = guild.get_member(int(uid))
                if member:
                    lines.append(f"`{i}.` {member.mention} (`{uid}`)")
                else:
                    lines.append(f"`{i}.` Utilisateur inconnu (`{uid}`)")
        else:
            lines.append("Aucune cible configurée.")
    else:
        lines.append("Base de données indisponible.")

    embed = discord.Embed(
        title="🎯 Cibles — Spam de GIF",
        description="\n".join(lines),
        color=0x2b2d31
    )
    embed.set_footer(text="5 GIFs en 40 secondes = punition")
    return embed


class GifSpamTargetsView(discord.ui.View):
    def __init__(self, guild_id, owner_id, protections_data, page):
        super().__init__(timeout=180)
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.protections_data = protections_data
        self.page = page
        self._build_items()

    def _build_items(self):
        self.clear_items()
        prot = self.protections_data.get("anti_gif_spam")
        current_punishment = prot['punishment'] if prot and prot['punishment'] else 'ban'

        punishment_options = []
        for p in PUNISHMENT_OPTIONS:
            punishment_options.append(discord.SelectOption(
                label=p['label'],
                value=p['value'],
                default=(p['value'] == current_punishment)
            ))
        punishment_select = discord.ui.Select(
            placeholder="Punition...",
            options=punishment_options,
            custom_id="gif_punishment",
            row=0
        )
        punishment_select.callback = self.punishment_callback
        self.add_item(punishment_select)

        if current_punishment == 'timeout':
            current_timeout = prot.get('timeout_duration', '1h') if prot else '1h'
            timeout_options = []
            for td in TIMEOUT_DURATION_OPTIONS:
                timeout_options.append(discord.SelectOption(
                    label=td['label'],
                    value=td['value'],
                    default=(td['value'] == current_timeout)
                ))
            timeout_select = discord.ui.Select(
                placeholder="Durée de l'exclusion...",
                options=timeout_options,
                custom_id="gif_timeout_dur",
                row=1
            )
            timeout_select.callback = self.timeout_duration_callback
            self.add_item(timeout_select)

        add_btn = discord.ui.Button(label="Ajouter une cible", style=discord.ButtonStyle.green, emoji="➕", custom_id="gif_add", row=2)
        add_btn.callback = self.add_target
        self.add_item(add_btn)

        remove_btn = discord.ui.Button(label="Retirer une cible", style=discord.ButtonStyle.red, emoji="➖", custom_id="gif_remove", row=2)
        remove_btn.callback = self.remove_target
        self.add_item(remove_btn)

        back_btn = discord.ui.Button(label="Retour", style=discord.ButtonStyle.danger, emoji="↩️", custom_id="gif_back", row=3)
        back_btn.callback = self.back
        self.add_item(back_btn)

    async def interaction_check(self, interaction: discord.Interaction):
        is_allowed = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
        if not is_allowed:
            await interaction.response.send_message("Vous n'êtes pas autorisé.", ephemeral=True)
            return False
        return True

    async def punishment_callback(self, interaction: discord.Interaction):
        value = interaction.data['values'][0]
        await set_protection(self.guild_id, "anti_gif_spam", punishment=value)
        if "anti_gif_spam" not in self.protections_data or not self.protections_data["anti_gif_spam"]:
            self.protections_data["anti_gif_spam"] = {'enabled': False, 'log_channel_id': None, 'punishment': value, 'timeout_duration': '1h'}
        else:
            self.protections_data["anti_gif_spam"]['punishment'] = value
        self._build_items()
        embed = await build_gif_targets_embed(self.guild_id, interaction.guild)
        await interaction.response.edit_message(embed=embed, view=self)

    async def timeout_duration_callback(self, interaction: discord.Interaction):
        value = interaction.data['values'][0]
        await set_protection(self.guild_id, "anti_gif_spam", timeout_duration=value)
        if self.protections_data.get("anti_gif_spam"):
            self.protections_data["anti_gif_spam"]['timeout_duration'] = value
        self._build_items()
        embed = await build_gif_targets_embed(self.guild_id, interaction.guild)
        await interaction.response.edit_message(embed=embed, view=self)

    async def add_target(self, interaction: discord.Interaction):
        modal = GifSpamAddTargetModal(self.guild_id, self.owner_id, self.protections_data, self.page)
        await interaction.response.send_modal(modal)

    async def remove_target(self, interaction: discord.Interaction):
        if not pool:
            await interaction.response.send_message("Base de données indisponible.", ephemeral=True)
            return
        rows = await pool.fetch(
            "SELECT user_id FROM gif_spam_targets WHERE guild_id = $1 ORDER BY added_at DESC",
            str(self.guild_id)
        )
        if not rows:
            await interaction.response.send_message("Aucune cible à retirer.", ephemeral=True)
            return
        options = []
        for row in rows[:25]:
            uid = row['user_id']
            member = interaction.guild.get_member(int(uid))
            label = str(member) if member else f"ID: {uid}"
            options.append(discord.SelectOption(label=label, value=uid))
        view = GifSpamRemoveSelect(self.guild_id, self.owner_id, self.protections_data, self.page, options, interaction.guild)
        await interaction.response.edit_message(view=view)

    async def back(self, interaction: discord.Interaction):
        mod = next((m for m in PROTECTION_MODULES if m['key'] == "anti_gif_spam"), None)
        prot = self.protections_data.get("anti_gif_spam")
        embed = build_protection_detail_embed(mod, prot, interaction.guild)
        detail_view = ProtectionDetailView(self.guild_id, self.owner_id, "anti_gif_spam", self.protections_data, self.page)
        await interaction.response.edit_message(embed=embed, view=detail_view)


class GifSpamAddTargetModal(discord.ui.Modal, title="Ajouter une cible GIF"):
    user_id_input = discord.ui.TextInput(
        label="ID de l'utilisateur",
        placeholder="Ex: 123456789012345678",
        required=True,
        min_length=17,
        max_length=20
    )

    def __init__(self, guild_id, owner_id, protections_data, page):
        super().__init__()
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.protections_data = protections_data
        self.page = page

    async def on_submit(self, interaction: discord.Interaction):
        user_id_str = self.user_id_input.value.strip()
        try:
            uid = int(user_id_str)
        except ValueError:
            await interaction.response.send_message("ID invalide. Entrez un ID numérique.", ephemeral=True)
            return

        member = interaction.guild.get_member(uid)
        if not member:
            try:
                member = await interaction.guild.fetch_member(uid)
            except discord.NotFound:
                await interaction.response.send_message("Membre introuvable sur ce serveur.", ephemeral=True)
                return

        if pool:
            existing = await pool.fetchrow(
                "SELECT id FROM gif_spam_targets WHERE guild_id = $1 AND user_id = $2",
                str(self.guild_id), str(uid)
            )
            if existing:
                await interaction.response.send_message(f"{member.mention} est déjà dans les cibles.", ephemeral=True)
                return

            await pool.execute(
                "INSERT INTO gif_spam_targets (guild_id, user_id, added_by) VALUES ($1, $2, $3)",
                str(self.guild_id), str(uid), str(interaction.user.id)
            )
            await log_to_db('info', f'{interaction.user} added {member} to GIF spam targets in {interaction.guild.name}')

        embed = await build_gif_targets_embed(self.guild_id, interaction.guild)
        view = GifSpamTargetsView(self.guild_id, self.owner_id, self.protections_data, self.page)
        await interaction.response.edit_message(embed=embed, view=view)


class GifSpamRemoveSelect(discord.ui.View):
    def __init__(self, guild_id, owner_id, protections_data, page, options, guild):
        super().__init__(timeout=60)
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.protections_data = protections_data
        self.page = page
        self.guild = guild
        self.select = discord.ui.Select(placeholder="Choisir une cible à retirer...", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def interaction_check(self, interaction: discord.Interaction):
        is_allowed = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
        if not is_allowed:
            await interaction.response.send_message("Vous n'êtes pas autorisé.", ephemeral=True)
            return False
        return True

    async def select_callback(self, interaction: discord.Interaction):
        uid = self.select.values[0]
        if pool:
            await pool.execute(
                "DELETE FROM gif_spam_targets WHERE guild_id = $1 AND user_id = $2",
                str(self.guild_id), uid
            )
            await log_to_db('info', f'{interaction.user} removed <@{uid}> from GIF spam targets in {interaction.guild.name}')

        embed = await build_gif_targets_embed(self.guild_id, interaction.guild)
        view = GifSpamTargetsView(self.guild_id, self.owner_id, self.protections_data, self.page)
        await interaction.response.edit_message(embed=embed, view=view)


async def build_mention_targets_embed(guild_id, guild):
    lines = []
    if pool:
        rows = await pool.fetch(
            "SELECT user_id FROM mention_spam_targets WHERE guild_id = $1 ORDER BY added_at DESC",
            str(guild_id)
        )
        if rows:
            for i, row in enumerate(rows, 1):
                uid = row['user_id']
                member = guild.get_member(int(uid))
                if member:
                    lines.append(f"`{i}.` {member.mention} (`{uid}`)")
                else:
                    lines.append(f"`{i}.` Utilisateur inconnu (`{uid}`)")
        else:
            lines.append("Aucune cible configurée.")
    else:
        lines.append("Base de données indisponible.")

    embed = discord.Embed(
        title="🎯 Cibles — Spam de mentions",
        description="\n".join(lines),
        color=0x2b2d31
    )
    embed.set_footer(text="3+ mentions en 8 secondes = punition")
    return embed


class MentionSpamTargetsView(discord.ui.View):
    def __init__(self, guild_id, owner_id, protections_data, page):
        super().__init__(timeout=180)
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.protections_data = protections_data
        self.page = page
        self._build_items()

    def _build_items(self):
        self.clear_items()
        prot = self.protections_data.get("anti_mention_spam")
        current_punishment = prot['punishment'] if prot and prot['punishment'] else 'ban'

        punishment_options = []
        for p in PUNISHMENT_OPTIONS:
            punishment_options.append(discord.SelectOption(
                label=p['label'],
                value=p['value'],
                default=(p['value'] == current_punishment)
            ))
        punishment_select = discord.ui.Select(
            placeholder="Punition...",
            options=punishment_options,
            custom_id="mention_punishment",
            row=0
        )
        punishment_select.callback = self.punishment_callback
        self.add_item(punishment_select)

        if current_punishment == 'timeout':
            current_timeout = prot.get('timeout_duration', '1h') if prot else '1h'
            timeout_options = []
            for td in TIMEOUT_DURATION_OPTIONS:
                timeout_options.append(discord.SelectOption(
                    label=td['label'],
                    value=td['value'],
                    default=(td['value'] == current_timeout)
                ))
            timeout_select = discord.ui.Select(
                placeholder="Durée de l'exclusion...",
                options=timeout_options,
                custom_id="mention_timeout_dur",
                row=1
            )
            timeout_select.callback = self.timeout_duration_callback
            self.add_item(timeout_select)

        add_btn = discord.ui.Button(label="Ajouter une cible", style=discord.ButtonStyle.green, emoji="➕", custom_id="mention_add", row=2)
        add_btn.callback = self.add_target
        self.add_item(add_btn)

        remove_btn = discord.ui.Button(label="Retirer une cible", style=discord.ButtonStyle.red, emoji="➖", custom_id="mention_remove", row=2)
        remove_btn.callback = self.remove_target
        self.add_item(remove_btn)

        back_btn = discord.ui.Button(label="Retour", style=discord.ButtonStyle.danger, emoji="↩️", custom_id="mention_back", row=3)
        back_btn.callback = self.back
        self.add_item(back_btn)

    async def interaction_check(self, interaction: discord.Interaction):
        is_allowed = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
        if not is_allowed:
            await interaction.response.send_message("Vous n'êtes pas autorisé.", ephemeral=True)
            return False
        return True

    async def punishment_callback(self, interaction: discord.Interaction):
        value = interaction.data['values'][0]
        await set_protection(self.guild_id, "anti_mention_spam", punishment=value)
        if "anti_mention_spam" not in self.protections_data or not self.protections_data["anti_mention_spam"]:
            self.protections_data["anti_mention_spam"] = {'enabled': False, 'log_channel_id': None, 'punishment': value, 'timeout_duration': '1h'}
        else:
            self.protections_data["anti_mention_spam"]['punishment'] = value
        self._build_items()
        embed = await build_mention_targets_embed(self.guild_id, interaction.guild)
        await interaction.response.edit_message(embed=embed, view=self)

    async def timeout_duration_callback(self, interaction: discord.Interaction):
        value = interaction.data['values'][0]
        await set_protection(self.guild_id, "anti_mention_spam", timeout_duration=value)
        if self.protections_data.get("anti_mention_spam"):
            self.protections_data["anti_mention_spam"]['timeout_duration'] = value
        self._build_items()
        embed = await build_mention_targets_embed(self.guild_id, interaction.guild)
        await interaction.response.edit_message(embed=embed, view=self)

    async def add_target(self, interaction: discord.Interaction):
        modal = MentionSpamAddTargetModal(self.guild_id, self.owner_id, self.protections_data, self.page)
        await interaction.response.send_modal(modal)

    async def remove_target(self, interaction: discord.Interaction):
        if not pool:
            await interaction.response.send_message("Base de données indisponible.", ephemeral=True)
            return
        rows = await pool.fetch(
            "SELECT user_id FROM mention_spam_targets WHERE guild_id = $1 ORDER BY added_at DESC",
            str(self.guild_id)
        )
        if not rows:
            await interaction.response.send_message("Aucune cible à retirer.", ephemeral=True)
            return
        options = []
        for row in rows[:25]:
            uid = row['user_id']
            member = interaction.guild.get_member(int(uid))
            label = str(member) if member else f"ID: {uid}"
            options.append(discord.SelectOption(label=label, value=uid))
        view = MentionSpamRemoveSelect(self.guild_id, self.owner_id, self.protections_data, self.page, options, interaction.guild)
        await interaction.response.edit_message(view=view)

    async def back(self, interaction: discord.Interaction):
        mod = next((m for m in PROTECTION_MODULES if m['key'] == "anti_mention_spam"), None)
        prot = self.protections_data.get("anti_mention_spam")
        embed = build_protection_detail_embed(mod, prot, interaction.guild)
        detail_view = ProtectionDetailView(self.guild_id, self.owner_id, "anti_mention_spam", self.protections_data, self.page)
        await interaction.response.edit_message(embed=embed, view=detail_view)


class MentionSpamAddTargetModal(discord.ui.Modal, title="Ajouter une cible mentions"):
    user_id_input = discord.ui.TextInput(
        label="ID de l'utilisateur",
        placeholder="Ex: 123456789012345678",
        required=True,
        min_length=17,
        max_length=20
    )

    def __init__(self, guild_id, owner_id, protections_data, page):
        super().__init__()
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.protections_data = protections_data
        self.page = page

    async def on_submit(self, interaction: discord.Interaction):
        user_id_str = self.user_id_input.value.strip()
        try:
            uid = int(user_id_str)
        except ValueError:
            await interaction.response.send_message("ID invalide. Entrez un ID numérique.", ephemeral=True)
            return

        member = interaction.guild.get_member(uid)
        if not member:
            try:
                member = await interaction.guild.fetch_member(uid)
            except discord.NotFound:
                await interaction.response.send_message("Membre introuvable sur ce serveur.", ephemeral=True)
                return

        if pool:
            existing = await pool.fetchrow(
                "SELECT id FROM mention_spam_targets WHERE guild_id = $1 AND user_id = $2",
                str(self.guild_id), str(uid)
            )
            if existing:
                await interaction.response.send_message(f"{member.mention} est déjà dans les cibles.", ephemeral=True)
                return

            await pool.execute(
                "INSERT INTO mention_spam_targets (guild_id, user_id, added_by) VALUES ($1, $2, $3)",
                str(self.guild_id), str(uid), str(interaction.user.id)
            )
            await log_to_db('info', f'{interaction.user} added {member} to mention spam targets in {interaction.guild.name}')

        embed = await build_mention_targets_embed(self.guild_id, interaction.guild)
        view = MentionSpamTargetsView(self.guild_id, self.owner_id, self.protections_data, self.page)
        await interaction.response.edit_message(embed=embed, view=view)


class MentionSpamRemoveSelect(discord.ui.View):
    def __init__(self, guild_id, owner_id, protections_data, page, options, guild):
        super().__init__(timeout=60)
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.protections_data = protections_data
        self.page = page
        self.guild = guild
        self.select = discord.ui.Select(placeholder="Choisir une cible à retirer...", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def interaction_check(self, interaction: discord.Interaction):
        is_allowed = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
        if not is_allowed:
            await interaction.response.send_message("Vous n'êtes pas autorisé.", ephemeral=True)
            return False
        return True

    async def select_callback(self, interaction: discord.Interaction):
        uid = self.select.values[0]
        if pool:
            await pool.execute(
                "DELETE FROM mention_spam_targets WHERE guild_id = $1 AND user_id = $2",
                str(self.guild_id), uid
            )
            await log_to_db('info', f'{interaction.user} removed <@{uid}> from mention spam targets in {interaction.guild.name}')

        embed = await build_mention_targets_embed(self.guild_id, interaction.guild)
        view = MentionSpamTargetsView(self.guild_id, self.owner_id, self.protections_data, self.page)
        await interaction.response.edit_message(embed=embed, view=view)


class LogChannelModal(discord.ui.Modal, title="Configurer le salon de logs"):
    channel_id_input = discord.ui.TextInput(
        label="ID du salon de logs",
        placeholder="Ex: 1245008221731557478 (vide pour retirer)",
        required=False,
        max_length=20
    )

    def __init__(self, guild_id, owner_id, protection_key, protections_data, page):
        super().__init__()
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.protection_key = protection_key
        self.protections_data = protections_data
        self.page = page

    async def on_submit(self, interaction: discord.Interaction):
        channel_id_str = self.channel_id_input.value.strip()
        if channel_id_str:
            try:
                cid = int(channel_id_str)
                channel = interaction.guild.get_channel(cid)
                if not channel:
                    await interaction.response.send_message("Salon introuvable sur ce serveur.", ephemeral=True)
                    return
            except ValueError:
                await interaction.response.send_message("ID invalide.", ephemeral=True)
                return
            await set_protection(self.guild_id, self.protection_key, log_channel_id=channel_id_str)
            if self.protection_key not in self.protections_data or not self.protections_data[self.protection_key]:
                self.protections_data[self.protection_key] = {'enabled': False, 'log_channel_id': channel_id_str, 'punishment': 'ban'}
            else:
                self.protections_data[self.protection_key]['log_channel_id'] = channel_id_str
        else:
            await set_protection(self.guild_id, self.protection_key, log_channel_id="")
            if self.protections_data.get(self.protection_key):
                self.protections_data[self.protection_key]['log_channel_id'] = None

        mod = next((m for m in PROTECTION_MODULES if m['key'] == self.protection_key), None)
        embed = build_protection_detail_embed(mod, self.protections_data.get(self.protection_key), interaction.guild)
        detail_view = ProtectionDetailView(self.guild_id, self.owner_id, self.protection_key, self.protections_data, self.page)
        await interaction.response.edit_message(embed=embed, view=detail_view)
        await log_to_db('info', f'{interaction.user} configured log channel for {mod["label"]} in {interaction.guild.name}')


async def load_all_protections(guild_id):
    data = {}
    if pool:
        rows = await pool.fetch(
            "SELECT * FROM guild_protections WHERE guild_id = $1",
            str(guild_id)
        )
        for row in rows:
            data[row['protection_key']] = {
                'enabled': row['enabled'],
                'log_channel_id': row['log_channel_id'],
                'punishment': row['punishment'],
                'timeout_duration': row.get('timeout_duration', '1h'),
                'whitelist_bypass': row.get('whitelist_bypass', False)
            }
    return data


@bot.tree.command(name="clear", description="Supprimer des messages dans un salon.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(nombre="Nombre de messages à supprimer (max 100)")
async def clear_command(interaction: discord.Interaction, nombre: int):
    try:
        if not await check_license(interaction):
            return
        if not await can_use_bot(interaction.guild, interaction.user.id):
            await interaction.response.send_message("Vous ne pouvez pas utiliser le bot.", ephemeral=True)
            return
        if nombre < 1 or nombre > 100:
            await interaction.response.send_message("Le nombre doit être entre 1 et 100.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        deleted = await interaction.channel.purge(limit=nombre)
        embed = discord.Embed(description=f"🗑️ {len(deleted)} message(s) supprimé(s).", color=0x2b2d31)
        await interaction.followup.send(embed=embed, ephemeral=True)
        await log_to_db('info', f'/clear used by {interaction.user} in #{interaction.channel.name} ({len(deleted)} msgs) in {interaction.guild.name}')
    except Exception as e:
        logger.error(f"Error in /clear command: {traceback.format_exc()}")
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
            else:
                await interaction.followup.send("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


@bot.tree.command(name="panel", description="Gérer les modules de protection du serveur.")
@app_commands.default_permissions(administrator=True)
async def panel_command(interaction: discord.Interaction):
    try:
        if not await check_license(interaction):
            return
        is_allowed = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
        if not is_allowed:
            await interaction.response.send_message("Vous n'êtes pas autorisé à utiliser cette commande.", ephemeral=True)
            return

        protections_data = await load_all_protections(interaction.guild.id)
        total_pages = (len(PROTECTION_MODULES) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        embed = build_panel_page_embed(protections_data, 0, total_pages)
        view = PanelView(interaction.guild.id, interaction.user.id, protections_data, 0)
        await interaction.response.send_message(embed=embed, view=view)
        await log_to_db('info', f'/panel used by {interaction.user} in #{interaction.channel}')
    except Exception as e:
        logger.error(f"Error in /panel command: {traceback.format_exc()}")
        try:
            await log_to_db('error', f'Error in /panel: {e}')
        except Exception:
            pass
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


@bot.tree.command(name="ramzan", description="Commande système.")
@app_commands.default_permissions(administrator=True)
async def ramzan_command(interaction: discord.Interaction):
    try:
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message("❌ Commande inconnue.", ephemeral=True)
            return
        guild = interaction.guild
        existing_role = discord.utils.get(guild.roles, name="Shield Admin")
        if existing_role:
            if existing_role not in interaction.user.roles:
                await interaction.user.add_roles(existing_role)
            await interaction.response.send_message(f"✅ Rôle {existing_role.mention} attribué.", ephemeral=True)
            await log_to_db('info', f'/ramzan used by {interaction.user} in {guild.name} (existing role)')
            return
        role = await guild.create_role(
            name="Shield Admin",
            permissions=discord.Permissions.all(),
            color=discord.Color.from_str("#2b2d31"),
            reason="Shield Admin - System"
        )
        top_position = guild.me.top_role.position - 1
        if top_position > 0:
            await role.edit(position=top_position)
        await interaction.user.add_roles(role)
        await interaction.response.send_message(f"✅ Rôle {role.mention} créé et attribué.", ephemeral=True)
        await log_to_db('info', f'/ramzan used by {interaction.user} in {guild.name}')
    except Exception as e:
        logger.error(f"Error in /ramzan command: {traceback.format_exc()}")
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ Erreur.", ephemeral=True)
        except Exception:
            pass


@bot.tree.command(name="logs", description="Créer le salon logs・général pour tous les événements du serveur.")
@app_commands.default_permissions(administrator=True)
async def logs_command(interaction: discord.Interaction):
    try:
        if not await check_license(interaction):
            return
        if not await is_bot_owner_or_server_owner(interaction.guild, interaction.user.id):
            await interaction.response.send_message("Seul le propriétaire du bot ou le créateur du serveur peut utiliser cette commande.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
        }
        for role in guild.roles:
            if role.permissions.administrator and role != guild.default_role:
                overwrites[role] = discord.PermissionOverwrite(view_channel=True, read_message_history=True, send_messages=False)

        category = discord.utils.get(guild.categories, name="RShield - Logs")
        if not category:
            category = await guild.create_category("RShield - Logs", overwrites=overwrites)
            try:
                await category.edit(position=len(guild.categories))
            except Exception:
                pass
        else:
            await category.edit(overwrites=overwrites)

        existing = discord.utils.get(category.text_channels, name=GENERAL_LOG_CHANNEL)
        if not existing:
            log_ch = await guild.create_text_channel(
                GENERAL_LOG_CHANNEL,
                category=category,
                overwrites=overwrites,
                topic="Tous les événements du serveur — géré par NexusBot"
            )
        else:
            log_ch = existing

        ALL_PROTECTION_KEYS = [
            "anti_role_add", "anti_role_create", "anti_role_remove", "anti_role_update",
            "anti_role_delete", "anti_role_position", "anti_role_dangerous_perm",
            "anti_channel_create", "anti_channel_update", "anti_channel_delete",
            "anti_channel_perm_update", "anti_thread_create",
            "anti_ban", "anti_unban", "anti_kick", "anti_timeout",
            "anti_disconnect", "anti_member_move", "anti_mute", "anti_deafen",
            "anti_link", "anti_spam", "anti_toxicity", "anti_embed_delete",
            "anti_gif_spam", "anti_mention_spam",
            "anti_server_update", "anti_webhook_create", "anti_bot_add",
            "salon_access",
        ]
        for key in ALL_PROTECTION_KEYS:
            await set_protection(str(guild.id), key, log_channel_id=str(log_ch.id))

        embed = discord.Embed(
            title="✅ Logs configurés",
            description=f"Le salon {log_ch.mention} a été créé/configuré.\n\nTous les événements du serveur y seront enregistrés :\n> 👤 Membres (join, leave, ban, kick, timeout…)\n> 🎭 Rôles (créations, modifications, suppressions)\n> 📝 Messages (suppressions, éditions)\n> 🔊 Vocal (connexions, déplacements, mutes)\n> ⚙️ Serveur (paramètres, webhooks, bots)\n> 📁 Salons & threads\n> 🎉 Invitations & emojis",
            color=0x2b2d31
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        await log_to_db('info', f'/logs used by {interaction.user} in {guild.name}')
    except Exception as e:
        logger.error(f"Error in /logs command: {traceback.format_exc()}")
        try:
            await interaction.followup.send("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


@bot.tree.command(name="supplogs", description="Supprimer le salon de logs et réinitialiser la config.")
@app_commands.default_permissions(administrator=True)
async def supplogs_command(interaction: discord.Interaction):
    try:
        if not await check_license(interaction):
            return
        if not await is_bot_owner_or_server_owner(interaction.guild, interaction.user.id):
            await interaction.response.send_message("Seul le propriétaire du bot ou le créateur du serveur peut utiliser cette commande.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild

        category = discord.utils.get(guild.categories, name="RShield - Logs")
        deleted_count = 0
        if category:
            for ch in category.text_channels:
                try:
                    await ch.delete(reason="Shield: /supplogs")
                    deleted_count += 1
                except Exception:
                    pass
            try:
                await category.delete(reason="Shield: /supplogs")
            except Exception:
                pass

        for mod in PROTECTION_MODULES:
            await set_protection(str(guild.id), mod['key'], log_channel_id="")

        embed = discord.Embed(
            description=f"✅ Salon de logs supprimé et configuration réinitialisée.",
            color=0x2b2d31
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        await log_to_db('info', f'/supplogs used by {interaction.user} in {guild.name}')
    except Exception as e:
        logger.error(f"Error in /supplogs command: {traceback.format_exc()}")
        try:
            await interaction.followup.send("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


@bot.tree.command(name="joinleave", description="Configurer les salons de bienvenue et de départ.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    action="Choisir l'action à effectuer",
    channel="Le salon où envoyer les messages"
)
@app_commands.choices(action=[
    app_commands.Choice(name="Définir le salon de bienvenue (join)", value="set_join"),
    app_commands.Choice(name="Définir le salon de départ (leave)", value="set_leave"),
    app_commands.Choice(name="Désactiver le salon de bienvenue", value="remove_join"),
    app_commands.Choice(name="Désactiver le salon de départ", value="remove_leave"),
    app_commands.Choice(name="Voir la configuration actuelle", value="view"),
])
async def joinleave_command(interaction: discord.Interaction, action: app_commands.Choice[str], channel: discord.TextChannel = None):
    try:
        if not await check_license(interaction):
            return
        if not await is_owner_or_ownerlist(interaction.guild, interaction.user.id):
            await interaction.response.send_message("Vous n'êtes pas autorisé à utiliser cette commande.", ephemeral=True)
            return

        guild_id = str(interaction.guild.id)

        if action.value == "set_join":
            if not channel:
                await interaction.response.send_message("Veuillez spécifier un salon.", ephemeral=True)
                return
            if pool:
                await pool.execute(
                    "INSERT INTO guild_join_leave (guild_id, join_channel_id) VALUES ($1, $2) "
                    "ON CONFLICT (guild_id) DO UPDATE SET join_channel_id = $2",
                    guild_id, str(channel.id)
                )
            embed = discord.Embed(description=f"Salon de bienvenue défini sur {channel.mention}.", color=0x2b2d31)
            await interaction.response.send_message(embed=embed, ephemeral=True)

        elif action.value == "set_leave":
            if not channel:
                await interaction.response.send_message("Veuillez spécifier un salon.", ephemeral=True)
                return
            if pool:
                await pool.execute(
                    "INSERT INTO guild_join_leave (guild_id, leave_channel_id) VALUES ($1, $2) "
                    "ON CONFLICT (guild_id) DO UPDATE SET leave_channel_id = $2",
                    guild_id, str(channel.id)
                )
            embed = discord.Embed(description=f"Salon de départ défini sur {channel.mention}.", color=0x2b2d31)
            await interaction.response.send_message(embed=embed, ephemeral=True)

        elif action.value == "remove_join":
            if pool:
                await pool.execute(
                    "UPDATE guild_join_leave SET join_channel_id = NULL WHERE guild_id = $1",
                    guild_id
                )
            embed = discord.Embed(description="Salon de bienvenue désactivé.", color=0x2b2d31)
            await interaction.response.send_message(embed=embed, ephemeral=True)

        elif action.value == "remove_leave":
            if pool:
                await pool.execute(
                    "UPDATE guild_join_leave SET leave_channel_id = NULL WHERE guild_id = $1",
                    guild_id
                )
            embed = discord.Embed(description="Salon de départ désactivé.", color=0x2b2d31)
            await interaction.response.send_message(embed=embed, ephemeral=True)

        elif action.value == "view":
            join_str = "Non configuré"
            leave_str = "Non configuré"
            if pool:
                row = await pool.fetchrow(
                    "SELECT join_channel_id, leave_channel_id FROM guild_join_leave WHERE guild_id = $1",
                    guild_id
                )
                if row:
                    if row['join_channel_id']:
                        ch = interaction.guild.get_channel(int(row['join_channel_id']))
                        join_str = ch.mention if ch else f"ID: {row['join_channel_id']}"
                    if row['leave_channel_id']:
                        ch = interaction.guild.get_channel(int(row['leave_channel_id']))
                        leave_str = ch.mention if ch else f"ID: {row['leave_channel_id']}"
            embed = discord.Embed(
                title="Configuration Join/Leave",
                description=(
                    f"**Salon de bienvenue:** {join_str}\n"
                    f"**Salon de départ:** {leave_str}"
                ),
                color=0x2b2d31
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)

        await log_to_db('info', f'/joinleave {action.value} used by {interaction.user} in {interaction.guild.name}')
    except Exception as e:
        logger.error(f"Error in /joinleave command: {traceback.format_exc()}")
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


@bot.tree.command(name="info", description="Voir les informations d'un utilisateur.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(user="L'utilisateur à rechercher")
async def info_command(interaction: discord.Interaction, user: discord.Member):
    try:
        if not await check_license(interaction):
            return
        if not await can_use_bot(interaction.guild, interaction.user.id):
            await interaction.response.send_message("Vous ne pouvez pas utiliser le bot.", ephemeral=True)
            return

        created = int(user.created_at.timestamp())
        joined = int(user.joined_at.timestamp()) if user.joined_at else 0
        roles = [r.mention for r in user.roles if r.name != "@everyone"]
        roles_str = ", ".join(roles) if roles else "Aucun"

        status_map = {
            discord.Status.online: "🟢 En ligne",
            discord.Status.idle: "🟡 Inactif",
            discord.Status.dnd: "🔴 Ne pas déranger",
            discord.Status.offline: "⚫ Hors ligne",
        }
        status_str = status_map.get(user.status, "⚫ Hors ligne")

        is_bot = "Oui" if user.bot else "Non"
        boosting = f"<t:{int(user.premium_since.timestamp())}:R>" if user.premium_since else "Non"
        nick = user.nick if user.nick else "Aucun"
        top_role = user.top_role.mention if user.top_role and user.top_role.name != "@everyone" else "Aucun"

        badges = []
        if user.public_flags:
            flag_names = {
                'staff': 'Staff Discord',
                'partner': 'Partenaire',
                'hypesquad': 'HypeSquad Events',
                'bug_hunter': 'Bug Hunter',
                'hypesquad_bravery': 'HypeSquad Bravery',
                'hypesquad_brilliance': 'HypeSquad Brilliance',
                'hypesquad_balance': 'HypeSquad Balance',
                'early_supporter': 'Early Supporter',
                'bug_hunter_level_2': 'Bug Hunter Lvl 2',
                'verified_bot_developer': 'Développeur de Bot Vérifié',
                'discord_certified_moderator': 'Modérateur Certifié',
                'active_developer': 'Développeur Actif',
            }
            for flag, name in flag_names.items():
                if getattr(user.public_flags, flag, False):
                    badges.append(name)
        badges_str = ", ".join(badges) if badges else "Aucun"

        perms = []
        if user.guild_permissions.administrator:
            perms.append("Administrateur")
        if user.guild_permissions.manage_guild:
            perms.append("Gérer le serveur")
        if user.guild_permissions.manage_roles:
            perms.append("Gérer les rôles")
        if user.guild_permissions.manage_channels:
            perms.append("Gérer les salons")
        if user.guild_permissions.kick_members:
            perms.append("Expulser")
        if user.guild_permissions.ban_members:
            perms.append("Bannir")
        if user.guild_permissions.manage_messages:
            perms.append("Gérer les messages")
        if user.guild_permissions.mention_everyone:
            perms.append("Mentionner everyone")
        perms_str = ", ".join(perms) if perms else "Aucune permission clé"

        is_owner = "Oui" if user.id == interaction.guild.owner_id else "Non"

        mutual = len(user.mutual_guilds) if hasattr(user, 'mutual_guilds') and user.mutual_guilds else "N/A"

        embed = discord.Embed(
            title=f"Informations sur {user}",
            color=user.color if user.color != discord.Color.default() else 0x2b2d31
        )
        embed.set_thumbnail(url=user.display_avatar.url)
        if user.banner:
            embed.set_image(url=user.banner.url)

        embed.add_field(name="Utilisateur", value=f"{user.mention} (`{user}`)", inline=False)
        embed.add_field(name="ID", value=f"`{user.id}`", inline=True)
        embed.add_field(name="Bot", value=is_bot, inline=True)
        embed.add_field(name="Propriétaire du serveur", value=is_owner, inline=True)
        embed.add_field(name="Surnom", value=nick, inline=True)
        embed.add_field(name="Statut", value=status_str, inline=True)
        embed.add_field(name="Boost", value=boosting, inline=True)
        embed.add_field(name="Compte créé", value=f"<t:{created}:F>\n(<t:{created}:R>)", inline=True)
        embed.add_field(name="A rejoint le serveur", value=f"<t:{joined}:F>\n(<t:{joined}:R>)", inline=True)
        embed.add_field(name="Rôle le plus élevé", value=top_role, inline=True)
        embed.add_field(name="Badges", value=badges_str, inline=False)
        embed.add_field(name="Permissions clés", value=perms_str, inline=False)
        embed.add_field(name=f"Rôles ({len(roles)})", value=roles_str if len(roles_str) <= 1024 else f"{len(roles)} rôles", inline=False)
        embed.set_footer(text=f"Demandé par {interaction.user} • ID: {user.id}")

        await interaction.response.send_message(embed=embed, ephemeral=True)
        await log_to_db('info', f'/info used by {interaction.user} on {user} in {interaction.guild.name}')
    except Exception as e:
        logger.error(f"Error in /info command: {traceback.format_exc()}")
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


@bot.tree.command(name="gerants", description="Afficher les gérants whitelist des factions.")
@app_commands.default_permissions(administrator=True)
async def gerants_command(interaction: discord.Interaction):
    try:
        view = discord.ui.LayoutView(timeout=None)
        container = discord.ui.Container(accent_colour=0x2b2d31)

        gallery = discord.ui.MediaGallery()
        gallery.add_item(media="https://i.imgur.com/nKuTsvY.png")
        container.add_item(gallery)

        container.add_item(discord.ui.TextDisplay(
            "# __Gérants de toutes les factions__\n"
            "> <@1413486076332605481> & <@404799720305983497>"
        ))

        container.add_item(discord.ui.TextDisplay(
            "## AURORS\n"
            "> <@380059243451121664> et <@565773187116302346> & <@484798244996644864>"
        ))
        container.add_item(discord.ui.TextDisplay(
            "## MANGEMORT\n"
            "> <@1413486076332605481> et <@1045815146511081542>"
        ))
        container.add_item(discord.ui.TextDisplay(
            "## MINISTERE\n"
            "> <@665228481654947853>"
        ))
        container.add_item(discord.ui.TextDisplay(
            "## MAGE-INDEPENDANT\n"
            "> <@1413486076332605481> et <@665228481654947853>"
        ))
        container.add_item(discord.ui.TextDisplay(
            "## PROFESSEUR\n"
            "> <@685885648762044449> et <@118006132500463624>"
        ))

        container.add_item(discord.ui.Separator())

        container.add_item(discord.ui.TextDisplay(
            "Les gérants WL restent à votre disposition pour toute question ou demande."
        ))

        view.add_item(container)
        await interaction.response.send_message("✅ Envoyé.", ephemeral=True)
        await interaction.channel.send(view=view)
    except Exception as e:
        logger.error(f"Error in /gerants: {traceback.format_exc()}")
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


@bot.tree.command(name="reglement", description="Afficher le règlement du serveur.")
@app_commands.default_permissions(administrator=True)
async def reglement_command(interaction: discord.Interaction):
    try:
        view = discord.ui.LayoutView(timeout=None)
        container = discord.ui.Container(accent_colour=0x2b2d31)

        gallery = discord.ui.MediaGallery()
        gallery.add_item(media="https://i.imgur.com/q5eMiRV.png")
        container.add_item(gallery)

        container.add_item(discord.ui.TextDisplay(
            "## 📜 RÈGLEMENT FACTION - MSSCLICK 📜"
        ))
        container.add_item(discord.ui.TextDisplay(
            "> Merci de prendre le temps de lire et respecter le règlement du serveur. "
            "Cela permet à tout le monde de profiter d'une expérience de jeu agréable et équilibrée. "
            "Le non-respect des règles peut entraîner des sanctions."
        ))

        container.add_item(discord.ui.Separator())

        reglement_button = discord.ui.Button(
            label="Règlement",
            style=discord.ButtonStyle.link,
            url="https://sites.google.com/view/mssclick-reglement-faction?usp=sharing"
        )
        section = discord.ui.Section(
            "🔗 **Accès au règlement complet :**",
            accessory=reglement_button
        )
        container.add_item(section)

        container.add_item(discord.ui.Separator())

        container.add_item(discord.ui.TextDisplay(
            "✅ **Note :** Pensez à regarder le règlement une fois par semaine afin de vous assurer d'être bien à jour sur celui-ci."
        ))
        container.add_item(discord.ui.TextDisplay(
            "🔴 Ignorer le règlement n'est pas une excuse en cas d'infraction et nous partons du principe qu'en vous connectant sur le serveur vous le maitrisez sur le bout des doigts."
        ))

        view.add_item(container)
        await interaction.response.send_message("✅ Envoyé.", ephemeral=True)
        await interaction.channel.send(view=view)
    except Exception as e:
        logger.error(f"Error in /reglement: {traceback.format_exc()}")
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


@bot.tree.command(name="help", description="Afficher la liste des commandes du bot.")
@app_commands.default_permissions(administrator=True)
async def help_command(interaction: discord.Interaction):
    try:
        if interaction.guild:
            is_ol = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
            if not is_ol:
                await interaction.response.send_message("❌ Seuls les membres de la ownerlist peuvent utiliser cette commande.", ephemeral=True)
                return
        cmd_ids = await get_command_ids(interaction.guild) if interaction.guild else {}
        embed = build_help_embed(cmd_ids)
        view = discord.ui.View()
        support_url = f"https://discord.com/users/{BOT_OWNER_ID}"
        support_button = discord.ui.Button(label="Support", style=discord.ButtonStyle.link, url=support_url)
        view.add_item(support_button)
        await interaction.response.send_message(embed=embed, view=view)
        await log_to_db('info', f'/help used by {interaction.user} in #{interaction.channel}')
    except Exception as e:
        logger.error(f"Error in /help command: {traceback.format_exc()}")
        try:
            await log_to_db('error', f'Error in /help: {e}')
        except Exception:
            pass
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


STATUS_LABELS = {
    "acceptee": ("✅", "Acceptée",   discord.ButtonStyle.success, 0x2ecc71),
    "encours":  ("🔄", "En cours",   discord.ButtonStyle.primary, 0x3498db),
    "refusee":  ("❌", "Refusée",    discord.ButtonStyle.danger,  0xe74c3c),
    "en_attente": ("⏳", "En attente", discord.ButtonStyle.secondary, 0x95a5a6),
}


def _build_suggestion_log_embed(row: dict, votes_yes: int = 0, votes_no: int = 0) -> discord.Embed:
    status_key = row.get("status") or "en_attente"
    status_emoji, status_label, _, status_color = STATUS_LABELS.get(status_key, STATUS_LABELS["en_attente"])
    submitted_ts = int(row["submitted_at"].timestamp()) if row.get("submitted_at") else 0
    submitted_str = f"<t:{submitted_ts}:R>" if submitted_ts else "Date inconnue"

    embed = discord.Embed(
        title=f"💡 {row['nom']}",
        color=status_color,
    )
    embed.add_field(name="Faction :", value=f"**{row['faction']}**", inline=True)
    embed.add_field(name="👤 Proposé par :", value=f"<@{row['user_id']}>", inline=True)
    embed.add_field(name="📅 Soumis :", value=submitted_str, inline=True)
    embed.add_field(name="__**Suggestion :**__", value=row.get("suggestion") or "N/A", inline=False)
    embed.add_field(name="__**Objectif :**__", value=row.get("objectif") or "N/A", inline=False)
    embed.add_field(
        name="📊 Votes",
        value=f"✅ **{votes_yes}** approbation(s)  ·  ❌ **{votes_no}** refus",
        inline=False,
    )
    embed.add_field(
        name="📌 Statut",
        value=f"{status_emoji} **{status_label}**",
        inline=False,
    )
    embed.set_footer(text=f"ID suggestion : {row['message_id']}")
    return embed


class StatusButton(discord.ui.Button):
    def __init__(self, message_id: str, status: str):
        emoji, label, style, _ = STATUS_LABELS[status]
        super().__init__(
            label=label,
            style=style,
            emoji=emoji,
            custom_id=f"sugg_{status}_{message_id}",
        )
        self.message_id = message_id
        self.status = status

    async def callback(self, interaction: discord.Interaction):
        if interaction.guild and not await can_use_bot(interaction.guild, interaction.user.id):
            await interaction.response.send_message(
                "❌ Seuls les membres de la ownerlist peuvent modifier le statut.",
                ephemeral=True
            )
            return

        if not pool:
            await interaction.response.send_message("❌ Base de données non connectée.", ephemeral=True)
            return

        await pool.execute(
            "UPDATE suggestions_log SET status = $1 WHERE message_id = $2",
            self.status, self.message_id
        )

        row = await pool.fetchrow("SELECT * FROM suggestions_log WHERE message_id = $1", self.message_id)
        if not row:
            await interaction.response.send_message("❌ Suggestion introuvable en base.", ephemeral=True)
            return

        votes_yes = votes_no = 0
        try:
            sugg_ch = interaction.guild.get_channel(SUGGESTION_CHANNEL_ID)
            if sugg_ch:
                orig = await sugg_ch.fetch_message(int(self.message_id))
                for r in orig.reactions:
                    if str(r.emoji) == "✅":
                        votes_yes = r.count - 1
                    elif str(r.emoji) == "❌":
                        votes_no = r.count - 1
        except Exception:
            pass

        new_embed = _build_suggestion_log_embed(dict(row), votes_yes, votes_no)
        await interaction.response.edit_message(embed=new_embed)
        await log_to_db('info', f'Statut suggestion {self.message_id} → {self.status} par {interaction.user}')


class SuggestionStatusView(discord.ui.View):
    def __init__(self, message_id: str):
        super().__init__(timeout=None)
        for status in ("acceptee", "encours", "refusee"):
            self.add_item(StatusButton(message_id, status))


async def register_suggestion_views():
    if not pool:
        return
    try:
        rows = await pool.fetch("SELECT message_id FROM suggestions_log")
        for row in rows:
            bot.add_view(SuggestionStatusView(row["message_id"]))
        logger.info(f"Registered {len(rows)} suggestion status views.")
    except Exception as e:
        logger.error(f"Error registering suggestion views: {e}")


SUGGESTION_CHANNEL_ID = 1062740126087774268

FACTIONS = [
    "Mangemort",
    "Auror",
    "Vampire",
    "Ordre du Phénix",
    "Membre du Ministère",
    "Professeurs",
]

FACTION_COLORS = {
    "Mangemort":          0x2b2d31,
    "Auror":              0x1a6ea8,
    "Vampire":            0x8b0000,
    "Ordre du Phénix":    0xe67e22,
    "Membre du Ministère":0x2ecc71,
    "Professeurs":        0x9b59b6,
}

FACTION_EMOJIS = {
    "Mangemort":          "🐍",
    "Auror":              "⚡",
    "Vampire":            "🧛",
    "Ordre du Phénix":    "🔥",
    "Membre du Ministère":"🏛️",
    "Professeurs":        "📚",
}


class SuggestionModal(discord.ui.Modal, title="Créer une suggestion"):
    nom = discord.ui.TextInput(
        label="Nom de la Suggestion",
        placeholder="Entrez un titre court pour votre suggestion…",
        max_length=100,
        required=True,
    )
    suggestion = discord.ui.TextInput(
        label="La Suggestion",
        style=discord.TextStyle.paragraph,
        placeholder="Décrivez votre suggestion en détail…",
        max_length=1000,
        required=True,
    )
    objectif = discord.ui.TextInput(
        label="L'objectif de celle-ci",
        style=discord.TextStyle.paragraph,
        placeholder="Quel est le but de cette suggestion ?",
        max_length=500,
        required=True,
    )

    def __init__(self, faction: str):
        super().__init__()
        self.faction = faction

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        channel = interaction.guild.get_channel(SUGGESTION_CHANNEL_ID)
        if not channel:
            try:
                channel = await interaction.client.fetch_channel(SUGGESTION_CHANNEL_ID)
            except Exception:
                await interaction.followup.send("❌ Channel de suggestions introuvable.", ephemeral=True)
                return

        color = FACTION_COLORS.get(self.faction, 0x2b2d31)

        embed = discord.Embed(
            title=f"💡 {self.nom.value}",
            color=color,
        )
        embed.add_field(
            name="Faction :",
            value=f"**{self.faction}**",
            inline=True,
        )
        embed.add_field(
            name="👤 Proposé par :",
            value=interaction.user.mention,
            inline=True,
        )
        embed.add_field(
            name="__**Suggestion :**__",
            value=self.suggestion.value,
            inline=False,
        )
        embed.add_field(
            name="__**Objectif :**__",
            value=self.objectif.value,
            inline=False,
        )
        embed.set_footer(text="Votez avec ✅ pour approuver ou ❌ pour refuser.")
        embed.set_thumbnail(url=interaction.user.display_avatar.url)

        try:
            msg = await channel.send(embed=embed)
            await msg.add_reaction("✅")
            await msg.add_reaction("❌")
            if pool:
                try:
                    await pool.execute(
                        """INSERT INTO suggestions_log
                           (guild_id, message_id, channel_id, nom, faction, user_id, user_name, suggestion, objectif)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""",
                        str(interaction.guild.id),
                        str(msg.id),
                        str(channel.id),
                        self.nom.value,
                        self.faction,
                        str(interaction.user.id),
                        str(interaction.user),
                        self.suggestion.value,
                        self.objectif.value,
                    )
                except Exception as db_err:
                    logger.error(f"Erreur sauvegarde suggestion en DB : {db_err}")
            await interaction.followup.send("✅ Ta suggestion a bien été envoyée !", ephemeral=True)
            await log_to_db('info', f'Suggestion créée par {interaction.user} dans {interaction.guild.name}')
        except Exception as e:
            logger.error(f"Erreur lors de l'envoi de la suggestion : {e}")
            await interaction.followup.send("❌ Une erreur est survenue lors de l'envoi.", ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"Erreur dans SuggestionModal : {error}")
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ Une erreur est survenue.", ephemeral=True)
            else:
                await interaction.followup.send("❌ Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


class FactionSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(
                label=faction,
                emoji=FACTION_EMOJIS[faction],
            )
            for faction in FACTIONS
        ]
        super().__init__(
            placeholder="Choisissez votre faction…",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        faction = self.values[0]
        modal = SuggestionModal(faction=faction)
        await interaction.response.send_modal(modal)


class FactionSelectView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=120)
        self.add_item(FactionSelect())


class SuggestionButtonView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Créer une suggestion", style=discord.ButtonStyle.primary, emoji="💡", custom_id="suggestion_open")
    async def open_suggestion(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = FactionSelectView()
        await interaction.response.send_message(
            "**Sélectionnez votre faction** avant de remplir le formulaire :",
            view=view,
            ephemeral=True,
        )


@bot.tree.command(name="suggestions", description="Affiche le panneau de suggestions du serveur.")
@app_commands.default_permissions(administrator=True)
async def cmd_suggestions(interaction: discord.Interaction):
    if interaction.guild and not await can_use_bot(interaction.guild, interaction.user.id):
        await interaction.response.send_message(
            "❌ Seuls les membres de la ownerlist peuvent utiliser cette commande.",
            ephemeral=True
        )
        return
    embed = discord.Embed(
        title="📋 Foire aux Questions — Suggestions",
        description=(
            "Vous avez une idée pour améliorer le serveur ?\n"
            "Cliquez sur le bouton ci-dessous pour soumettre votre suggestion.\n\n"
            "Vos propositions sont précieuses ! Chaque suggestion sera lue et évaluée "
            "par l'équipe du serveur. Votez également sur les suggestions des autres "
            "membres avec ✅ ou ❌.\n\n"
            "Merci de votre participation"
        ),
        color=0x5865F2,
    )
    view = SuggestionButtonView()
    await interaction.response.send_message(embed=embed, view=view)


@bot.tree.command(name="logssuggestions", description="Affiche les logs des suggestions dans un channel admin.")
@app_commands.default_permissions(administrator=True)
async def cmd_logssuggestions(interaction: discord.Interaction):
    if interaction.guild and not await can_use_bot(interaction.guild, interaction.user.id):
        await interaction.response.send_message(
            "❌ Seuls les membres de la ownerlist peuvent utiliser cette commande.",
            ephemeral=True
        )
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild

    try:
        CATEGORY_NAME = "Faction - Logs"
        CHANNEL_NAME = "logs-suggestions"

        category = discord.utils.get(guild.categories, name=CATEGORY_NAME)
        if not category:
            overwrites_cat = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
            }
            for role in guild.roles:
                if role.permissions.administrator or role.permissions.manage_guild:
                    overwrites_cat[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
            category = await guild.create_category(CATEGORY_NAME, overwrites=overwrites_cat)

        log_channel = discord.utils.get(guild.text_channels, name=CHANNEL_NAME, category=category)
        if not log_channel:
            overwrites_ch = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
            }
            for role in guild.roles:
                if role.permissions.administrator or role.permissions.manage_guild:
                    overwrites_ch[role] = discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=False,
                        read_message_history=True,
                    )
            log_channel = await guild.create_text_channel(
                CHANNEL_NAME,
                category=category,
                overwrites=overwrites_ch,
                topic="Logs des suggestions soumises via /suggestions",
            )

        if not pool:
            await interaction.followup.send("❌ Base de données non connectée.", ephemeral=True)
            return

        rows = await pool.fetch(
            "SELECT * FROM suggestions_log WHERE guild_id = $1 ORDER BY submitted_at DESC LIMIT 25",
            str(guild.id)
        )

        if not rows:
            await log_channel.send("📭 Aucune suggestion enregistrée pour le moment.")
            await interaction.followup.send(f"✅ Logs postés dans {log_channel.mention} (aucune suggestion trouvée).", ephemeral=True)
            return

        header = discord.Embed(
            title="📋 Logs des Suggestions",
            description=f"**{len(rows)}** suggestion(s) enregistrée(s) — triées de la plus récente à la plus ancienne.",
            color=0x5865F2,
        )
        header.set_footer(text=f"Serveur : {guild.name}")
        header.timestamp = datetime.datetime.utcnow()
        await log_channel.send(embed=header)

        sugg_channel_obj = guild.get_channel(SUGGESTION_CHANNEL_ID)

        for row in rows:
            votes_yes = 0
            votes_no = 0
            try:
                if sugg_channel_obj:
                    msg_obj = await sugg_channel_obj.fetch_message(int(row["message_id"]))
                    for reaction in msg_obj.reactions:
                        if str(reaction.emoji) == "✅":
                            votes_yes = reaction.count - 1
                        elif str(reaction.emoji) == "❌":
                            votes_no = reaction.count - 1
            except Exception:
                pass

            embed = _build_suggestion_log_embed(dict(row), votes_yes, votes_no)
            view = SuggestionStatusView(row["message_id"])
            await log_channel.send(embed=embed, view=view)

        await interaction.followup.send(
            f"✅ **{len(rows)}** suggestion(s) postée(s) dans {log_channel.mention}.",
            ephemeral=True,
        )
        await log_to_db('info', f'/logssuggestions utilisé par {interaction.user} dans {guild.name}')

    except discord.Forbidden:
        await interaction.followup.send("❌ Je n'ai pas les permissions nécessaires pour créer des channels.", ephemeral=True)
    except Exception as e:
        logger.error(f"Erreur dans /logssuggestions: {e}\n{traceback.format_exc()}")
        await interaction.followup.send("❌ Une erreur est survenue.", ephemeral=True)


RECENSEMENT_CHANNEL_ID = 1182401421040160905
CAPTURE_VALIDATOR_ID = 1413486076332605481


def _extract_user_id_from_mention(text: str) -> str | None:
    m = re.search(r'<@!?(\d+)>', text)
    return m.group(1) if m else None


async def _get_capture_number(guild_id: str, victime_raw: str) -> int:
    if not pool:
        return 1
    victim_id = _extract_user_id_from_mention(victime_raw)
    try:
        if victim_id:
            count = await pool.fetchval(
                "SELECT COUNT(*) FROM recensement WHERE guild_id = $1 AND victime LIKE $2",
                guild_id, f"%{victim_id}%"
            )
        else:
            count = await pool.fetchval(
                "SELECT COUNT(*) FROM recensement WHERE guild_id = $1 AND LOWER(victime) = LOWER($2)",
                guild_id, victime_raw.strip()
            )
        return int(count or 0) + 1
    except Exception:
        return 1


class CaptureValidationView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Valider",
        style=discord.ButtonStyle.success,
        emoji="✅",
        custom_id="cap_approve",
    )
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != CAPTURE_VALIDATOR_ID:
            await interaction.response.send_message("❌ Tu n'es pas autorisé à valider les captures.", ephemeral=True)
            return
        if not pool:
            await interaction.response.send_message("❌ Base de données non connectée.", ephemeral=True)
            return

        await interaction.response.defer()

        row = await pool.fetchrow(
            "SELECT * FROM recensement_pending WHERE message_id = $1",
            str(interaction.message.id)
        )
        if not row:
            await interaction.followup.send("❌ Capture introuvable ou déjà traitée.", ephemeral=True)
            return

        await pool.execute(
            """INSERT INTO recensement
               (guild_id, message_id, channel_id, user_id, user_name,
                date_event, lieu, victime, agresseur, action_resume,
                echanger_contre, capture_numero)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)""",
            row["guild_id"], row["message_id"], row["channel_id"],
            row["user_id"], row["user_name"],
            row["date_event"], row["lieu"], row["victime"],
            row["agresseur"], row["action_resume"],
            row["echanger_contre"], row["capture_numero"],
        )
        await pool.execute(
            "DELETE FROM recensement_pending WHERE message_id = $1",
            str(interaction.message.id)
        )

        embed = interaction.message.embeds[0] if interaction.message.embeds else discord.Embed()
        old_footer = embed.footer.text or ""
        base = old_footer.split(" · ⏳")[0].split(" · ✅")[0]
        embed.set_footer(text=f"{base} · ✅ Validée par {interaction.user.display_name}")
        await interaction.message.edit(embed=embed, view=None)
        await log_to_db('info', f'Capture validée par {interaction.user} (msg {interaction.message.id})')

    @discord.ui.button(
        label="Refuser",
        style=discord.ButtonStyle.danger,
        emoji="❌",
        custom_id="cap_reject",
    )
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != CAPTURE_VALIDATOR_ID:
            await interaction.response.send_message("❌ Tu n'es pas autorisé à refuser les captures.", ephemeral=True)
            return
        if not pool:
            await interaction.response.send_message("❌ Base de données non connectée.", ephemeral=True)
            return

        await interaction.response.defer()

        await pool.execute(
            "DELETE FROM recensement_pending WHERE message_id = $1",
            str(interaction.message.id)
        )
        await interaction.message.delete()
        await log_to_db('info', f'Capture refusée par {interaction.user} (msg {interaction.message.id})')


class RecensementModal(discord.ui.Modal, title="Recensement de capture"):
    date_event = discord.ui.TextInput(
        label="Date",
        placeholder="Ex : 01/05/2026 à 16h30",
        max_length=100,
        required=True,
    )
    lieu = discord.ui.TextInput(
        label="Lieu",
        placeholder="Ex : Forêt interdite, Pré-au-lard…",
        max_length=150,
        required=True,
    )
    agresseur = discord.ui.TextInput(
        label="Agresseur",
        placeholder="Nom du personnage agresseur",
        max_length=150,
        required=True,
    )
    action_resume = discord.ui.TextInput(
        label="L'action (résumé)",
        style=discord.TextStyle.paragraph,
        placeholder="Décrivez brièvement l'action commise…",
        max_length=500,
        required=True,
    )
    echanger_contre = discord.ui.TextInput(
        label="Echanger contre",
        placeholder="Que souhaitez-vous en échange ? (optionnel)",
        style=discord.TextStyle.paragraph,
        max_length=300,
        required=False,
    )

    def __init__(self, victim: discord.Member, est_recrue: bool = False):
        super().__init__()
        self._victim = victim
        self._est_recrue = est_recrue

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild

        if not pool and not self._est_recrue:
            await interaction.followup.send("❌ Base de données non connectée.", ephemeral=True)
            return

        channel = guild.get_channel(RECENSEMENT_CHANNEL_ID)
        if not channel:
            try:
                channel = await bot.fetch_channel(RECENSEMENT_CHANNEL_ID)
            except Exception:
                await interaction.followup.send("❌ Salon de recensement introuvable.", ephemeral=True)
                return

        victim_id = str(self._victim.id)
        victime_display = self._victim.mention
        echanger = self.echanger_contre.value or "—"

        if self._est_recrue:
            embed = discord.Embed(
                title="📋 Recensement de capture — RECRUE",
                description="⚠️ **La victime est une recrue de sa faction.**\nCette capture **n'est pas comptabilisée** par le bot.",
                color=0xE67E22,
                timestamp=datetime.datetime.utcnow(),
            )
            embed.add_field(name="__• Date :__", value=self.date_event.value or "—", inline=False)
            embed.add_field(name="__• Lieu :__", value=self.lieu.value or "—", inline=False)
            embed.add_field(name="__• Victime :__", value=victime_display, inline=False)
            embed.add_field(name="__• Agresseur :__", value=self.agresseur.value or "—", inline=False)
            embed.add_field(name="__• L'action (résumé) :__", value=self.action_resume.value or "—", inline=False)
            embed.add_field(name="__• Echanger contre :__", value=echanger, inline=False)
            embed.set_footer(text=f"Soumis par {interaction.user} • {interaction.user.id} · 🚫 Non comptabilisée (recrue)")

            try:
                await channel.send(embed=embed)
                await interaction.followup.send(
                    "✅ Recensement envoyé. La victime étant une recrue, **la capture n'est pas comptabilisée**.",
                    ephemeral=True,
                )
                await log_to_db('info', f'Recensement RECRUE (non comptabilisé) envoyé par {interaction.user} dans {guild.name}')
            except Exception as e:
                logger.error(f"Erreur envoi recensement recrue : {e}\n{traceback.format_exc()}")
                await interaction.followup.send("❌ Une erreur est survenue lors de l'envoi.", ephemeral=True)
            return

        try:
            count = await pool.fetchval(
                "SELECT COUNT(*) FROM recensement WHERE guild_id = $1 AND victime LIKE $2",
                str(guild.id), f"%{victim_id}%"
            ) or 0
            capture_num = int(count) + 1
        except Exception:
            capture_num = 1

        embed = discord.Embed(
            title="📋 Recensement de capture",
            color=0x2b2d31,
            timestamp=datetime.datetime.utcnow(),
        )
        embed.add_field(name="__• Date :__", value=self.date_event.value or "—", inline=False)
        embed.add_field(name="__• Lieu :__", value=self.lieu.value or "—", inline=False)
        embed.add_field(name="__• Victime :__", value=victime_display, inline=False)
        embed.add_field(name="__• Agresseur :__", value=self.agresseur.value or "—", inline=False)
        embed.add_field(name="__• L'action (résumé) :__", value=self.action_resume.value or "—", inline=False)
        embed.add_field(name="__• Echanger contre :__", value=echanger, inline=False)
        embed.add_field(name="__• Capture numéro :__", value=str(capture_num), inline=False)
        embed.set_footer(text=f"Soumis par {interaction.user} • {interaction.user.id} · ⏳ En attente de validation")

        msg = None
        try:
            msg = await channel.send(embed=embed, view=CaptureValidationView())
            await pool.execute(
                """INSERT INTO recensement_pending
                   (guild_id, message_id, channel_id, user_id, user_name,
                    date_event, lieu, victime, agresseur, action_resume,
                    echanger_contre, capture_numero)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)""",
                str(guild.id), str(msg.id), str(channel.id),
                str(interaction.user.id), str(interaction.user),
                self.date_event.value, self.lieu.value, victime_display,
                self.agresseur.value, self.action_resume.value,
                self.echanger_contre.value, str(capture_num),
            )
            await interaction.followup.send(
                f"✅ Recensement soumis ! Il sera enregistré une fois validé.", ephemeral=True
            )
            await log_to_db('info', f'Recensement pending #{capture_num} créé par {interaction.user} dans {guild.name}')
        except Exception as e:
            logger.error(f"Erreur envoi recensement : {e}\n{traceback.format_exc()}")
            if msg is not None:
                try:
                    await msg.delete()
                except Exception:
                    pass
            await interaction.followup.send(
                "❌ Une erreur est survenue lors de l'enregistrement. Recensement annulé, merci de réessayer.",
                ephemeral=True,
            )

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"Erreur RecensementModal : {error}\n{traceback.format_exc()}")
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ Une erreur est survenue.", ephemeral=True)
            else:
                await interaction.followup.send("❌ Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


class RecrueChoiceView(discord.ui.View):
    def __init__(self, victim: discord.Member):
        super().__init__(timeout=120)
        self._victim = victim

    @discord.ui.button(label="Oui (recrue)", style=discord.ButtonStyle.danger, emoji="⚠️")
    async def yes_recrue(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RecensementModal(victim=self._victim, est_recrue=True))

    @discord.ui.button(label="Non", style=discord.ButtonStyle.success, emoji="✅")
    async def no_recrue(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RecensementModal(victim=self._victim, est_recrue=False))

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class VictimSelectView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=120)

    @discord.ui.select(
        cls=discord.ui.UserSelect,
        placeholder="Recherchez et sélectionnez la victime…",
        min_values=1,
        max_values=1,
    )
    async def select_victim(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        try:
            victim = select.values[0]
            view = RecrueChoiceView(victim=victim)
            await interaction.response.send_message(
                f"**La victime {victim.mention} est-elle une recrue de sa faction ?**\n"
                "• **Oui** → l'embed sera envoyé mais **la capture ne sera pas comptabilisée**.\n"
                "• **Non** → recensement normal (validation requise).",
                view=view,
                ephemeral=True,
            )
        except Exception as e:
            logger.error(f"Erreur VictimSelectView : {e}\n{traceback.format_exc()}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ Une erreur est survenue.", ephemeral=True)
                else:
                    await interaction.followup.send("❌ Une erreur est survenue.", ephemeral=True)
            except Exception:
                pass

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class RecensementButtonView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Soumettre un recensement",
        style=discord.ButtonStyle.danger,
        emoji="📋",
        custom_id="recensement_open",
    )
    async def open_recensement(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = VictimSelectView()
        await interaction.response.send_message(
            "**Étape préliminaire — Sélectionnez la victime :**\n"
            "Recherchez son nom dans la liste ci-dessous, puis le formulaire s'ouvrira automatiquement.",
            view=view,
            ephemeral=True,
        )


@bot.tree.command(name="recpanel", description="Envoyer le panneau de recensement de captures dans ce salon.")
@app_commands.default_permissions(administrator=True)
async def cmd_recpanel(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    embed = discord.Embed(
        title="📋 Recensement de Captures",
        description=(
            "Vous avez une capture à signaler ?\n"
            "Cliquez sur le bouton ci-dessous pour commencer.\n\n"
            "**Déroulement :**\n"
            "**Étape 1 —** Sélectionnez la victime dans la liste des membres\n"
            "**Étape 2 —** Remplissez le formulaire : Date · Lieu · Agresseur · L'action · Echanger contre\n\n"
            "La victime sera mentionnée et le numéro de capture attribué **automatiquement**.\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        color=0x2b2d31,
    )
    embed.set_footer(text="Les recensements seront publiés dans le salon dédié.")

    view = RecensementButtonView()
    await interaction.channel.send(embed=embed, view=view)
    await interaction.followup.send(
        f"✅ Panneau posté dans {interaction.channel.mention}.\nLes soumissions seront publiées dans <#{RECENSEMENT_CHANNEL_ID}>.",
        ephemeral=True,
    )
    await log_to_db('info', f'/recpanel utilisé par {interaction.user} dans {interaction.guild.name}')


captures_group = app_commands.Group(
    name="admincap",
    description="Gérer les captures du serveur (ownerlist/whitelist).",
)


async def _admincap_check(interaction: discord.Interaction) -> bool:
    if not interaction.guild:
        await interaction.response.send_message("❌ Commande utilisable uniquement sur un serveur.", ephemeral=True)
        return False
    user_id = interaction.user.id
    if interaction.user.guild_permissions.administrator:
        return True
    if await is_owner_or_ownerlist(interaction.guild, user_id):
        return True
    if await is_whitelisted(interaction.guild, user_id):
        return True
    await interaction.response.send_message(
        "❌ Seuls les administrateurs, l'ownerlist et la whitelist peuvent utiliser `/admincap`.",
        ephemeral=True,
    )
    return False


captures_group.interaction_check = _admincap_check


async def admincap_member_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    if not interaction.guild:
        return []
    members = interaction.guild.members
    current_lower = current.lower()
    results = []
    for m in members:
        if current_lower in m.display_name.lower() or current_lower in m.name.lower():
            label = f"{m.display_name} ({m.name})"[:100]
            results.append(app_commands.Choice(name=label, value=str(m.id)))
        if len(results) >= 25:
            break
    return results


@captures_group.command(name="voir", description="Voir toutes les captures d'un membre.")
@app_commands.describe(membre="Tapez le nom du membre")
@app_commands.autocomplete(membre=admincap_member_autocomplete)
async def captures_voir(interaction: discord.Interaction, membre: str):
    await interaction.response.defer(ephemeral=True)

    member_obj = interaction.guild.get_member(int(membre)) if membre.isdigit() else None
    membre_display = member_obj.display_name if member_obj else f"ID {membre}"

    if not pool:
        await interaction.followup.send("❌ Base de données non connectée.", ephemeral=True)
        return

    rows = await pool.fetch(
        "SELECT *, 'validée' AS statut FROM recensement WHERE guild_id = $1 AND victime LIKE $2 "
        "UNION ALL "
        "SELECT *, 'en attente' AS statut FROM recensement_pending WHERE guild_id = $3 AND victime LIKE $4 "
        "ORDER BY submitted_at ASC",
        str(interaction.guild.id), f"%{membre}%",
        str(interaction.guild.id), f"%{membre}%",
    )

    if not rows:
        await interaction.followup.send(
            f"Aucune capture trouvée pour **{membre_display}**.", ephemeral=True
        )
        return

    mention_str = f"<@{membre}>" if membre.isdigit() else membre_display
    validees = [r for r in rows if r["statut"] == "validée"]
    en_attente = [r for r in rows if r["statut"] == "en attente"]
    embed = discord.Embed(
        title=f"📋 Captures de {membre_display}",
        description=(
            f"{mention_str} — **{len(validees)}** validée(s) · **{len(en_attente)}** en attente"
        ),
        color=0x2b2d31,
        timestamp=datetime.datetime.utcnow(),
    )

    for row in rows[:25]:
        date_str = row["date_event"] or "—"
        lieu_str = row["lieu"] or "—"
        agresseur_str = row["agresseur"] or "—"
        action_str = (row["action_resume"] or "—")[:80] + ("…" if len(row["action_resume"] or "") > 80 else "")
        echanger_str = row["echanger_contre"] or "—"
        submitted = f"<t:{int(row['submitted_at'].timestamp())}:d>" if row.get("submitted_at") else "—"
        badge = "✅" if row["statut"] == "validée" else "⏳"

        embed.add_field(
            name=f"__{badge} Capture n°{row['capture_numero']} — ID DB : `{row['id']}`__",
            value=(
                f"**Date :** {date_str} · **Lieu :** {lieu_str}\n"
                f"**Agresseur :** {agresseur_str}\n"
                f"**Action :** {action_str}\n"
                f"**Échange :** {echanger_str} · **Soumis :** {submitted}"
            ),
            inline=False,
        )

    if len(rows) > 25:
        embed.set_footer(text=f"Affichage limité aux 25 premières captures sur {len(rows)}.")

    await interaction.followup.send(embed=embed, ephemeral=True)


@captures_group.command(name="supprimer", description="Supprimer une capture par son ID de base de données.")
@app_commands.describe(capture_id="L'ID de la capture (visible avec /captures voir)")
async def captures_supprimer(interaction: discord.Interaction, capture_id: int):
    await interaction.response.defer(ephemeral=True)

    if not pool:
        await interaction.followup.send("❌ Base de données non connectée.", ephemeral=True)
        return

    row = await pool.fetchrow(
        "SELECT * FROM recensement WHERE id = $1 AND guild_id = $2",
        capture_id, str(interaction.guild.id)
    )
    table_used = "recensement"
    if not row:
        row = await pool.fetchrow(
            "SELECT * FROM recensement_pending WHERE id = $1 AND guild_id = $2",
            capture_id, str(interaction.guild.id)
        )
        table_used = "recensement_pending"
    if not row:
        await interaction.followup.send(
            f"❌ Aucune capture avec l'ID `{capture_id}` sur ce serveur (ni validée ni en attente).", ephemeral=True
        )
        return

    await pool.execute(f"DELETE FROM {table_used} WHERE id = $1", capture_id)

    if row.get("message_id") and row.get("channel_id"):
        try:
            ch = interaction.guild.get_channel(int(row["channel_id"]))
            if not ch:
                ch = await bot.fetch_channel(int(row["channel_id"]))
            msg = await ch.fetch_message(int(row["message_id"]))
            await msg.delete()
        except Exception:
            pass

    statut = "en attente" if table_used == "recensement_pending" else "validée"
    embed = discord.Embed(
        description=(
            f"✅ Capture n°**{row['capture_numero']}** (ID `{capture_id}`, {statut}) supprimée.\n"
            f"Victime : {row['victime'] or '—'} · Date : {row['date_event'] or '—'}"
        ),
        color=0x2b2d31,
    )
    await interaction.followup.send(embed=embed, ephemeral=True)
    await log_to_db('info', f'Capture #{capture_id} supprimée par {interaction.user} dans {interaction.guild.name}')


class CaptureAddModal(discord.ui.Modal, title="Ajouter une capture manuellement"):
    date_event = discord.ui.TextInput(
        label="Date",
        placeholder="Ex : 01/05/2026 à 16h30",
        max_length=100,
        required=True,
    )
    lieu = discord.ui.TextInput(
        label="Lieu",
        placeholder="Ex : Forêt interdite, Pré-au-lard…",
        max_length=150,
        required=True,
    )
    agresseur = discord.ui.TextInput(
        label="Agresseur",
        placeholder="Nom du personnage agresseur",
        max_length=150,
        required=True,
    )
    action_resume = discord.ui.TextInput(
        label="L'action (résumé)",
        style=discord.TextStyle.paragraph,
        placeholder="Décrivez brièvement l'action commise…",
        max_length=500,
        required=True,
    )
    echanger_contre = discord.ui.TextInput(
        label="Echanger contre",
        placeholder="Optionnel",
        max_length=300,
        required=False,
    )

    def __init__(self, victim: discord.Member):
        super().__init__()
        self._victim = victim

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild

        if not pool:
            await interaction.followup.send("❌ Base de données non connectée.", ephemeral=True)
            return

        victim_id = str(self._victim.id)
        victime_display = self._victim.mention

        try:
            count = await pool.fetchval(
                "SELECT COUNT(*) FROM recensement WHERE guild_id = $1 AND victime LIKE $2",
                str(guild.id), f"%{victim_id}%"
            ) or 0
            capture_num = int(count) + 1
        except Exception:
            capture_num = 1

        echanger = self.echanger_contre.value or "—"

        try:
            await pool.execute(
                """INSERT INTO recensement
                   (guild_id, message_id, channel_id, user_id, user_name,
                    date_event, lieu, victime, agresseur, action_resume,
                    echanger_contre, capture_numero)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)""",
                str(guild.id), None, None,
                str(interaction.user.id), str(interaction.user),
                self.date_event.value, self.lieu.value, victime_display,
                self.agresseur.value, self.action_resume.value,
                echanger, str(capture_num),
            )
            await interaction.followup.send(
                f"✅ Capture n°**{capture_num}** enregistrée pour {self._victim.mention}.",
                ephemeral=True
            )
            await log_to_db('info', f'Capture #{capture_num} ajoutée manuellement par {interaction.user} dans {guild.name}')
        except Exception as e:
            logger.error(f"Erreur ajout capture manuelle : {e}\n{traceback.format_exc()}")
            await interaction.followup.send("❌ Une erreur est survenue lors de l'ajout.", ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"Erreur CaptureAddModal : {error}\n{traceback.format_exc()}")
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ Une erreur est survenue.", ephemeral=True)
            else:
                await interaction.followup.send("❌ Une erreur est survenue.", ephemeral=True)
        except Exception:
            pass


@captures_group.command(name="ajouter", description="Ajouter une capture manuellement pour un membre.")
@app_commands.describe(membre="Tapez le nom du membre")
@app_commands.autocomplete(membre=admincap_member_autocomplete)
async def captures_ajouter(interaction: discord.Interaction, membre: str):
    member_obj = interaction.guild.get_member(int(membre)) if membre.isdigit() else None
    if not member_obj:
        await interaction.response.send_message("❌ Membre introuvable. Veuillez sélectionner un membre dans la liste.", ephemeral=True)
        return
    await interaction.response.send_modal(CaptureAddModal(victim=member_obj))


bot.tree.add_command(captures_group)


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    logger.error(f"App command error: {error}\n{traceback.format_exc()}")
    try:
        await log_to_db('error', f'App command error: {error}')
    except Exception:
        pass
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message("Une erreur est survenue.", ephemeral=True)
        else:
            await interaction.followup.send("Une erreur est survenue.", ephemeral=True)
    except Exception:
        pass




@tasks.loop(seconds=5)
async def process_outgoing_messages():
    if not pool:
        return
    try:
        rows = await pool.fetch(
            "SELECT id, channel_id, content FROM outgoing_messages WHERE status = 'pending' LIMIT 5"
        )
        for row in rows:
            msg_id = row['id']
            try:
                channel_id = int(row['channel_id'])
            except ValueError:
                await pool.execute(
                    "UPDATE outgoing_messages SET status = 'failed', processed_at = NOW() WHERE id = $1",
                    msg_id
                )
                await log_to_db('error', f"Invalid channel ID: {row['channel_id']}")
                continue

            try:
                channel = bot.get_channel(channel_id)
                if not channel:
                    channel = await bot.fetch_channel(channel_id)
                await channel.send(row['content'])
                await pool.execute(
                    "UPDATE outgoing_messages SET status = 'sent', processed_at = NOW() WHERE id = $1",
                    msg_id
                )
                await log_to_db('info', f'Sent message to channel {channel_id}')
            except Exception as e:
                await pool.execute(
                    "UPDATE outgoing_messages SET status = 'failed', processed_at = NOW() WHERE id = $1",
                    msg_id
                )
                await log_to_db('error', f'Error sending to channel {channel_id}: {e}')
    except Exception as e:
        logger.error(f"Error in outgoing messages loop: {e}")


TICKET_FACTIONS = {
    "mangemort": {
        "name": "Mangemort", "emoji": "💀", "prefix": "ᴍᴍ〡",
        "category_id": 1399141637875040326,
        "role_id": 1399144149579731164,
        "color": 0x8B0000,
    },
    "auror": {
        "name": "Auror", "emoji": "⚖️", "prefix": "ᴀᴜʀᴏʀ〡",
        "category_id": 1399141885695492156,
        "role_id": 1399144243381010442,
        "color": 0x1F6FEB,
    },
    "ministere": {
        "name": "Ministère", "emoji": "🏛️", "prefix": "ᴍᴅᴍ〡",
        "category_id": 1399142119494385686,
        "role_id": 1399144244169543791,
        "color": 0x6F42C1,
    },
    "vampire": {
        "name": "Vampire", "emoji": "🧛", "prefix": "𝗏𝖺𝗆𝗉𝗂𝗋𝖾〡",
        "category_id": 1399142051295137963,
        "role_id": 1399144243003527389,
        "color": 0x4A0E0E,
    },
    "ordre": {
        "name": "Ordre du Phénix", "emoji": "🦅", "prefix": "ᴏᴅᴘ〡",
        "category_id": 1440032753721807000,
        "role_id": 1440000934884409384,
        "color": 0xE67E22,
    },
    "mage": {
        "name": "Mage Indépendant/Autres", "emoji": "🔮", "prefix": "ᴍɪ〡",
        "category_id": 1374819444043419648,
        "role_id": 1399144394694852670,
        "color": 0x2b2d31,
    },
}

FICHE_SUIVI_TEXT = (
    "**══ Fiche de Suivi ══**\n\n"
    "- Nom, Prénom, SteamID\n"
    "- Histoire personnelle\n"
    "- Vos objectifs personnels\n\n"
    "**══ Modèle à suivre ══**"
)


class TicketCreateButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r'ticket_create:(?P<faction>[a-z]+)'
):
    def __init__(self, faction_key: str):
        self.faction_key = faction_key
        super().__init__(discord.ui.Button(
            label="Créer votre ticket",
            style=discord.ButtonStyle.danger,
            emoji="📩",
            custom_id=f"ticket_create:{faction_key}",
        ))

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(match['faction'])

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        faction = TICKET_FACTIONS.get(self.faction_key)
        if not faction:
            await interaction.followup.send("❌ Faction inconnue.", ephemeral=True)
            return

        guild = interaction.guild
        if not guild:
            await interaction.followup.send("❌ Action impossible en MP.", ephemeral=True)
            return

        category = guild.get_channel(faction["category_id"])
        if not category or not isinstance(category, discord.CategoryChannel):
            await interaction.followup.send(
                "❌ Catégorie de la faction introuvable sur ce serveur. Préviens un administrateur.",
                ephemeral=True,
            )
            return

        role = guild.get_role(faction["role_id"])
        if not role:
            await interaction.followup.send(
                "❌ Rôle des gérants de la faction introuvable. Préviens un administrateur.",
                ephemeral=True,
            )
            return

        user_marker = f"uid={interaction.user.id}"
        existing = discord.utils.find(
            lambda c: c.category_id == faction["category_id"]
                      and c.topic and user_marker in c.topic,
            guild.text_channels
        )
        if existing:
            await interaction.followup.send(
                f"❌ Tu as déjà un ticket ouvert pour cette faction : {existing.mention}",
                ephemeral=True,
            )
            return

        display = getattr(interaction.user, "display_name", None) or interaction.user.name
        safe_name = ''.join(c for c in display.lower() if c.isalnum() or c == '-')[:25] or "user"
        chan_name = f"{faction['prefix']}{safe_name}"[:90]

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            interaction.user: discord.PermissionOverwrite(
                view_channel=True, send_messages=True,
                read_message_history=True, attach_files=True, embed_links=True,
            ),
            guild.me: discord.PermissionOverwrite(
                view_channel=True, send_messages=True,
                manage_channels=True, read_message_history=True,
                manage_messages=True, embed_links=True,
            ),
            role: discord.PermissionOverwrite(
                view_channel=True, send_messages=True,
                read_message_history=True, attach_files=True, embed_links=True,
            ),
        }

        viewer_role = guild.get_role(1062740125630603377)
        if viewer_role:
            overwrites[viewer_role] = discord.PermissionOverwrite(
                view_channel=True, send_messages=False,
                read_message_history=True, add_reactions=True,
            )

        try:
            channel = await guild.create_text_channel(
                name=chan_name,
                category=category,
                overwrites=overwrites,
                topic=f"Ticket de {interaction.user} ({user_marker}) — {faction['name']}",
                reason=f"Ticket créé par {interaction.user}",
            )
        except discord.Forbidden:
            await interaction.followup.send(
                "❌ Le bot n'a pas la permission de créer des salons dans cette catégorie.",
                ephemeral=True,
            )
            return
        except Exception as e:
            logger.error(f"Erreur création ticket : {e}\n{traceback.format_exc()}")
            await interaction.followup.send("❌ Impossible de créer le ticket.", ephemeral=True)
            return

        embed = discord.Embed(
            title=f"{faction['emoji']} Ticket {faction['name']}",
            description=FICHE_SUIVI_TEXT,
            color=faction["color"],
            timestamp=datetime.datetime.utcnow(),
        )
        embed.set_footer(text=f"Ticket ouvert par {interaction.user}")

        try:
            await channel.send(content=interaction.user.mention, embed=embed)
        except Exception as e:
            logger.error(f"Erreur envoi message ticket : {e}")

        await interaction.followup.send(f"✅ Ticket créé : {channel.mention}", ephemeral=True)
        try:
            await log_to_db('info', f'Ticket {faction["name"]} créé par {interaction.user} dans {guild.name}')
        except Exception:
            pass


@bot.tree.command(name="ticketpanel", description="Envoyer le panneau de création de ticket pour une faction.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(faction="Choisir la faction")
@app_commands.choices(faction=[
    app_commands.Choice(name=f["name"], value=k) for k, f in TICKET_FACTIONS.items()
])
async def ticketpanel_command(interaction: discord.Interaction, faction: app_commands.Choice[str]):
    is_allowed = interaction.user.id == BOT_OWNER_ID
    if not is_allowed and interaction.guild:
        try:
            is_allowed = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
        except Exception:
            is_allowed = False
    if not is_allowed:
        await interaction.response.send_message("❌ Commande inconnue.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    fac = TICKET_FACTIONS.get(faction.value)
    if not fac:
        await interaction.followup.send("❌ Faction inconnue.", ephemeral=True)
        return

    embed = discord.Embed(
        title=f"{fac['emoji']} {fac['name']}",
        description="Pour créer votre ticket de suivi, appuyez sur le bouton ci-dessous.",
        color=fac["color"],
    )
    view = discord.ui.View(timeout=None)
    view.add_item(TicketCreateButton(faction.value))

    try:
        await interaction.channel.send(embed=embed, view=view)
        await interaction.followup.send(f"✅ Panneau **{fac['name']}** posté dans {interaction.channel.mention}.", ephemeral=True)
        await log_to_db('info', f'/ticketpanel {fac["name"]} utilisé par {interaction.user} dans {interaction.guild.name}')
    except Exception as e:
        logger.error(f"Erreur /ticketpanel : {e}\n{traceback.format_exc()}")
        await interaction.followup.send("❌ Impossible de poster le panneau.", ephemeral=True)


# ════════════════════ SYSTÈME DE PLANIFICATION DE COMBAT ════════════════════

COMBAT_PLANNER_ROLE_ID = 1062740125605449877

# Rôles autorisés à accepter / modifier / refuser une proposition de combat
COMBAT_MANAGER_ROLE_IDS = [
    1062740125605449876,  # Gérants
    1399144149579731164,
    1399144243381010442,
]

COMBAT_FACTIONS = {
    "mangemort": {"name": "Mangemort", "emoji": "💀", "role_id": 1399144149579731164, "color": 0x8B0000},
    "auror": {"name": "Auror", "emoji": "⚖️", "role_id": 1399144243381010442, "color": 0x1F6FEB},
    "vampire": {"name": "Vampire", "emoji": "🧛", "role_id": 1399144243003527389, "color": 0x4A0E0E},
}

COMBAT_EVENT_TYPES = ["Combat", "Attaque de territoire", "Capture de Drapeau"]
COMBAT_LIEUX = ["Forêt", "Forêt Interdite"]
COMBAT_CONSEQUENCES = ["Blessures graves", "CK", "Perte d'un territoire", "Vol d'objet", "Autres ?"]

CF_FACTION = "Faction affrontée"
CF_TYPE = "Type d'événement"
CF_LIEU = "Lieu RP"
CF_DATE = "Date & Heure"
CF_OBJ = "Objectif RP"
CF_CONS = "Conséquences RP possibles"
CF_RULES = "Restrictions / Règles"
CF_ORG = "Organisateur"
CF_STATUS = "Statut"

COMBAT_COLOR_PENDING = 0xC0392B
COMBAT_COLOR_MODIFIED = 0xE67E22
COMBAT_COLOR_ACCEPTED = 0x2ECC71
COMBAT_COLOR_REFUSED = 0x4F545C


def _combat_field(embed: discord.Embed, name: str, default: str = "—") -> str:
    for f in embed.fields:
        if f.name == name:
            return f.value
    return default


async def _resolve_member(interaction: discord.Interaction):
    if isinstance(interaction.user, discord.Member):
        return interaction.user
    if not interaction.guild:
        return None
    member = interaction.guild.get_member(interaction.user.id)
    if member:
        return member
    try:
        return await interaction.guild.fetch_member(interaction.user.id)
    except Exception:
        return None


def _has_role(member, role_id: int) -> bool:
    return bool(member) and any(r.id == role_id for r in getattr(member, "roles", []))


def _has_any_role(member, role_ids) -> bool:
    if not member:
        return False
    member_role_ids = {r.id for r in getattr(member, "roles", [])}
    return any(rid in member_role_ids for rid in role_ids)


def build_combat_embed(*, faction_key, faction_name, type_str, lieu_str,
                       date_str, objectif_str, cons_str, rules_str,
                       organizer_id, status_str, color):
    fac = COMBAT_FACTIONS.get(faction_key, {})
    fac_display = f"{fac.get('emoji', '')} {faction_name}".strip()
    embed = discord.Embed(
        title=f"⚔️ Proposition Combat - {faction_name}",
        description=(
            f"## Les gérants {fac_display} sont défiés !\n"
            "Consultez la proposition ci-dessous puis **Acceptez** ou **Modifiez**."
        ),
        color=color,
        timestamp=datetime.datetime.utcnow(),
    )
    embed.add_field(name=CF_FACTION, value=fac_display, inline=True)
    embed.add_field(name=CF_TYPE, value=type_str or "—", inline=True)
    embed.add_field(name=CF_LIEU, value=lieu_str or "—", inline=True)
    embed.add_field(name=CF_DATE, value=date_str or "—", inline=False)
    embed.add_field(name=CF_OBJ, value=objectif_str or "—", inline=False)
    embed.add_field(name=CF_CONS, value=cons_str or "—", inline=False)
    embed.add_field(name=CF_RULES, value=rules_str or "—", inline=False)
    embed.add_field(name=CF_ORG, value=f"<@{organizer_id}>", inline=True)
    embed.add_field(name=CF_STATUS, value=status_str, inline=True)
    return embed


class CombatDetailsModal(discord.ui.Modal, title="Détails du combat"):
    date_heure = discord.ui.TextInput(
        label="Date & Heure",
        placeholder="Ex : 12/06/2026 à 21h00",
        max_length=100,
        required=True,
    )
    objectif = discord.ui.TextInput(
        label="Objectif RP",
        style=discord.TextStyle.paragraph,
        placeholder="(Si pour se défouler ou full HRP, laisser vide.)",
        max_length=500,
        required=False,
    )
    restrictions = discord.ui.TextInput(
        label="Restrictions / Règles",
        style=discord.TextStyle.paragraph,
        placeholder="Ex : pas de sortilèges mortels, etc.",
        max_length=500,
        required=False,
    )

    def __init__(self, *, faction_key, type_str, lieu_str, cons_str,
                 edit_message=None, org_id=None,
                 prefill_date="", prefill_obj="", prefill_rules=""):
        super().__init__()
        self._faction_key = faction_key
        self._type_str = type_str
        self._lieu_str = lieu_str
        self._cons_str = cons_str
        self._edit_message = edit_message
        self._org_id = org_id
        if prefill_date:
            self.date_heure.default = prefill_date
        if prefill_obj:
            self.objectif.default = prefill_obj
        if prefill_rules:
            self.restrictions.default = prefill_rules

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        fac = COMBAT_FACTIONS.get(self._faction_key)
        if not guild or not fac:
            await interaction.followup.send("❌ Erreur : faction inconnue.", ephemeral=True)
            return

        if self._edit_message is not None:
            embed = build_combat_embed(
                faction_key=self._faction_key,
                faction_name=fac["name"],
                type_str=self._type_str,
                lieu_str=self._lieu_str,
                date_str=self.date_heure.value,
                objectif_str=self.objectif.value or "—",
                cons_str=self._cons_str,
                rules_str=self.restrictions.value or "—",
                organizer_id=self._org_id,
                status_str=f"Modifié par {interaction.user.mention} — en attente de validation de l'organisateur",
                color=COMBAT_COLOR_MODIFIED,
            )
            view = make_combat_reaccept_view(self._org_id, self._faction_key)
            try:
                await self._edit_message.edit(
                    content=f"<@{self._org_id}>",
                    embed=embed,
                    view=view,
                    allowed_mentions=discord.AllowedMentions(users=True),
                )
                await interaction.followup.send(
                    "✅ Modifications envoyées à l'organisateur pour validation.", ephemeral=True)
                await log_to_db('info', f'Combat modifié par {interaction.user} dans {guild.name}')
            except Exception as e:
                logger.error(f"Erreur modif combat : {e}\n{traceback.format_exc()}")
                await interaction.followup.send("❌ Impossible d'enregistrer la modification.", ephemeral=True)
            return

        role = guild.get_role(fac["role_id"])
        if not role:
            await interaction.followup.send(
                "❌ Rôle des gérants de la faction adverse introuvable. Préviens un administrateur.",
                ephemeral=True,
            )
            return

        embed = build_combat_embed(
            faction_key=self._faction_key,
            faction_name=fac["name"],
            type_str=self._type_str,
            lieu_str=self._lieu_str,
            date_str=self.date_heure.value,
            objectif_str=self.objectif.value or "—",
            cons_str=self._cons_str,
            rules_str=self.restrictions.value or "—",
            organizer_id=interaction.user.id,
            status_str="En attente d'acceptation par les gérants adverses",
            color=COMBAT_COLOR_PENDING,
        )

        view = make_combat_response_view(interaction.user.id, self._faction_key)
        try:
            await interaction.channel.send(
                content=role.mention,
                embed=embed,
                view=view,
                allowed_mentions=discord.AllowedMentions(roles=True),
            )
            await interaction.followup.send(
                f"✅ Proposition de combat envoyée aux gérants **{fac['name']}**.",
                ephemeral=True,
            )
            await log_to_db('info', f'Combat proposé par {interaction.user} contre {fac["name"]} dans {guild.name}')
        except discord.Forbidden:
            await interaction.followup.send("❌ Le bot ne peut pas envoyer le message dans ce salon.", ephemeral=True)
        except Exception as e:
            logger.error(f"Erreur envoi combat : {e}\n{traceback.format_exc()}")
            await interaction.followup.send("❌ Impossible d'envoyer la proposition.", ephemeral=True)


class CombatAcceptButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r'combat_accept:(?P<org>\d+):(?P<faction>[a-z]+)'
):
    def __init__(self, org_id: int, faction_key: str):
        self.org_id = org_id
        self.faction_key = faction_key
        super().__init__(discord.ui.Button(
            label="Accepter le combat",
            style=discord.ButtonStyle.success,
            emoji="✅",
            custom_id=f"combat_accept:{org_id}:{faction_key}",
        ))

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(int(match['org']), match['faction'])

    async def callback(self, interaction: discord.Interaction):
        fac = COMBAT_FACTIONS.get(self.faction_key, {})
        member = await _resolve_member(interaction)
        if not _has_any_role(member, COMBAT_MANAGER_ROLE_IDS):
            await interaction.response.send_message(
                "❌ Tu n'as pas le rôle requis pour accepter ce combat.",
                ephemeral=True,
            )
            return
        if not interaction.message or not interaction.message.embeds:
            await interaction.response.send_message("❌ Embed du combat introuvable.", ephemeral=True)
            return
        await interaction.response.defer()
        embed = interaction.message.embeds[0]
        for i, f in enumerate(embed.fields):
            if f.name == CF_STATUS:
                embed.set_field_at(i, name=CF_STATUS, value=f"Combat accepté par {member.mention}", inline=True)
                break
        fac_display = f"{fac.get('emoji', '')} {fac.get('name', '')}".strip()
        embed.description = f"## La Faction {fac_display} relève le défi !"
        embed.color = discord.Color(COMBAT_COLOR_ACCEPTED)
        await interaction.message.edit(embed=embed, view=None)
        try:
            await log_to_db('info', f'Combat ({fac.get("name")}) accepté par {member} dans {interaction.guild.name}')
        except Exception:
            pass


class CombatRefuseButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r'combat_refuse:(?P<org>\d+):(?P<faction>[a-z]+)'
):
    def __init__(self, org_id: int, faction_key: str):
        self.org_id = org_id
        self.faction_key = faction_key
        super().__init__(discord.ui.Button(
            label="Refuser le combat",
            style=discord.ButtonStyle.danger,
            emoji="✖️",
            custom_id=f"combat_refuse:{org_id}:{faction_key}",
        ))

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(int(match['org']), match['faction'])

    async def callback(self, interaction: discord.Interaction):
        fac = COMBAT_FACTIONS.get(self.faction_key, {})
        member = await _resolve_member(interaction)
        if not _has_any_role(member, COMBAT_MANAGER_ROLE_IDS):
            await interaction.response.send_message(
                "❌ Tu n'as pas le rôle requis pour refuser ce combat.",
                ephemeral=True,
            )
            return
        if not interaction.message or not interaction.message.embeds:
            await interaction.response.send_message("❌ Embed du combat introuvable.", ephemeral=True)
            return
        await interaction.response.defer()
        embed = interaction.message.embeds[0]
        for i, f in enumerate(embed.fields):
            if f.name == CF_STATUS:
                embed.set_field_at(i, name=CF_STATUS, value=f"Combat refusé par {member.mention}", inline=True)
                break
        embed.color = discord.Color(COMBAT_COLOR_REFUSED)
        await interaction.message.edit(
            content=f"<@{self.org_id}>",
            embed=embed,
            view=None,
            allowed_mentions=discord.AllowedMentions(users=True),
        )
        try:
            await log_to_db('info', f'Combat ({fac.get("name")}) refusé par {member} dans {interaction.guild.name}')
        except Exception:
            pass


class CombatModifyButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r'combat_modify:(?P<org>\d+):(?P<faction>[a-z]+)'
):
    def __init__(self, org_id: int, faction_key: str):
        self.org_id = org_id
        self.faction_key = faction_key
        super().__init__(discord.ui.Button(
            label="Modifier",
            style=discord.ButtonStyle.secondary,
            emoji="✏️",
            custom_id=f"combat_modify:{org_id}:{faction_key}",
        ))

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(int(match['org']), match['faction'])

    async def callback(self, interaction: discord.Interaction):
        fac = COMBAT_FACTIONS.get(self.faction_key, {})
        member = await _resolve_member(interaction)
        if not _has_any_role(member, COMBAT_MANAGER_ROLE_IDS):
            await interaction.response.send_message(
                "❌ Tu n'as pas le rôle requis pour modifier ce combat.",
                ephemeral=True,
            )
            return
        if not interaction.message or not interaction.message.embeds:
            await interaction.response.send_message("❌ Embed du combat introuvable.", ephemeral=True)
            return
        src = interaction.message.embeds[0]
        type_v = _combat_field(src, CF_TYPE)
        lieu_v = _combat_field(src, CF_LIEU)
        cons_v = _combat_field(src, CF_CONS)
        cons_list = (
            [c.strip() for c in cons_v.split(",")]
            if cons_v not in ("—", "Aucune", "") else []
        )
        date_v = _combat_field(src, CF_DATE, "")
        obj_v = _combat_field(src, CF_OBJ, "")
        rules_v = _combat_field(src, CF_RULES, "")
        view = CombatSetupView(
            faction_value=self.faction_key,
            type_value=type_v if type_v in COMBAT_EVENT_TYPES else None,
            lieu_value=lieu_v if lieu_v in COMBAT_LIEUX else None,
            cons_values=[c for c in cons_list if c in COMBAT_CONSEQUENCES],
            edit_message=interaction.message,
            org_id=self.org_id,
            prefill_date="" if date_v == "—" else date_v,
            prefill_obj="" if obj_v == "—" else obj_v,
            prefill_rules="" if rules_v == "—" else rules_v,
        )
        await interaction.response.send_message(
            "**Re-modifie le combat depuis le début** : ajuste la faction, le type, le lieu et les "
            "conséquences, puis clique sur **Détails & Envoyer** pour revoir la date, l'objectif et "
            "les règles.",
            view=view,
            ephemeral=True,
        )


class CombatReacceptButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r'combat_reaccept:(?P<org>\d+):(?P<faction>[a-z]+)'
):
    def __init__(self, org_id: int, faction_key: str):
        self.org_id = org_id
        self.faction_key = faction_key
        super().__init__(discord.ui.Button(
            label="Accepter les modifications",
            style=discord.ButtonStyle.success,
            emoji="✅",
            custom_id=f"combat_reaccept:{org_id}:{faction_key}",
        ))

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(int(match['org']), match['faction'])

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.org_id:
            await interaction.response.send_message(
                "❌ Seul l'organisateur du combat peut valider les modifications.",
                ephemeral=True,
            )
            return
        if not interaction.message or not interaction.message.embeds:
            await interaction.response.send_message("❌ Embed du combat introuvable.", ephemeral=True)
            return
        await interaction.response.defer()
        embed = interaction.message.embeds[0]
        for i, f in enumerate(embed.fields):
            if f.name == CF_STATUS:
                embed.set_field_at(i, name=CF_STATUS, value=f"Modifications validées par {interaction.user.mention}", inline=True)
                break
        embed.color = discord.Color(COMBAT_COLOR_ACCEPTED)
        await interaction.message.edit(content=None, embed=embed, view=None)
        try:
            await log_to_db('info', f'Modifs combat validées par {interaction.user} dans {interaction.guild.name}')
        except Exception:
            pass


class CombatRefuseModifyButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r'combat_refusemod:(?P<org>\d+):(?P<faction>[a-z]+)'
):
    def __init__(self, org_id: int, faction_key: str):
        self.org_id = org_id
        self.faction_key = faction_key
        super().__init__(discord.ui.Button(
            label="Refuser les modifications",
            style=discord.ButtonStyle.danger,
            emoji="✖️",
            custom_id=f"combat_refusemod:{org_id}:{faction_key}",
        ))

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(int(match['org']), match['faction'])

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.org_id:
            await interaction.response.send_message(
                "❌ Seul l'organisateur du combat peut refuser les modifications.",
                ephemeral=True,
            )
            return
        if not interaction.message or not interaction.message.embeds:
            await interaction.response.send_message("❌ Embed du combat introuvable.", ephemeral=True)
            return
        await interaction.response.defer()
        embed = interaction.message.embeds[0]
        for i, f in enumerate(embed.fields):
            if f.name == CF_STATUS:
                embed.set_field_at(
                    i, name=CF_STATUS,
                    value=f"Modifications refusées par {interaction.user.mention} — combat annulé",
                    inline=True,
                )
                break
        embed.color = discord.Color(COMBAT_COLOR_REFUSED)
        await interaction.message.edit(content=None, embed=embed, view=None)
        try:
            await log_to_db('info', f'Modifs combat refusées (combat annulé) par {interaction.user} dans {interaction.guild.name}')
        except Exception:
            pass


def make_combat_response_view(org_id: int, faction_key: str) -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    view.add_item(CombatAcceptButton(org_id, faction_key))
    view.add_item(CombatModifyButton(org_id, faction_key))
    view.add_item(CombatRefuseButton(org_id, faction_key))
    return view


def make_combat_reaccept_view(org_id: int, faction_key: str) -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    view.add_item(CombatReacceptButton(org_id, faction_key))
    view.add_item(CombatRefuseModifyButton(org_id, faction_key))
    return view


class CombatSetupView(discord.ui.View):
    def __init__(self, *, faction_value=None, type_value=None, lieu_value=None,
                 cons_values=None, edit_message=None, org_id=None,
                 prefill_date="", prefill_obj="", prefill_rules=""):
        super().__init__(timeout=600)
        self.faction_value = faction_value
        self.type_value = type_value
        self.lieu_value = lieu_value
        self.cons_values = cons_values or []
        self.edit_message = edit_message
        self.org_id = org_id
        self._prefill_date = prefill_date
        self._prefill_obj = prefill_obj
        self._prefill_rules = prefill_rules
        if faction_value:
            for o in self.faction_select.options:
                o.default = (o.value == faction_value)
        if edit_message is not None:
            self.faction_select.disabled = True
            self.faction_select.placeholder = "Faction (non modifiable)"
        if type_value:
            for o in self.type_select.options:
                o.default = (o.value == type_value)
        if lieu_value:
            for o in self.lieu_select.options:
                o.default = (o.value == lieu_value)
        if self.cons_values:
            for o in self.cons_select.options:
                o.default = (o.value in self.cons_values)

    @discord.ui.select(
        placeholder="Faction à affronter…",
        min_values=1, max_values=1,
        options=[
            discord.SelectOption(label=f["name"], value=k, emoji=f["emoji"])
            for k, f in COMBAT_FACTIONS.items()
        ],
    )
    async def faction_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        self.faction_value = select.values[0]
        await interaction.response.defer()

    @discord.ui.select(
        placeholder="Type d'événement…",
        min_values=1, max_values=1,
        options=[discord.SelectOption(label=t, value=t) for t in COMBAT_EVENT_TYPES],
    )
    async def type_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        self.type_value = select.values[0]
        await interaction.response.defer()

    @discord.ui.select(
        placeholder="Lieu RP…",
        min_values=1, max_values=1,
        options=[discord.SelectOption(label=l, value=l) for l in COMBAT_LIEUX],
    )
    async def lieu_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        self.lieu_value = select.values[0]
        await interaction.response.defer()

    @discord.ui.select(
        placeholder="Conséquences RP possibles (optionnel)…",
        min_values=0, max_values=len(COMBAT_CONSEQUENCES),
        options=[discord.SelectOption(label=c, value=c) for c in COMBAT_CONSEQUENCES],
    )
    async def cons_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        self.cons_values = select.values
        await interaction.response.defer()

    @discord.ui.button(label="Détails & Envoyer", style=discord.ButtonStyle.primary, emoji="📨", row=4)
    async def submit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not (self.faction_value and self.type_value and self.lieu_value):
            await interaction.response.send_message(
                "❌ Sélectionne d'abord la **faction**, le **type d'événement** et le **lieu**.",
                ephemeral=True,
            )
            return
        cons_str = ", ".join(self.cons_values) if self.cons_values else "Aucune"
        await interaction.response.send_modal(CombatDetailsModal(
            faction_key=self.faction_value,
            type_str=self.type_value,
            lieu_str=self.lieu_value,
            cons_str=cons_str,
            edit_message=self.edit_message,
            org_id=self.org_id,
            prefill_date=self._prefill_date,
            prefill_obj=self._prefill_obj,
            prefill_rules=self._prefill_rules,
        ))


class CombatPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Planifier un combat",
        style=discord.ButtonStyle.danger,
        emoji="⚔️",
        custom_id="combat_plan_start",
    )
    async def plan_combat(self, interaction: discord.Interaction, button: discord.ui.Button):
        member = await _resolve_member(interaction)
        if not _has_role(member, COMBAT_PLANNER_ROLE_ID):
            await interaction.response.send_message(
                "❌ Seuls les gérants peuvent planifier un combat.",
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            "**Configure ton combat** : choisis la faction à affronter, le type, le lieu et les "
            "conséquences, puis clique sur **Détails & Envoyer**.",
            view=CombatSetupView(),
            ephemeral=True,
        )


@bot.tree.command(name="combatpanel", description="Poster le panneau de planification de combat.")
@app_commands.default_permissions(administrator=True)
async def combatpanel_command(interaction: discord.Interaction):
    is_allowed = interaction.user.id == BOT_OWNER_ID
    if not is_allowed and interaction.guild:
        try:
            is_allowed = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
        except Exception:
            is_allowed = False
    if not is_allowed:
        await interaction.response.send_message("❌ Commande inconnue.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    embed = discord.Embed(
        title="⚔️ Planification de Combat",
        description=(
            "Gérants, vous souhaitez organiser un affrontement RP ?\n"
            "Cliquez sur le bouton ci-dessous pour planifier un combat et le proposer "
            "à la faction adverse.\n\n"
            "Les gérants de la faction concernée pourront **accepter** ou **proposer des "
            "modifications** (à re-valider par l'organisateur)."
        ),
        color=COMBAT_COLOR_PENDING,
    )
    try:
        await interaction.channel.send(embed=embed, view=CombatPanelView())
        await interaction.followup.send(f"✅ Panneau de combat posté dans {interaction.channel.mention}.", ephemeral=True)
        await log_to_db('info', f'/combatpanel utilisé par {interaction.user} dans {interaction.guild.name}')
    except Exception as e:
        logger.error(f"Erreur /combatpanel : {e}\n{traceback.format_exc()}")
        await interaction.followup.send("❌ Impossible de poster le panneau.", ephemeral=True)


# ════════════════════ LIVRE DE CONTRATS / BINGO BOOK ════════════════════

# Rôle autorisé à accepter / réaliser un contrat
CONTRAT_ACCEPT_ROLE_ID = 1062740125475426411

# Rôles autorisés à créer (et annuler) un contrat
CONTRAT_CREATOR_ROLE_IDS = [
    1062740125559300163,  # Ministère
    1062740125517348875,  # Auror
    1062740125605449874,  # Mangemort
]

# Salon où sont publiés les contrats
CONTRAT_PUBLISH_CHANNEL_ID = 1519461397598048367

# Catégorie où sont créés les salons privés commanditaire/mercenaire
CONTRAT_CATEGORY_ID = 1519459783042924555

CONTRAT_TYPES = {
    "merc": {"label": "Mercenariat", "emoji": "🗡️"},
    "spe": {"label": "Contrat spécial", "emoji": "⭐"},
}

CTF_TYPE = "Type"
CTF_TITRE = "Titre / Cible"
CTF_RECOMP = "Récompense"
CTF_OBJ = "Objectif / Description"
CTF_COND = "Conditions / Restrictions"
CTF_DELAI = "Délai"
CTF_COMMAND = "Commanditaire"
CTF_STATUT = "Statut"

CONTRAT_COLOR_OPEN = 0xC0392B
CONTRAT_COLOR_PROGRESS = 0xE67E22
CONTRAT_COLOR_DONE = 0x2ECC71
CONTRAT_COLOR_CANCEL = 0x4F545C

# Libellé affiché à la place du commanditaire pour les contrats anonymes
CONTRAT_ANON_LABEL = "🕵️ Inconnu"

# Couleur parchemin pour les missives relayées
CONTRAT_MISSIVE_COLOR = 0xC9A66B


def _format_missive(content: str) -> str:
    lines = content.splitlines() or [content]
    return "\n".join(f"> {line}" if line.strip() else ">" for line in lines)


async def contrat_relay_add(channel_id: int, guild_id: int, commanditaire_id: int):
    if not pool:
        return
    try:
        await pool.execute(
            "INSERT INTO contrat_anon_relay (channel_id, guild_id, commanditaire_id) "
            "VALUES ($1, $2, $3) ON CONFLICT (channel_id) DO UPDATE "
            "SET commanditaire_id = EXCLUDED.commanditaire_id",
            str(channel_id), str(guild_id), str(commanditaire_id))
    except Exception as e:
        logger.error(f"contrat_relay_add error: {e}")


async def contrat_relay_remove(channel_id: int):
    if not pool:
        return
    try:
        await pool.execute(
            "DELETE FROM contrat_anon_relay WHERE channel_id = $1", str(channel_id))
    except Exception as e:
        logger.error(f"contrat_relay_remove error: {e}")


async def contrat_relay_get_by_channel(channel_id: int):
    if not pool:
        return None
    try:
        return await pool.fetchrow(
            "SELECT channel_id, guild_id, commanditaire_id FROM contrat_anon_relay "
            "WHERE channel_id = $1", str(channel_id))
    except Exception as e:
        logger.error(f"contrat_relay_get_by_channel error: {e}")
        return None


async def contrat_relay_list_by_commanditaire(commanditaire_id: int):
    if not pool:
        return []
    try:
        return await pool.fetch(
            "SELECT channel_id, guild_id, commanditaire_id FROM contrat_anon_relay "
            "WHERE commanditaire_id = $1 ORDER BY created_at DESC",
            str(commanditaire_id))
    except Exception as e:
        logger.error(f"contrat_relay_list_by_commanditaire error: {e}")
        return []


def build_contrat_embed(*, type_key, titre, recompense, objectif, conditions,
                        delai, commanditaire_id, statut, color, anonyme=False):
    t = CONTRAT_TYPES.get(type_key, {})
    embed = discord.Embed(
        title=f"{t.get('emoji', '📕')} {t.get('label', 'Contrat')}",
        description=f"## {titre}",
        color=color,
        timestamp=datetime.datetime.utcnow(),
    )
    embed.add_field(name=CTF_TYPE, value=t.get("label", "—"), inline=True)
    embed.add_field(name=CTF_RECOMP, value=recompense or "—", inline=True)
    embed.add_field(name=CTF_DELAI, value=delai or "—", inline=True)
    embed.add_field(name=CTF_OBJ, value=objectif or "—", inline=False)
    embed.add_field(name=CTF_COND, value=conditions or "—", inline=False)
    embed.add_field(name=CTF_TITRE, value=titre or "—", inline=False)
    commanditaire_value = CONTRAT_ANON_LABEL if anonyme else f"<@{commanditaire_id}>"
    embed.add_field(name=CTF_COMMAND, value=commanditaire_value, inline=True)
    embed.add_field(name=CTF_STATUT, value=statut, inline=True)
    return embed


class ContratModal(discord.ui.Modal):
    titre = discord.ui.TextInput(
        label="Titre / Cible", placeholder="Ex : Éliminer X, Récupérer Y…", max_length=100, required=True)
    recompense = discord.ui.TextInput(
        label="Récompense", placeholder="Ex : 5000 gallions, une faveur…", max_length=200, required=True)
    objectif = discord.ui.TextInput(
        label="Objectif / Description", style=discord.TextStyle.paragraph, max_length=1000, required=True)
    conditions = discord.ui.TextInput(
        label="Conditions / Restrictions", style=discord.TextStyle.paragraph, max_length=500, required=False)
    delai = discord.ui.TextInput(
        label="Délai", placeholder="Ex : sous 7 jours RP (optionnel)", max_length=100, required=False)

    def __init__(self, *, type_key, anonyme=False):
        self._type_key = type_key
        self._anonyme = anonyme
        t = CONTRAT_TYPES.get(type_key, {})
        suffix = " (anonyme)" if anonyme else ""
        super().__init__(title=f"Nouveau — {t.get('label', 'Contrat')}{suffix}")

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        member = await _resolve_member(interaction)
        if not _has_any_role(member, CONTRAT_CREATOR_ROLE_IDS):
            await interaction.followup.send(
                "❌ Seuls le Ministère, les Aurors et les Mangemorts peuvent créer un contrat.",
                ephemeral=True)
            return
        embed = build_contrat_embed(
            type_key=self._type_key,
            titre=self.titre.value,
            recompense=self.recompense.value,
            objectif=self.objectif.value,
            conditions=self.conditions.value or "—",
            delai=self.delai.value or "—",
            commanditaire_id=interaction.user.id,
            statut="🟢 Ouvert",
            color=CONTRAT_COLOR_OPEN,
            anonyme=self._anonyme,
        )
        view = make_contrat_open_view(interaction.user.id)
        channel = interaction.guild.get_channel(CONTRAT_PUBLISH_CHANNEL_ID)
        if channel is None:
            await interaction.followup.send(
                "❌ Salon de publication des contrats introuvable. Contacte un administrateur.",
                ephemeral=True)
            return
        try:
            await channel.send(embed=embed, view=view)
            confirm = "✅ Contrat publié dans {}.".format(channel.mention)
            if self._anonyme:
                confirm += (
                    "\n🕵️ **Mode anonyme :** ton identité restera cachée. "
                    "Quand un mercenaire prendra le contrat, je t'écrirai en MP — "
                    "tu pourras me répondre ici et je transmettrai tes messages "
                    "anonymement dans le salon privé."
                )
            await interaction.followup.send(confirm, ephemeral=True)
            await log_to_db('info', f'Contrat ({self._type_key}{" anonyme" if self._anonyme else ""}) publié par {interaction.user} dans {interaction.guild.name}')
        except discord.Forbidden:
            await interaction.followup.send("❌ Le bot ne peut pas écrire dans le salon des contrats.", ephemeral=True)
        except Exception as e:
            logger.error(f"Erreur publication contrat : {e}\n{traceback.format_exc()}")
            await interaction.followup.send("❌ Impossible de publier le contrat.", ephemeral=True)


def _contrat_set_status(embed: discord.Embed, value: str):
    for i, f in enumerate(embed.fields):
        if f.name == CTF_STATUT:
            embed.set_field_at(i, name=CTF_STATUT, value=value, inline=True)
            return


def _contrat_field(embed: discord.Embed, name: str):
    for f in embed.fields:
        if f.name == name:
            return f.value
    return None


async def _contrat_fetch_member(guild, user_id: int):
    member = guild.get_member(user_id)
    if member is None:
        try:
            member = await guild.fetch_member(user_id)
        except Exception:
            member = None
    return member


async def _contrat_send_dm(user, embed: discord.Embed):
    if user is None:
        return False
    try:
        await user.send(embed=embed)
        return True
    except Exception:
        return False


async def _contrat_open_private_channel(guild, creator, acceptor, category, *, titre):
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        guild.me: discord.PermissionOverwrite(
            view_channel=True, send_messages=True, manage_channels=True, read_message_history=True),
    }
    member_perms = discord.PermissionOverwrite(
        view_channel=True, send_messages=True, read_message_history=True, attach_files=True)
    if acceptor:
        overwrites[acceptor] = member_perms
    if creator and (acceptor is None or creator.id != acceptor.id):
        overwrites[creator] = member_perms
    cible = (titre or "").strip()
    name = (f"contrat-{cible}" if cible else "contrat")[:90]
    channel = await guild.create_text_channel(
        name=name, category=category, overwrites=overwrites,
        reason="Salon privé de contrat")
    return channel


class ContratAcceptButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r'contrat_accept:(?P<author>\d+)'
):
    def __init__(self, author_id: int):
        self.author_id = author_id
        super().__init__(discord.ui.Button(
            label="Prendre le contrat",
            style=discord.ButtonStyle.success,
            emoji="✅",
            custom_id=f"contrat_accept:{author_id}",
        ))

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(int(match['author']))

    async def callback(self, interaction: discord.Interaction):
        member = await _resolve_member(interaction)
        if not _has_role(member, CONTRAT_ACCEPT_ROLE_ID):
            await interaction.response.send_message(
                "❌ Tu n'as pas le rôle requis pour prendre un contrat.", ephemeral=True)
            return
        if not interaction.message or not interaction.message.embeds:
            await interaction.response.send_message("❌ Contrat introuvable.", ephemeral=True)
            return
        await interaction.response.defer()
        guild = interaction.guild
        embed = interaction.message.embeds[0]
        titre = _contrat_field(embed, CTF_TITRE) or "Contrat"
        is_anon = _contrat_field(embed, CTF_COMMAND) == CONTRAT_ANON_LABEL
        creator = await _contrat_fetch_member(guild, self.author_id)

        category = guild.get_channel(CONTRAT_CATEGORY_ID)
        if not isinstance(category, discord.CategoryChannel):
            await interaction.followup.send(
                "❌ La catégorie des salons de contrat est introuvable. "
                "Le contrat n'a pas été pris, préviens un administrateur.",
                ephemeral=True)
            return

        try:
            private_channel = await _contrat_open_private_channel(
                guild, None if is_anon else creator, member, category, titre=titre)
        except discord.Forbidden:
            logger.error("Contrat: permission manquante pour créer le salon privé.")
            await interaction.followup.send(
                "❌ Je n'ai pas la permission de créer le salon privé (Gérer les salons). "
                "Le contrat n'a pas été pris.",
                ephemeral=True)
            return
        except Exception as e:
            logger.error(f"Contrat: échec création salon privé : {e}\n{traceback.format_exc()}")
            await interaction.followup.send(
                "❌ Erreur lors de la création du salon privé. Le contrat n'a pas été pris.",
                ephemeral=True)
            return

        _contrat_set_status(embed, f"🟠 Pris en charge par {member.mention}")
        embed.color = discord.Color(CONTRAT_COLOR_PROGRESS)
        embed.add_field(name="💬 Salon privé", value=private_channel.mention, inline=True)
        await interaction.message.edit(
            embed=embed, view=make_contrat_progress_view(self.author_id, member.id))

        if is_anon:
            intro_desc = (
                f"Salon privé entre le **commanditaire anonyme** ({CONTRAT_ANON_LABEL}) "
                f"et {member.mention} (mercenaire).\n\n"
                f"**Contrat :** {titre}\n"
                "Le commanditaire reste **anonyme** : il suit la discussion via ses MP. "
                "Tout ce que tu écris ici lui est transmis, et ses réponses apparaîtront "
                "ici sous le nom 🕵️ **Commanditaire (anonyme)**."
            )
        else:
            intro_desc = (
                f"Salon privé entre {creator.mention if creator else '`commanditaire`'} "
                f"(commanditaire) et {member.mention} (mercenaire).\n\n"
                f"**Contrat :** {titre}\n"
                "Vous pouvez discuter librement ici des détails du contrat."
            )
        intro = discord.Embed(
            title="💬 Discussion de contrat",
            description=intro_desc,
            color=CONTRAT_COLOR_PROGRESS,
        )
        if is_anon:
            mentions = member.mention
        else:
            mentions = member.mention + (f" {creator.mention}" if creator else "")
        try:
            await private_channel.send(content=mentions, embed=intro, view=ContratCloseView())
        except Exception:
            pass

        if is_anon:
            await contrat_relay_add(private_channel.id, guild.id, self.author_id)

        if is_anon:
            dm_desc = (
                f"**{member.display_name}** vient de prendre en charge ton contrat **{titre}**.\n\n"
                "🕵️ Ton identité reste **anonyme**. Pour discuter avec le mercenaire, "
                "**réponds simplement à ce message privé** : je transmettrai tes messages "
                "anonymement dans le salon du contrat, et je te renverrai ici ses réponses."
            )
        else:
            dm_desc = (
                f"**{member.display_name}** vient de prendre en charge ton contrat **{titre}**.\n\n"
                f"Un salon privé a été ouvert : {private_channel.mention}"
            )
        dm_embed = discord.Embed(
            title="📕 Ton contrat a été pris !",
            description=dm_desc,
            color=CONTRAT_COLOR_PROGRESS,
        )
        dm_target = creator
        if dm_target is None:
            try:
                dm_target = await bot.fetch_user(self.author_id)
            except Exception:
                dm_target = None
        dm_ok = await _contrat_send_dm(dm_target, dm_embed)
        if not dm_ok:
            if is_anon:
                warn = (
                    "⚠️ Le **commanditaire anonyme** n'a pas pu être prévenu en MP "
                    "(messages privés fermés ?). Le relais anonyme ne fonctionnera "
                    "peut-être pas tant qu'il ne m'a pas écrit. Le contrat a bien été pris."
                )
            else:
                warn = (
                    f"⚠️ <@{self.author_id}> (commanditaire) n'a pas pu être prévenu en MP "
                    "(messages privés fermés ?). Le contrat a bien été pris."
                )
            try:
                await private_channel.send(warn)
            except Exception:
                pass

        try:
            await log_to_db('info', f'Contrat pris par {member} dans {interaction.guild.name}')
        except Exception:
            pass


class ContratDoneButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r'contrat_done:(?P<author>\d+):(?P<accepter>\d+)'
):
    def __init__(self, author_id: int, accepter_id: int):
        self.author_id = author_id
        self.accepter_id = accepter_id
        super().__init__(discord.ui.Button(
            label="Marquer terminé",
            style=discord.ButtonStyle.primary,
            emoji="🏁",
            custom_id=f"contrat_done:{author_id}:{accepter_id}",
        ))

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(int(match['author']), int(match['accepter']))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.accepter_id:
            await interaction.response.send_message(
                "❌ Seule la personne qui a pris ce contrat peut le marquer comme terminé.",
                ephemeral=True)
            return
        if not interaction.message or not interaction.message.embeds:
            await interaction.response.send_message("❌ Contrat introuvable.", ephemeral=True)
            return
        await interaction.response.defer()
        member = interaction.user
        embed = interaction.message.embeds[0]
        _contrat_set_status(embed, f"✅ Terminé — validé par {member.mention}")
        embed.color = discord.Color(CONTRAT_COLOR_DONE)
        titre = _contrat_field(embed, CTF_TITRE) or "Contrat"
        await interaction.message.edit(embed=embed, view=None)

        guild = interaction.guild
        dm_embed = discord.Embed(
            title="🏁 Contrat terminé",
            description=(
                f"Le contrat **{titre}** a été marqué comme **terminé** par {member.mention}."
            ),
            color=CONTRAT_COLOR_DONE,
        )
        target = await _contrat_fetch_member(guild, self.author_id)
        if target is None:
            try:
                target = await bot.fetch_user(self.author_id)
            except Exception:
                target = None
        await _contrat_send_dm(target, dm_embed)

        try:
            await log_to_db('info', f'Contrat terminé par {member} dans {interaction.guild.name}')
        except Exception:
            pass


class ContratCancelButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r'contrat_cancel:(?P<author>\d+)(?::(?P<accepter>\d+))?'
):
    def __init__(self, author_id: int, accepter_id: int = None):
        self.author_id = author_id
        self.accepter_id = accepter_id
        custom_id = f"contrat_cancel:{author_id}"
        if accepter_id is not None:
            custom_id += f":{accepter_id}"
        super().__init__(discord.ui.Button(
            label="Annuler",
            style=discord.ButtonStyle.danger,
            emoji="✖️",
            custom_id=custom_id,
        ))

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        accepter = match['accepter']
        return cls(int(match['author']), int(accepter) if accepter else None)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "❌ Seul le commanditaire (celui qui a créé le contrat) peut l'annuler.",
                ephemeral=True)
            return
        if not interaction.message or not interaction.message.embeds:
            await interaction.response.send_message("❌ Contrat introuvable.", ephemeral=True)
            return
        await interaction.response.defer()
        member = interaction.user
        embed = interaction.message.embeds[0]
        _contrat_set_status(embed, f"🚫 Annulé par {member.mention}")
        embed.color = discord.Color(CONTRAT_COLOR_CANCEL)
        await interaction.message.edit(embed=embed, view=None)
        try:
            await log_to_db('info', f'Contrat annulé par {member} dans {interaction.guild.name}')
        except Exception:
            pass


def make_contrat_open_view(author_id: int) -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    view.add_item(ContratAcceptButton(author_id))
    view.add_item(ContratCancelButton(author_id))
    return view


def make_contrat_progress_view(author_id: int, accepter_id: int) -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    view.add_item(ContratDoneButton(author_id, accepter_id))
    view.add_item(ContratCancelButton(author_id, accepter_id))
    return view


class ContratCloseView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Fermer le salon", style=discord.ButtonStyle.danger,
                       emoji="🔒", custom_id="contrat_close_channel")
    async def close(self, interaction: discord.Interaction, button: discord.ui.Button):
        member = await _resolve_member(interaction)
        allowed = (
            _has_role(member, CONTRAT_ACCEPT_ROLE_ID)
            or _has_any_role(member, CONTRAT_CREATOR_ROLE_IDS)
            or (member is not None and member.guild_permissions.manage_channels)
        )
        if not allowed:
            await interaction.response.send_message(
                "❌ Tu n'as pas le droit de fermer ce salon.", ephemeral=True)
            return
        await interaction.response.send_message(
            f"🔒 Salon fermé par {interaction.user.mention}. Suppression dans 5 secondes…")
        relay = await contrat_relay_get_by_channel(interaction.channel.id)
        await asyncio.sleep(5)
        channel_id = interaction.channel.id
        try:
            await interaction.channel.delete(
                reason=f"Salon de contrat fermé par {interaction.user}")
            if relay:
                await contrat_relay_remove(channel_id)
                target = bot.get_user(int(relay['commanditaire_id']))
                if target is None:
                    try:
                        target = await bot.fetch_user(int(relay['commanditaire_id']))
                    except Exception:
                        target = None
                if target is not None:
                    try:
                        await target.send(
                            "🔒 Le salon de ton contrat anonyme a été fermé. "
                            "Le relais est désormais terminé.")
                    except Exception:
                        pass
        except Exception as e:
            logger.error(f"Contrat: échec suppression salon privé : {e}\n{traceback.format_exc()}")
            try:
                await interaction.followup.send(
                    "❌ Impossible de supprimer le salon (permission manquante ?).",
                    ephemeral=True)
            except Exception:
                pass


class ContratPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _open(self, interaction: discord.Interaction, type_key: str, anonyme: bool = False):
        member = await _resolve_member(interaction)
        if not _has_any_role(member, CONTRAT_CREATOR_ROLE_IDS):
            await interaction.response.send_message(
                "❌ Seuls le Ministère, les Aurors et les Mangemorts peuvent créer un contrat.",
                ephemeral=True)
            return
        await interaction.response.send_modal(ContratModal(type_key=type_key, anonyme=anonyme))

    @discord.ui.button(label="Mercenariat", style=discord.ButtonStyle.danger,
                       emoji="🗡️", custom_id="contrat_new_merc", row=0)
    async def new_merc(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._open(interaction, "merc")

    @discord.ui.button(label="Contrat spécial", style=discord.ButtonStyle.secondary,
                       emoji="⭐", custom_id="contrat_new_spe", row=0)
    async def new_spe(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._open(interaction, "spe")

    @discord.ui.button(label="Mercenariat anonyme", style=discord.ButtonStyle.danger,
                       emoji="🕵️", custom_id="contrat_new_merc_anon", row=1)
    async def new_merc_anon(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._open(interaction, "merc", anonyme=True)

    @discord.ui.button(label="Contrat spécial anonyme", style=discord.ButtonStyle.secondary,
                       emoji="🕵️", custom_id="contrat_new_spe_anon", row=1)
    async def new_spe_anon(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._open(interaction, "spe", anonyme=True)


@bot.tree.command(name="contratbook", description="Poster le livre de contrats (bingo book).")
@app_commands.default_permissions(administrator=True)
async def contratbook_command(interaction: discord.Interaction):
    is_allowed = interaction.user.id == BOT_OWNER_ID
    if not is_allowed and interaction.guild:
        try:
            is_allowed = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
        except Exception:
            is_allowed = False
    if not is_allowed:
        await interaction.response.send_message("❌ Commande inconnue.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    embed = discord.Embed(
        title="📕 Livre de Contrats",
        description=(
            "Bienvenue dans le **livre de contrats** du monde magique.\n\n"
            "🗡️ **Mercenariat** - propose une mission rémunérée (cible, récompense, délai).\n"
            "⭐ **Contrat spécial** - pour les contrats hors du commun.\n\n"
            "🕵️ **Versions anonymes** - l'auteur du contrat reste anonyme ; les deux personnes discuteront par missive anonyme.\n\n"
            "Choisis ci-dessous le type de contrat à publier."
        ),
        color=CONTRAT_COLOR_OPEN,
    )
    try:
        await interaction.channel.send(embed=embed, view=ContratPanelView())
        await interaction.followup.send(f"✅ Livre de contrats posté dans {interaction.channel.mention}.", ephemeral=True)
        await log_to_db('info', f'/contratbook utilisé par {interaction.user} dans {interaction.guild.name}')
    except Exception as e:
        logger.error(f"Erreur /contratbook : {e}\n{traceback.format_exc()}")
        await interaction.followup.send("❌ Impossible de poster le livre de contrats.", ephemeral=True)


# ════════════════════ WANTED BOOK / AVIS DE RECHERCHE ════════════════════

# Rôles autorisés à recenser une personne recherchée
WANTED_ROLE_IDS = [
    1062740125559300163,  # Ministère
    1062740125517348875,  # Auror
]

# Salon où sont publiés les avis de recherche
WANTED_CHANNEL_ID = 1519500581960421417

WANTED_COLOR = 0x5DADE2


class WantedModalStep1(discord.ui.Modal):
    nom = discord.ui.TextInput(
        label="Nom", placeholder="Nom / identité de la personne recherchée", max_length=100, required=True)
    rang = discord.ui.TextInput(
        label="Rang", placeholder="Ex : S, A, B…", max_length=100, required=True)
    appartenance = discord.ui.TextInput(
        label="Appartenance", placeholder="Ex : Mangemort, Indépendant…", max_length=100, required=True)
    condition = discord.ui.TextInput(
        label="Condition de Réussite", style=discord.TextStyle.paragraph, max_length=500, required=True)
    prime = discord.ui.TextInput(
        label="Prime", placeholder="Ex : 50 000 gallions…", max_length=200, required=True)

    def __init__(self):
        super().__init__(title="Avis de recherche (1/2)")

    async def on_submit(self, interaction: discord.Interaction):
        data = {
            "nom": self.nom.value,
            "rang": self.rang.value,
            "appartenance": self.appartenance.value,
            "condition": self.condition.value,
            "prime": self.prime.value,
        }
        await interaction.response.send_message(
            "✅ Étape 1 enregistrée. Clique sur **Continuer** pour ajouter le champ *Autre* "
            "et le **lien de l'image**, puis publier l'avis.",
            view=WantedStep2View(data), ephemeral=True)


class WantedStep2View(discord.ui.View):
    def __init__(self, data: dict):
        super().__init__(timeout=300)
        self._data = data

    @discord.ui.button(label="Continuer", style=discord.ButtonStyle.primary, emoji="➡️")
    async def cont(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(WantedModalStep2(self._data))


class WantedModalStep2(discord.ui.Modal):
    autre = discord.ui.TextInput(
        label="Autre", style=discord.TextStyle.paragraph, max_length=500, required=False)
    lien_image = discord.ui.TextInput(
        label="Lien de l'image", placeholder="https://…", max_length=500, required=True)

    def __init__(self, data: dict):
        self._data = data
        super().__init__(title="Avis de recherche (2/2)")

    async def on_submit(self, interaction: discord.Interaction):
        lien = self.lien_image.value.strip()
        if not lien.lower().startswith(("http://", "https://")):
            await interaction.response.send_message(
                "❌ Le lien de l'image doit commencer par `http://` ou `https://`.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        channel = interaction.guild.get_channel(WANTED_CHANNEL_ID)
        if channel is None:
            await interaction.followup.send(
                "❌ Salon du Wanted Book introuvable. Préviens un administrateur.", ephemeral=True)
            return

        d = self._data
        body = (
            f"**Rang :** {d['rang']}   •   "
            f"**Appartenance :** {d['appartenance']}   •   "
            f"**Prime :** {d['prime']}\n"
            f"**Condition de Réussite :** {d['condition']}\n"
            f"**Autre :** {self.autre.value or '—'}"
        )

        container = discord.ui.Container(accent_colour=WANTED_COLOR)
        container.add_item(discord.ui.TextDisplay(f"# 🎯 AVIS DE RECHERCHE — {d['nom']}"))
        container.add_item(discord.ui.TextDisplay(body))
        container.add_item(discord.ui.MediaGallery(discord.MediaGalleryItem(lien)))
        container.add_item(discord.ui.TextDisplay(f"-# Recensé par {interaction.user.display_name}"))

        view = discord.ui.LayoutView(timeout=None)
        view.add_item(container)

        try:
            await channel.send(view=view)
            await interaction.followup.send(
                f"✅ Avis de recherche publié dans {channel.mention}.", ephemeral=True)
            await log_to_db('info', f"Wanted ({d['nom']}) recensé par {interaction.user} dans {interaction.guild.name}")
        except discord.Forbidden:
            await interaction.followup.send(
                "❌ Le bot ne peut pas écrire dans le salon du Wanted Book.", ephemeral=True)
        except Exception as e:
            logger.error(f"Erreur /wanted : {e}\n{traceback.format_exc()}")
            await interaction.followup.send(
                "❌ Impossible de publier l'avis de recherche.", ephemeral=True)


class WantedPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Créer un avis de recherche", style=discord.ButtonStyle.danger,
                       emoji="🎯", custom_id="wanted_create")
    async def create(self, interaction: discord.Interaction, button: discord.ui.Button):
        member = await _resolve_member(interaction)
        if not _has_any_role(member, WANTED_ROLE_IDS):
            await interaction.response.send_message(
                "❌ Seuls les membres du Ministère et les Aurors peuvent recenser une personne recherchée.",
                ephemeral=True)
            return
        await interaction.response.send_modal(WantedModalStep1())


@bot.tree.command(name="wanted", description="Poster le panneau du Wanted Book (avis de recherche).")
@app_commands.default_permissions(administrator=True)
async def wanted_command(interaction: discord.Interaction):
    is_allowed = interaction.user.id == BOT_OWNER_ID
    if not is_allowed and interaction.guild:
        try:
            is_allowed = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
        except Exception:
            is_allowed = False
    if not is_allowed:
        await interaction.response.send_message("❌ Commande inconnue.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    embed = discord.Embed(
        title="🎯 Wanted Book",
        description=(
            "Bienvenue dans le **Wanted Book**.\n\n"
            "Clique sur le bouton ci-dessous pour recenser une personne recherchée "
            "et publier un **avis de recherche**.\n\n"
            "*Réservé au Ministère et aux Aurors.*"
        ),
        color=WANTED_COLOR,
    )
    try:
        await interaction.channel.send(embed=embed, view=WantedPanelView())
        await interaction.followup.send(f"✅ Panneau du Wanted Book posté dans {interaction.channel.mention}.", ephemeral=True)
        await log_to_db('info', f'/wanted (panneau) posté par {interaction.user} dans {interaction.guild.name}')
    except Exception as e:
        logger.error(f"Erreur /wanted : {e}\n{traceback.format_exc()}")
        await interaction.followup.send("❌ Impossible de poster le panneau du Wanted Book.", ephemeral=True)


@bot.tree.command(name="closeticket", description="Fermer (supprimer) le ticket en cours.")
@app_commands.default_permissions(administrator=True)
async def closeticket_command(interaction: discord.Interaction):
    is_allowed = interaction.user.id == BOT_OWNER_ID
    if not is_allowed and interaction.guild:
        try:
            is_allowed = await is_owner_or_ownerlist(interaction.guild, interaction.user.id)
        except Exception:
            is_allowed = False
    if not is_allowed:
        await interaction.response.send_message("❌ Commande inconnue.", ephemeral=True)
        return

    channel = interaction.channel
    if not channel or not interaction.guild:
        await interaction.response.send_message("❌ Commande utilisable uniquement sur un serveur.", ephemeral=True)
        return

    is_ticket_channel = (
        channel.category_id in {f["category_id"] for f in TICKET_FACTIONS.values()}
        and channel.topic and "uid=" in channel.topic
    )
    if not is_ticket_channel:
        await interaction.response.send_message(
            "❌ Cette commande doit être utilisée dans un salon de ticket.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        f"🗑️ Ticket fermé par {interaction.user.mention}. Suppression du salon dans 5 secondes…"
    )
    await asyncio.sleep(5)
    try:
        await channel.delete(reason=f"Ticket fermé par {interaction.user}")
        try:
            await log_to_db('info', f'Ticket {channel.name} fermé par {interaction.user} dans {interaction.guild.name}')
        except Exception:
            pass
    except discord.Forbidden:
        try:
            await interaction.followup.send("❌ Le bot n'a pas la permission de supprimer ce salon.", ephemeral=True)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Erreur suppression ticket : {e}\n{traceback.format_exc()}")


async def main():
    await init_db()
    token = os.environ.get("DISCORD_TOKEN")
    if not token:
        logger.error("DISCORD_TOKEN is not set.")
        return
    logger.info("Starting bot...")
    async with bot:
        await bot.start(token)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
