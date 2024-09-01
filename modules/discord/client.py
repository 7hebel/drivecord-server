from modules.discord import help
from modules.logs import Log

from dotenv import load_dotenv
from discord.ext import commands
import discord
import os


env_status = load_dotenv()
if not env_status:
    raise EnvironmentError("No .env file found or `DRIVECORD-TOKEN` not found in .env file.")


class BotClient(commands.Bot):
    # https://discord.com/oauth2/authorize?client_id=1273050122451816599
    _token = os.getenv("DRIVECORD-TOKEN")

    def __init__(self):
        intents = discord.Intents.all()
        self.owner_ids = [418673016093016066]

        super().__init__(command_prefix="", intents=intents, help_command=help.CustomHelpCommand())

    async def on_ready(self):
        await self.load_extension("modules.discord.cogs.console")
        await self.load_extension("modules.discord.cogs.events")
        await self.load_extension("modules.discord.cogs.tree")
        await self.tree.sync()
        Log.info("Discord bot is running...")


client = BotClient()