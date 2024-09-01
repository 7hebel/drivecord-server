from modules.discord.cogs import setups
from modules.discord.assets import *
from modules.discord import data
from modules import accounts
from modules import perms

from discord.ext import commands
from discord import app_commands
import discord


def _build_perms_toogle_buttons(caller_perms: perms.DrivePermissions, user: discord.Member, perms: perms.DrivePermissions) -> discord.ui.View:
    read_style = discord.ButtonStyle.green if perms.read else discord.ButtonStyle.red
    read_text = "Read: YES" if perms.read else "Read: NO"
    read_emoji = "✅" if perms.read else "✖️"
    read_locked = perms.admin
    
    write_style = discord.ButtonStyle.green if perms.write else discord.ButtonStyle.red
    write_text = "Write: YES" if perms.write else "Write: NO"
    write_emoji = "✅" if perms.write else "✖️"
    write_locked = perms.admin

    admin_style = discord.ButtonStyle.green if perms.admin else discord.ButtonStyle.red
    admin_text = "Admin: YES" if perms.admin else "Admin: NO"
    admin_emoji = "✅" if perms.admin else "✖️"
    admin_locked = not caller_perms.owner

    class _PermsManagementButtons(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=None)

        @discord.ui.button(label=read_text, style=read_style, emoji=read_emoji, disabled=read_locked)
        async def toogle_read(self, interaction: discord.Interaction, button: discord.ui.Button):
            if not read_locked:
                perms.update(read=not perms.read)
                
            drive_man = await data.DriveGuild.get(interaction.guild)
            await drive_man.set_permissions(user, perms)
            await interaction.response.edit_message(view=_build_perms_toogle_buttons(caller_perms, user, perms))

        @discord.ui.button(label=write_text, style=write_style, emoji=write_emoji, disabled=write_locked)
        async def toogle_write(self, interaction: discord.Interaction, button: discord.ui.Button):
            if not write_locked:
                perms.update(write=not perms.write)
                
            drive_man = await data.DriveGuild.get(interaction.guild)
            await drive_man.set_permissions(user, perms)
            await interaction.response.edit_message(view=_build_perms_toogle_buttons(caller_perms, user, perms))

        @discord.ui.button(label=admin_text, style=admin_style, emoji=admin_emoji, disabled=admin_locked)
        async def toogle_admin(self, interaction: discord.Interaction, button: discord.ui.Button):
            if not admin_locked:
                perms.update(admin=not perms.admin)
                
            drive_man = await data.DriveGuild.get(interaction.guild)
            await drive_man.set_permissions(user, perms)
            await interaction.response.edit_message(view=_build_perms_toogle_buttons(caller_perms, user, perms))
            
    return _PermsManagementButtons()


class BotTreeCommands(commands.Cog):
    def __init__(self, client: discord.Client) -> None:
        self.client = client

    @app_commands.command(name="ping", description="🏓 Check if bot is responding.")
    async def ping(self, interaction: discord.Interaction):
        await interaction.response.send_message(embed=discord.Embed(title="Pong 🏓", color=PRIMARY_COLOR), ephemeral=True)

    @app_commands.command(name="register", description="👤 Register DriveCord account.")
    async def manual_register(self, interaction: discord.Interaction):
        if str(interaction.user.id) in accounts.users_db.get_all_keys():
            embed_error = discord.Embed(
                title="This account is already registered.",
                color=discord.Color.red()
            )
            return await interaction.response.send_message(embed=embed_error)
            
        start_registration_embed = discord.Embed(
            color=PRIMARY_COLOR,
            description=f"# {EMOJI_ACCOUNT} | Register account.\nStart using **DriveCord** with Your **Discord** account.",
        )
        
        await interaction.response.send_message(embed=start_registration_embed, view=setups.RegisterUserButton(), ephemeral=True)

    @app_commands.command(name="permissions", description="🔒 Manage users permissions.")
    async def manage_perms(self, interaction: discord.Interaction, member: discord.Member):
        if isinstance(interaction.channel, discord.DMChannel):
            embed_error = discord.Embed(
                title="This command can be used only within DriveCord server instance.",
                color=discord.Color.red()
            )
            return await interaction.response.send_message(embed=embed_error)

        if str(interaction.user.id) not in accounts.users_db.get_all_keys():
            embed_error = discord.Embed(
                title="You are not registered DriveCord user. (Use /register)",
                color=discord.Color.red()
            )
            return await interaction.response.send_message(embed=embed_error)
          
        drive_man = await data.DriveGuild.get(interaction.guild)
        caller_perms = drive_man.get_permissions(interaction.user)
        member_perms = drive_man.get_permissions(member)
        
        if not caller_perms.admin:
            return await interaction.response.send_message(embed=perms.ADMIN_PERMS_ERROR_EMBED, ephemeral=True)

        if str(member.id) not in accounts.users_db.get_all_keys():
            embed_error = discord.Embed(
                title=f"Selected user: `{member.name}` has no DriveCord account yet. They cannot interract with the drive.",
                color=discord.Color.red()
            )
            return await interaction.response.send_message(embed=embed_error, ephemeral=True)

        if member_perms.owner:
            embed_error = discord.Embed(
                title=f"Selected user: `{member.name}` has `OWNER` permission. Their permissions cannot be managed.",
                color=discord.Color.red()
            )
            return await interaction.response.send_message(embed=embed_error, ephemeral=True)
            
        if member_perms.admin and not caller_perms.owner:
            embed_error = discord.Embed(
                title=f"Selected user: `{member.name}` has `ADMIN` permission. To manage their permissions `OWNER` permission is required.",
                color=discord.Color.red()
            )
            return await interaction.response.send_message(embed=embed_error, ephemeral=True)

        toogle_buttons = _build_perms_toogle_buttons(caller_perms, member, member_perms)
        embed_info = discord.Embed(
            title=f"{EMOJI_ACCOUNT} | Manage `{member.name}` permissions.",
            description="Use buttons below to toogle user's permissions.",
            color=PRIMARY_COLOR
        )
        
        return await interaction.response.send_message(embed=embed_info, ephemeral=True, view=toogle_buttons)


async def setup(client: discord.Client) -> None:
    await client.add_cog(BotTreeCommands(client))    