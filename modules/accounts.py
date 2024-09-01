from modules import timestamp
from modules.logs import Log
from modules import database
from modules import errors


@database.DBModel.model("users", "!discord_id")
class User:
    discord_id: int
    password: str
    date_join: int
    servers_ids: list = database.NOT_REQUIRED
    is_banned: bool = False

    @staticmethod
    def register(discord_id: int, password: str) -> "User":
        if str(discord_id) in users_db.get_all_keys():
            Log.error(f"Already registerd user tried to register: {discord_id}")
            return errors.ACCOUNT_ALREADY_REGISTERED

        user_model = User(
            discord_id=discord_id,
            password=password,
            date_join=timestamp.generate_timestamp(),
            servers_ids=[],
            is_banned=False
        )

        Log.info(f"Registered account via Discord modal: {discord_id}")
        return users_db.insert(user_model)


users_db = database.Database(User)
