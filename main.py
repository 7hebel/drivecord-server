from modules.discord.client import client
from modules import logs
from modules import api

import threading
import uvicorn


def main():
    discord_thread = threading.Thread(target=client.run, args=(client._token, ), kwargs={"log_formatter": logs._DCLogFormatter()}, daemon=True)
    discord_thread.start()
    uvicorn.run(api.api, host="localhost", port=8000)


if __name__ == "__main__":
    main()
