""" Current limits allow for 1GiB storage. """
MIN_MSG_PER_CHANNEL = 351
MAX_CHANNELS_PER_BUCKET = 48
MAX_BUCKETS = 30
MSG_SIZE = 1950  # 50 for header
TOTAL_CHANNEL_CONTENT_SIZE = MSG_SIZE * MIN_MSG_PER_CHANNEL  # 694980 (* total channel = 1Gb)
DISCORD_FILE_SIZE_B = 10 * 1000 * 1000  # 10MiB

MAX_ACCESS_TOKENS = 3