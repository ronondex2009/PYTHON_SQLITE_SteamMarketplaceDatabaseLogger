# PYTHON_SQLITE_SteamMarketplaceDatabaseLogger
Simple python scripts to monitor the top listings on the steam marketplace. Writes to an embedded SQLite database.

# SETUP
Simply git clone this repository:
`> git clone PYTHON_SQLITE_SteamMarketplaceDatabaseLogger; cd PYTHON_SQLITE_SteamMarketplaceDatabaseLogger`
I highly recommend removing the diff and database. The script will automatically generate a clean version of both.
`> rm price_diff.diff SteamMarketplace.db;`
Afterwards, you can simply execute the script in two ways:
`./FetchTopListings.py` or `python FetchTopListings`
The script is automatically configured to run every 30 minutes and scrape the top 20 pages (resulting in about 400 entries per hour), and will automatically generate a price diff with a timestamp.
If you want to run the script automatically on startup, I suggest installing the crontab with the @reboot directive.
**Make sure you are in the same directory as the diff and database when you run the script, otherwise it will generate a new one in your current pwd directory.**
