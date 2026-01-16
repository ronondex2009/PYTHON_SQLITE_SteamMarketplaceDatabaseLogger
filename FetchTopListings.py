#!/usr/bin/python
import requests
from bs4 import BeautifulSoup
import datetime
import sqlite3
import time
import json

NUMBER_OF_PAGES = 20
LOOP = True
LOOP_WAIT = 1600
conn = sqlite3.connect("./SteamMarketplace.db")

def get_marketplace_rows_from_URL(URL):
    """
    Fetches top marketplace entries
    returns array of tupes of (string item_name, string game_name, int count, float price_starting_at, string link)
    """
    # Retry the request if it returns 429 (too many requests) otherwise continue
    wait_time = 60
    while True:
        marketplace_request = requests.get(URL)
        if marketplace_request.status_code == 429:
            print("Status code 429 waiting and retrying...")
            # Timer
            for i in range(wait_time):
                print(f"{wait_time-i} ...", end="\r")
                time.sleep(1)
            wait_time *= 2 # Wait longer every attempt
        else:
            break
    # If after attempts, we dont get a 200 OK then the request failed for some reason.
    print("Status code", marketplace_request.status_code)
    if not marketplace_request.status_code == 200:
        print("Request Not OK")
        return []
    soup = BeautifulSoup(json.loads(marketplace_request.content)["results_html"], "lxml")   # Make BeautifulSoup xml object
    rows_xml = soup.find_all(class_="market_listing_row_link")  # Get rows of soup objects for every item in the page
    rows = []
    for row_xml in rows_xml:                                    # Parse the row boxes into data tuples
        rows.append((
            row_xml.find(class_="market_listing_item_name").get_text(),
            row_xml.find(class_="market_listing_game_name").get_text(),
            int(row_xml.find(class_="market_listing_num_listings_qty").get_text().replace(',','')),
            float(row_xml.find(class_="normal_price").find("span").get_text()[1::]),
            row_xml["href"],
        ))
    return rows

def get_marketplace_rows(num_requests):
    """get multiple pages of responses of rows."""
    rows = []
    # Generate URL queries for every page, then run get_marketplace_rows_from_URL(1) on them.
    for i in range(num_requests):
        URL = f"https://steamcommunity.com/market/search/render/?query=&start={i*10}&count=10&search_descriptions=0&sort_column=popular&sort_dir=desc"
        print(URL)
        _rows = get_marketplace_rows_from_URL(URL)
        print(f"{URL} fetched {len(_rows)} results.")
        rows.extend(_rows)
        time.sleep(1)
    print(f"Fetched a total of {len(rows)} rows")
    return rows

def write_to_top_results_database(rows):
    # Create connection, check database schema and create it if its not there, and then write all tuples to the database.
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS top_listings (item_name NVARCHAR(64) NOT NULL, game_name NVARCHAR(64) , quantity INTEGER, normal_price DECIMAL(9, 2), link NVARCHAR(256), during DATETIME);")
    cursor.executemany("INSERT INTO top_listings (item_name, game_name, quantity, normal_price, link, during) VALUES (?, ?, ?, ?, ?, STRFTIME('%s', 'now'))", rows)
    conn.commit()

def update_price_changes_diff():
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM (
            -- lag function to calculate delta on top two most recent results per item_name
            SELECT
                t1.item_name,
                t1.normal_price - LAG(t1.normal_price, 1)
                    OVER (PARTITION BY t1.item_name ORDER BY t1.during) price_delta,
                t1.normal_price,
                t1.during
            FROM top_listings t1
            -- WHERE top 2 most recent results for each item_name
            WHERE t1.during IN (
                SELECT during
                FROM top_listings t2
                WHERE t1.item_name = t2.item_name
                ORDER BY during DESC
                LIMIT 2
            )
            -- WHERE there is more than one result for each item_name
            AND 1 < (
                SELECT COUNT(*)
                FROM top_listings t2
                WHERE t1.item_name = t2.item_name
            )
        )
        WHERE price_delta IS NOT NULL
        ORDER BY during DESC
    """)
    string_lines = []
    for entry in cursor.fetchall():
        name, price_diff, price, when = entry[0], entry[1], entry[2], entry[3]
        diff_char = '+' if price_diff < 0 else '-'
        diff_char = ' ' if price_diff == 0 else diff_char
        string_lines.append(f"{diff_char} {name:<64} {price_diff:<9.2f} {price:<9.2f} {when}")
    with open("./price_diff.diff", "w") as f:
        f.write('\n'.join(string_lines))

if __name__=="__main__":
    while True:
        rows = get_marketplace_rows(NUMBER_OF_PAGES) # Get tuples
        # Print tuples
        print(f"{"Item Name":<64}{"Game Name":<64}{"Normal Price":<12}{"Quantity":<16}")
        for row in rows:
            print(f"{row[0]:<64}{row[1]:<64}{row[2]:<12}{row[3]:<16}")
        write_to_top_results_database(rows) # Write tuples
        update_price_changes_diff()
        if LOOP:
            time.sleep(LOOP_WAIT)
        else:
            break
