#!/usr/bin/python3
import requests
from bs4 import BeautifulSoup
import datetime
import sqlite3
import time

def get_marketplace_rows_from_URL(URL):
    """
    Fetches top marketplace entries
    returns array of tupes of (string item_name, string game_name, int count, float price_starting_at, string link, string datetime)
    """
    # Retry the request if it returns 429 (too many requests) otherwise continue
    wait_time = 30
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
    soup = BeautifulSoup(marketplace_request.content, "lxml")   # Make BeautifulSoup xml object
    rows_xml = soup.find_all(class_="market_listing_row_link")  # Get rows of soup objects for every item in the page
    rows = []
    for row_xml in rows_xml:                                    # Parse the row boxes into data tuples
        rows.append((
            row_xml.find(class_="market_listing_item_name").get_text(),
            row_xml.find(class_="market_listing_game_name").get_text(),
            int(row_xml.find(class_="market_listing_num_listings_qty").get_text().replace(',','')),
            float(row_xml.find(class_="normal_price").find("span").get_text()[1::]),
            row_xml["href"],
            datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%h:%m:%s")
        ))
    return rows

def get_marketplace_rows(numpages):
    rows = []
    # Generate URL queries for every page, then run get_marketplace_rows_from_URL(1) on them.
    for i in range(numpages):
        URL = f"https://steamcommunity.com/market/search?q=#p{i}_popular_desc"
        _rows = get_marketplace_rows_from_URL(URL)
        print(f"{URL} fetched {len(_rows)} results.")
        rows.extend(_rows)
        time.sleep(1)
    print(f"Fetched a total of {len(rows)} rows")
    return rows

def write_to_top_results_database(rows):
    # Create connection, check database schema and create it if its not there, and then write all tuples to the database.
    conn = sqlite3.connect("./SteamMarketplace.db")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS top_listings (item_name NVARCHAR(64) NOT NULL, game_name NVARCHAR(64) , quantity INTEGER, normal_price DECIMAL(9, 2), link NVARCHAR(256), during DATETIME);")
    cursor.executemany("INSERT INTO top_listings (item_name, game_name, quantity, normal_price, link, during) VALUES (?, ?, ?, ?, ?, ?)", rows)
    conn.commit()

if __name__=="__main__":
    # Get tuples
    rows = get_marketplace_rows(100)
    # Print tuples
    print(f"{"Item Name":<64}{"Game Name":<64}{"Normal Price":<12}{"Quantity":<16}")
    for row in rows:
        print(f"{row[0]:<64}{row[1]:<64}{row[2]:<12}{row[3]:<16}")
    # Write tuples
    write_to_top_results_database(rows)
