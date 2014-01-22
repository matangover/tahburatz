import sqlite3
import urllib
import os
import requests
import time
import logging

DB_FILENAME = os.path.join(os.path.dirname(__file__), "tahburatz.db")
STATIONS = (
    2591, # tschernichovsky / fichman -- direction to university
    5885, # shukenion / agripas -- direction of 6 to malha
    3523, # shuk mahane yehuda / agripas -- direction of 6 to pisgat zeev
    9870, # shuk mahane yehuda / agripas -- direction of 6 to malha
    6007, # shukenion / agripas -- direction of 6 to pisgat zeev
    )    
SECONDS_TO_WAIT = 120
MAX_ERRORS = 100

def create_table():
    global conn
    conn = connect()
    # Create table
    c = conn.cursor()
    c.execute("""
    CREATE TABLE realtime_logs (id INTEGER PRIMARY KEY, timestamp datetime default current_timestamp,
    line TEXT, station TEXT, remaining_minutes NUMERIC, destination TEXT);
    """)
    conn.commit()
    conn.close()
    conn = None
    
def connect():
    logging.info("Opening database connection...")
    return sqlite3.connect(DB_FILENAME)

def insert(line, station, remaining_minutes, destination):
    cursor.execute("""
    INSERT INTO realtime_logs(line, station, remaining_minutes, destination) VALUES('%s', '%s', %s, '%s');
    """ % (line, station, remaining_minutes, destination))
    conn.commit()

def get_data(station):
    headers = {
        "Origin": "http://mslworld.egged.co.il",
    }
    data = {"Task": "NextBusTime", "StopId": str(station), "lang": "he"}
    response = requests.post(
        "http://mslworld.egged.co.il/EggedTimeTable/ASHX/NextBus.ashx/ProcessRequest",
        data=data, headers=headers)
    return response.text
    
def log_realtime_info(station):
    t = get_data(station)
    logging.info("Response for station %s:\n%s" % (station, t))
    if t == "":
        logging.debug("No data")
        return
        
    bus_lines = t.split("+")
    for line in bus_lines:
        parts = [p.strip() for p in line.split("|")]
        remaining_minutes, line_number, destination, comments = parts
        insert(line_number, station, remaining_minutes, destination)
        if comments != "":
            logging.info("Found comment: %s" % comments);

def main():
    global conn, cursor
    logging.basicConfig(filename='tahburatz.log',level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
    conn = connect()
    cursor = conn.cursor()
    counter = 0
    error_count = 0
    while True:
        logging.debug("Getting station info, #%s" % counter)
        for station in STATIONS:
            try:
                log_realtime_info(station)
            except Exception as e:
                logging.exception("Error!")
                error_count += 1
                if error_count >= MAX_ERRORS:
                    logging.warning("Max error count reached. Exiting")
                    break
                
        counter = counter + 1
        logging.debug("Sleeping...")
        time.sleep(SECONDS_TO_WAIT)
    
    conn.commit()
    conn.close()
    
if __name__ == "__main__":
    main()
