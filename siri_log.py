import sqlite3
import os
import time
import datetime
import logging
import siri
import collections
from xml.etree import ElementTree
import dateutil.parser

DB_FILENAME = os.path.join(os.path.dirname(__file__), "siri_log.db")
SECONDS_TO_WAIT = 60
MAX_ERRORS = 100
BusData = collections.namedtuple("BusData", "station_id, next_bus_time, aimed_departure, latitude, longitude, line_id, data_timestamp, is_realtime")

def create_table():
    global conn
    conn = connect()
    # Create table
    c = conn.cursor()
    c.execute("""
    CREATE TABLE realtime_logs (id INTEGER PRIMARY KEY, timestamp datetime default current_timestamp,
    station_id TEXT, next_bus_time DATETIME, aimed_departure DATETIME, latitude TEXT, longitude TEXT, line_id TEXT, data_timestamp DATETIME, is_realtime BOOLEAN);
    """)

    conn.commit()
    
    c.execute("""
    CREATE TABLE realtime_trips (line_id TEXT, aimed_departure TEXT, station_id TEXT,
    arrival_time DATETIME, data_timestamp TEXT, timestamp datetime default current_timestamp, PRIMARY KEY(line_id, aimed_departure, station_id));
    """)
    # TODO: add constraint - unique aimed_departure and station_id
    conn.commit()
    
    conn.close()
    conn = None
    
def connect():
    logging.info("Opening database connection...")
    return sqlite3.connect(DB_FILENAME, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

def insert(bus_data):
    cursor.execute("""
    INSERT INTO realtime_logs(station_id, next_bus_time, aimed_departure, latitude, longitude, line_id, data_timestamp, is_realtime) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');
    """ % bus_data)
    conn.commit()
    
def log_siri_info():
    response_text = siri.send_request()
    logging.info("Got response, length: " + str(len(response_text)))
    # siri.save_response(response_text)
    bus_data = parse_siri_response(response_text)
    logging.info("Adding %s new rows" % len(bus_data))
    for data in bus_data:
        insert(data)
    for data in bus_data:
        process_stop_data(data)

def parse_siri_response(response_text):
    lines_filter = siri.route_ids
    root = ElementTree.fromstring(response_text)
    ns = "http://www.siri.org.uk/siri"
    visits = root.findall(".//{%s}MonitoredStopVisit" % ns)
    siri_find = lambda el, path: el.find("{%s}%s" % (ns, path))
    bus_data = []
    for visit in visits:
        journey = siri_find(visit, "MonitoredVehicleJourney")
        line_id = siri_find(journey, "LineRef").text
        if line_id not in lines_filter:
            continue
        data_timestamp = siri_find(visit, "RecordedAtTime").text
        monitored_call = siri_find(journey, "MonitoredCall")
        stop_code = siri_find(monitored_call, "StopPointRef").text
        longitude = None
        latitude = None
        vehicle_location = siri_find(journey, "VehicleLocation")
        if vehicle_location is not None:
            longitude = siri_find(vehicle_location, "Longitude").text
            latitude = siri_find(vehicle_location, "Latitude").text
        origin_aimed_departude_time = None
        origin_aimed_departure_time_element = siri_find(journey, "OriginAimedDepartureTime")
        if origin_aimed_departure_time_element is not None:
            origin_aimed_departude_time = origin_aimed_departure_time_element.text
        expected_arrival_time = siri_find(monitored_call, "ExpectedArrivalTime").text
        # if MonitoredCall contains AimedArrivalTime, it's not realtime data - even ExpectedArrivalTime [apparently]
        is_realtime = siri_find(monitored_call, "AimedArrivalTime") is None
        
        data = BusData(stop_code, expected_arrival_time, origin_aimed_departude_time, latitude, longitude, line_id, data_timestamp, is_realtime)
        bus_data.append(data)
    return bus_data

def process_stop_data(data):
    if not data.is_realtime:
        return
    # Don't rely on forecasts for more than 10 minutes ahead
    if dateutil.parser.parse(data.next_bus_time) - dateutil.parser.parse(data.data_timestamp) > datetime.timedelta(0, 60*10):
        # This should happen because we're limiting the preview interval to 10 minutes in the request, but check anyway.
        logging.debug("Found realtime prediction outside limit. Expected arrival time: %s. Data timestamp: %s" % (data.next_bus_time, data.data_timestamp))
        return
    cur = conn.cursor()
    # TODO: convert to stored procedure
    cur.execute("""
    SELECT 1 FROM realtime_trips WHERE line_id=? and aimed_departure=? and station_id=?
    """, (data.line_id, data.aimed_departure, data.station_id))
    row = cur.fetchone()
    if row is None:
        cur.execute("""
        INSERT INTO realtime_trips(aimed_departure, station_id, arrival_time, line_id, data_timestamp)
        VALUES(?, ?, ?, ?, ?)""",
        (data.aimed_departure, data.station_id, data.next_bus_time, data.line_id, data.data_timestamp))
    else:
        cur.execute("""
        UPDATE realtime_trips
        SET arrival_time=?, data_timestamp=?, timestamp=datetime()
        WHERE line_id=? AND aimed_departure=? AND station_id=?
        """, (data.next_bus_time, data.data_timestamp, data.line_id, data.aimed_departure, data.station_id))
    conn.commit()

def configure_logging():
    logging.basicConfig(filename='siri.log', level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

def connect_to_db():
    global conn, cursor
    conn = connect()
    cursor = conn.cursor()
    
def main():
    configure_logging()
    connect_to_db()
    
    counter = 0
    error_count = 0
    while True:
        logging.debug("Getting SIRI info, #%s" % counter)
        try:
            log_siri_info()
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
