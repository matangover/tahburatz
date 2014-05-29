import sqlite3
import os
import time
import logging
import siri
import collections
from xml.etree import ElementTree

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
    station_id TEXT, next_bus_time TEXT, aimed_departure TEXT, latitude TEXT, longitude TEXT, line_id TEXT, data_timestamp TEXT, is_realtime BOOLEAN);
    """)

    conn.commit()
    conn.close()
    conn = None
    
def connect():
    logging.info("Opening database connection...")
    return sqlite3.connect(DB_FILENAME)

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

def parse_siri_response(response_text):
    lines_filter = siri.line14_route_ids
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
