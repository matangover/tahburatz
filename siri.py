import datetime
import uuid
import requests
from config import DEV_KEY, SERVER

#line14_stops_to_malha = ['10789', '9181', '10515', '10323', '9621', '9695', '9342', '34238', '10768', '34268', '9543', '9896', '11604', '9824', '9825', '9408', '9394', '9401', '9405', '11624', '9646', '9585', '9438', '9397', '9398', '9384', '9385', '9584', '9375', '9372', '10779', '9808', '10108', '35302', '11725', '11813', '10177']
#line14_stops_to_central = ["10177","10319","10062","9809","9593","9324","9373","9377","9381","9382","9458","9399","9603","9402","9405","9396","9620","10012","10739","9395","9822","9545","11847","11844","10768","11507","9903","9694","10325","10326","10324","8734","34273","11574"]
line14_stops_to_central = ["2923", "3093", "2715", "2181", "1629", "962", "1013", "1017", "1021", "1022", "1260", "1043", "1682", "1046", "1049", "1040", "1728", "2591", "4050", "1039", "2258", "1524", "9918", "9915", "4187", "5882", "2406", "1911", "3109", "3111", "3108", "203", "6138", "5949"]
line14_route_ids = [
#'10179', 
'10180',   # Weekdays
#'12834',
#'12835',
'12836',   # Friday
'12837'    # Saturday
]
"""
10179,3,14,תחנה מרכזית/יפו-ירושלים<->קניון מלחה/א''ס מכבי-ירושלים-1#,15014-1-#,3,
10180,3,14,קניון מלחה/א''ס מכבי-ירושלים<->תחנה מרכזית/יפו-ירושלים-2#,15014-2-#,3,
12834,3,14,תחנה מרכזית/יפו-ירושלים<->קניון מלחה/א''ס מכבי-ירושלים-1א,15014-1-א,3,
12835,3,14,תחנה מרכזית/יפו-ירושלים<->קניון מלחה/א''ס מכבי-ירושלים-1ב,15014-1-ב,3,
12836,3,14,קניון מלחה/א''ס מכבי-ירושלים<->תחנה מרכזית/יפו-ירושלים-2א,15014-2-א,3,
12837,3,14,קניון מלחה/א''ס מכבי-ירושלים<->תחנה מרכזית/יפו-ירושלים-2ב,15014-2-ב,3,
"""

def get_timestamp():
    return datetime.datetime.utcnow().isoformat()
    
def construct_request(stop_ids):
    timestamp = get_timestamp()
    monitoring_requests = "\n".join(
                """<siri:StopMonitoringRequest version="IL2.7" xsi:type="siri:StopMonitoringRequestStructure">
                    <siri:RequestTimestamp>{timestamp}</siri:RequestTimestamp>
                    <siri:MessageIdentifier xsi:type="siri:MessageQualifierStructure">{message_id}</siri:MessageIdentifier>
                    <siri:PreviewInterval>PT1H</siri:PreviewInterval>
                    <siri:MonitoringRef xsi:type="siri:MonitoringRefStructure">{stop_id}</siri:MonitoringRef>
                    <siri:MaximumStopVisits>100</siri:MaximumStopVisits>
                </siri:StopMonitoringRequest>""".format(timestamp=timestamp, stop_id=stop_id, message_id=i)
            for i, stop_id in enumerate(stop_ids))
            
    return """
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" xmlns:acsb="http://www.ifopt.org.uk/acsb" xmlns:datex2="http://datex2.eu/schema/1_0/1_0" xmlns:ifopt="http://www.ifopt.org.uk/ifopt" xmlns:siri="http://www.siri.org.uk/siri" xmlns:siriWS="http://new.webservice.namespace" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="./siri">
    <SOAP-ENV:Header />
    <SOAP-ENV:Body>
        <siriWS:GetStopMonitoringService>
            <Request xsi:type="siri:ServiceRequestStructure">
                <siri:RequestTimestamp>{timestamp}</siri:RequestTimestamp>
                <siri:RequestorRef xsi:type="siri:ParticipantRefStructure">{username}</siri:RequestorRef>
                <siri:MessageIdentifier xsi:type="siri:MessageQualifierStructure">{message_id}</siri:MessageIdentifier>
                {monitoring_requests}
            </Request>
        </siriWS:GetStopMonitoringService>
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
""".format(timestamp=timestamp, username=DEV_KEY, message_id=uuid.uuid4(), monitoring_requests=monitoring_requests)

SOAP_ADDRESS = SERVER + "Siri/SiriServices"

def send_request():
    request_data = construct_request(line14_stops_to_central)
    response = requests.post(SOAP_ADDRESS, data=request_data)
    response.raise_for_status()
    return response.text

def save_response(response_text):
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    output_filename = r"c:\temp\siri\response_" + timestamp + ".xml"
    open(output_filename, "wb").write(response_text.encode("utf8"))
    
def main():
    response_text = send_request()
    save_response(response_text)

if __name__ == "__main__":
    main()