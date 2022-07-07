# velotrain

velotrain reads raw transponder passing information from a set
of up to 9 Chronelec Protime decoders, filters the messages according
to track configuration parameters and then emits valid timing
measurements as JSON-encoded objects to MQTT.


## Telegraph (MQTT) Interface

The topics below are relative to the configured basetopic (default:
'velotrain'), from the persepective of the velotrain process. For
all JSON encoded objects, invalid or unavailable properties will be
reported as null. Examples use the default basetopic.


### passing (publish)

Issue filtered and accepted passing records. Passing records
are JSON encoded objects with the following properties:

   - index : (integer) index of the passing record (reset to 0 each day)
   - date: (string) Date of the passing formatted YYYY-MM-DD
   - time: (string) Time of day of the passing formatted HH:MM:SS.dc
   - mpid: (integer) Measurement point ID 0 - 9
   - refid: (string) Transponder ID or system passing ID 'gate', 'moto'
     or 'marker'
   - env: (list) [tmperature, humidity, pressure] where each value
     is a float value in units degrees Celsius, %rh, and hPa respectively
   - moto: (string) Proximity to moto in seconds if drafted
   - elap: (string) Elapsed time since last start gate or start of run
     formatted HH:MM:SS.dc
   - lap: (string) Time for the last full lap to the measurement point
   - half: (string) Time for the last half lap to the measurement point
   - qtr: (string) Time for the last quarter lap to the measurement point
   - 200: (string) Time for the last 200m to the measurement point
   - 100: (string) Time for the last 100m to the measurement point
   - 50: (string) Time for the last 50m to the measurement point
   - text: (string) Name of measurement point, or user-supplied text
     for markers

Example: A moto passing over the 100m split at about 4:18pm

	Topic:		velotrain/passing
	Payload:	{"index": 108, "date": "2022-07-06",
			 "time": "16:18:32.30", "mpid": 5,
			 "refid": "moto", "env": [16.3, 55.0, 1015.0],
			 "moto": "0.00", "elap": "2:10.51",
			 "lap": "22.95", "half": null, "qtr": null,
			 "200": "18.32", "100": "9.07", "50": "4.52",
			 "text": "100m Split"}

### rawpass (publish)

Issue raw, unprocessed passing records as received from decoders. Raw
passing records are JSON encoded objects with the following properties:

   - date: (string) Date of the raw passing formatted YYYY-MM-DD
   - time: (string) Time of day of the passing formatted HH:MM:SS.dc
   - rcv: (string) Time of day passing was received by host system,
     formatted HH:MM:SS.dc
   - mpid: (integer) Measurement point ID 0 - 9
   - refid: (string) Raw transponder ID
   - env: (list) [tmperature, humidity, pressure] where each value
     is a float value in units degrees Celsius, %rh, and hPa respectively
   - name: (string) Name of measurement point

Example: The raw passing that might have generated the above moto passing

	Topic:		velotrain/rawpass
	Payload:	{"date": "2022-07-06", "env": [16.3, 55.0, 1015.0],
			 "refid": "125328", "mpid": 5,
			 "name": "100m Split", "time": "16:18:32.303",
			 "rcv": "16:18:32.459"}

### status (publish)

Issue system and measurement point status records at the top of each
minute. Status records are JSON encoded objects with the following properties:

   - date: (string) Date of the status record formatted YYYY-MM-DD
   - time: (string) Time of day status was issued formatted HH:MM:SS.dc
   - offset: (string) Rough offset of system clock to UTC
   - count: (integer) Count of passing records
   - env: (list) [tmperature, humidity, pressure] where each value is a float value in units degrees Celsius, %rh, and hPa respectively
   - time: (string) Time of day of last gate trigger HH:MM:SS.dc
   - name: (string) Name of measurement point

Example: Status update

	Topic:		velotrain/status
	Payload:	{"date": "2022-07-07", "time": "23:04:00.15",
			 "offset": "0.211", "env": [13.1, 62.0, 1013.0],
			 "count": 1, "gate": null, "units": [
			  {"mpid": 1, "name": "Finish",
                           "noise": 20, "offset": "0.000"},
			  {"mpid": 2, "name": "Pursuit A",
			   "noise": 32, "offset": "0.025"},
			  {"mpid": 3, "name": "Pursuit B",
			   "noise": 15, "offset": "0.005"},
			  {"mpid": 4, "name": "200m Start",
			   "noise": 32, "offset": "0.005"},
			  {"mpid": 5, "name": "100m Split",
			   "noise": 14, "offset": "-0.008"},
			  {"mpid": 6, "name": "50m Split",
			   "noise": 27, "offset": "0.064"},
			  {"mpid": 8, "name": "150m Split",
			   "noise": 18, "offset": "-0.020"}
			 ]}


### request (subscribe)

Receive requests for replay of passing records to the nominated serial
and filtered according to the optional JSON encoded object:

   - serial (string) optional request serial, appended to replay topic
   - index (list) [first, last] limit response to include passings from
     index first to index last.
   - time (list) [starttime, endtime] limit response to passingd between
     starttime and endtime strings formatted HH:MM:SS.dc
   - mpid (list) [mpid, ...] include only the nominated mpids in response
   - refid (list) [refid, ...] include only nominated transponder ids
     in response
   - marker (list) [marker, ...] include only passings following the
     nominated marker strings

Example: Request all passings, publish to default topic

	Topic:		velotrain/request
	Payload:	b''

Request a replay of passing 254 to 'xxfd'

	Topic:		velotrain/request
	Payload:	{"serial": "xxfd", "index": 254}

Replay passings from transponder '123456' and start gate after midday
before index 10000 and that occur following manual markers 'one' and 'two'

	Topic:		velotrain/request
	Payload:	{"refid": ["123456", "gate"],
			 "time": ["12:00:00", null],
			 "index": [null, 10000],
			 "marker": ["one", "two"]}


### replay[/serial] (publish)

Issues a JSON encoded list of passing objects, filtered according
to a provided request.


### marker (subscribe)

Insert the provided utf-8 encoded unicode payload as a manual marker
passing. If the supplied payload is empty, 'marker' is used.

Example: Insert an emoticon '🤷' (U+1F937) at the current time of day

	Topic:		velotrain/marker
	Payload:	b'\xf0\x9f\xa4\xb7'

### reset (subscribe)

Start a reset process, authorised by the provided utf-8 encoded 
authkey (config option 'authkey'). For a Tag Heuer Protime decoder network,
this process may take up to three minutes to complete. For systems
with foreign timers, this request will just clear passing records.

Example: Request reser process using authkey 'qwertyuiop'

	Topic:		velotrain/reset
	Payload:	b'qwertyuiop'


### timer (subscribe)

Receive raw passing data from a foreign timing device. Foreign timing
records are utf-8 encoded unicode strings in the format:

	INDEX;SOURCE;CHANNEL;REFID;TOD

Where the fields are as follows:

   - INDEX: ignored
   - SOURCE: measurement point id as a metarace timing channel
     eg: 'C1', 'C2', 'C3', ... 'C9'
   - CHANNEL: ignored
   - REFID: transponder ID eg '123456'
   - TOD: time of day of passing as string eg: '1:23.456', 'now', '12:34:56.789'

Note: velotrain expects each attached decoder to be triggered from a
reference clock at the top of each minute, and to report a passing with
refid matching config option 'trig' (default: '255').
For systems that maintain synchronisation via a
different mechanism, a fake top of minute tigger can be sent
with the channel of the configured sync source (config key 'sync')
at any time using the trig refid and a TOD of '0'.

Example: Insert a passing for transponder '123456' on mpid 3 at the current time

	Payload: b';C3;;123456;now'

Fake a top-of-minute message to mpid 1, using the default trig:

	Payload: b';C1;;255;0'

Insert a passing for transponder '123456' on mpid 4, 10.2345 seconds after 2 pm:

	Payload: b';C4;;123456;14:00:10.2345'


## Requirements

   - metarace (>= 2.0)
   - ypmeteo


## Installation

	$ pip3 install velotrain

To use as a systemd service, edit the provided unit file
and copy to /etc/systemd/system, then enable with:

	# systemctl enable velotrain
	# systemctl start
