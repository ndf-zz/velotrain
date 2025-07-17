# velotrain

velotrain reads raw transponder passing information from a set
of up to 9 measurement points, filters the messages according
to track configuration parameters and then emits valid timing
measurements as JSON-encoded objects to MQTT.


## Operation

The velodrome is divided into a closed loop of timing sectors
by measurement points at each of the interesting offsets, for example:

   - Finish line
   - 200m Start
   - Pursuit B
   - Pursuit A

A rider is considered to be in a 'run' if their speed over any
configured sector (eg Finish line to 200m start) is between
configured values (default: 30 km/h to 90 km/h).
Passings emitted for a rider in a run will include any available
lap, half-lap, quarter-lap, 100 m, and 50 m splits.
Rider passings slower or faster than the configured limits are
considered isolated, and will be reported without splits.

Where possible, valid raw passings received out of order will be
corrected and re-ordered before being emitted. In the case that
an isolated raw passing violates the configured track ordering and 
speed limits, it may be reported out of order.

For a Protime active/passive dual loop configuration, riders
will not be registered at all until they are moving faster than
about 30 km/h, no matter what software limits have been configured.


## Configuration

Track layout, system transponder ids and speed limits are configured
using a metarace jsonconfig entry with the section 'velotrain' and the
following keys:

   - authkey: (string) reset authorisation string, default: null
   - gate: (string) transponder ID for start gate, default: null
   - gatedelay: (string) trigger delay for start gate, default: '0.075' _[1]_
   - gatesrc: (string) channel start gate reports to eg 'C2', default: null
   - laplen: (float) lap length in metres, default: 250.0
   - maxspeed: (float) maximum sector speed in km/d, default: 90.0
   - minspeed: (float) minimum sector speed in km/h, default: 38.0
   - moto: (list) list of transponder IDs attached to motos, default: []
   - trig: (string) transponder ID of sync trigger messages, default: '255'
   - passlevel: (integer) minimum accepted passing level, default 40 _[2]_
   - uaddr: (string) UDP host listen address, default ''
   - uport: (integer) UDP host port, default: 2008
   - bcast: (string) broadcast address for timing LAN,
     default: '255.255.255.255'
   - basetopic: (string) base topic for telegraph interface,
     default: 'velotrain'
   - sync: (string) channel of synchronisation master unit, default: null
   - mingate: (float) minimum accepted speed in km/h over the
     start gate sector, default: 9.0
   - maxgate: (float) maximum accepted speed in km/h over the
     start gate sector, default: 22.5
   - dhi: (list) ['address', port] DHI port for Caprica scoreboard,
     default: null
   - dhiencoding: (string) text encoding for DHI messages, default: 'utf-8'
   - mpseq: (list) ordering of channels on the track,
     default: ['C1', 'C9', 'C4', 'C6', 'C3', 'C5', 'C7', 'C8', 'C2'] _[3]_
   - mps: (dict) mapping of channel IDs to measurement point configs,
     default: {} _[4]_

Configured measurement points divide the track into a closed loop of
timing sectors, with a set of splits at each measurement point.
Measurement point config entries have the following options, omitted keys
receive a default value:

   - name: (string) visible name of the measurement point,
     default: channel ID
   - ip: (string) IP address of connected Protime decoder,
     default: null _[4]_
   - offset: (float) distance in metres from finish line to measurement 
     point, default: null
   - half: (string) channel ID corresponding to a half lap before this unit,
     default: null
   - qtr: (string) channel ID corresponding to quarter lap before this unit,
     default: null
   - 200: (string) channel ID corresponding to 200 m before this unit,
     default: null
   - 100: (string) channel ID corresponding to 100 m before this unit,
     default: null
   - 50: (string) channel ID corresponding to 50 m before this unit,
     default: null

Notes:

   1. 0.075 seconds is LS transponder delay when triggered by
      start gate release
   2. Sets the 'level' option in all attached Protime decoder units
   3. Default channel ordering matches trackmeet, all configured units
      must appear once in this sequence, however the sequence may contain
      more channels than are configured.
   4. Measurement points require a non-null IP entry to be configured.
      For use with foreign timers, set the IP to be an empty string: ''.
      Refer to the sample velomon.json for an example setting


## Telegraph (MQTT) Interface

The topics below are relative to the configured basetopic (default:
'velotrain'), from the perspective of the velotrain process. For
all JSON encoded objects, invalid or unavailable properties will be
reported as null.


### passing (publish)

Issue filtered and accepted passing records. Passing records
are JSON encoded objects with the following properties:

   - index : (integer) index of the passing record (reset to 0 each day)
   - date: (string) Date of the passing formatted YYYY-MM-DD
   - time: (string) Time of day of the passing formatted HH:MM:SS.dcm
   - mpid: (integer) Measurement point ID 0 - 9
   - refid: (string) Transponder ID or system passing ID 'gate', 'moto'
     or 'marker'
   - env: (list) [temperature, humidity, pressure] where each value
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
			 "time": "16:18:32.300", "mpid": 5,
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
     formatted HH:MM:SS.dcm
   - mpid: (integer) Measurement point ID 0 - 9
   - refid: (string) Raw transponder ID
   - env: (list) [temperature, humidity, pressure] where each value
     is a float value in units degrees Celsius, %rh, and hPa respectively
   - name: (string) Name of measurement point
   - info: (string) Extra information provided by decoder

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
   - env: (list) [temperature, humidity, pressure] where each value
     is a float value in units degrees Celsius, %rh, and hPa respectively
   - gate: (string) Time of day of last gate trigger HH:MM:SS.dc
   - battery: (list) List of transponder refids that have reported a low
     battery warning since the last system reset
   - info: (string) Status info, one of: 'running', 'resetting',
     'error', 'offline' _[1]_
   - units: (list) List of JSON encoded objects, each containing a measurement
     point status:
       - mpid: (integer) Measurement point ID
       - name: (string) Measurement point name
       - noise: (integer) Interference noise value 0 - 100. Values under
         40 indicate normal operation. Larger values indicate interference.
       - offset: (string) Unit clock offset from system time in seconds
         formatted as [-]s.dcm

Notes:

   1. Status info strings:
       - running: Normal operation
       - resetting: System reset in progress
       - offline: velotrain was shutdown (date and time fields
         indicate when shutdown ocurred)
       - error: Network connection lost, server status temporarily
         unavailable

Example: Status update

	Topic:		velotrain/status
	Payload:	{"date": "2022-07-07", "time": "23:04:00.15",
			 "offset": "0.211", "env": [13.1, 62.0, 1013.0],
			 "count": 123, "gate": null, "battery": ["123876"],
			 "info": "running",
			 "units": [
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

Receive requests for replay of passing records to the nominated serial,
filtered according to the JSON encoded request object:

   - serial (string) optional request serial, appended to replay topic
   - index (list) optional [first, last] limit response to include
     passings from index first to index last.
   - time (list) optional [starttime, endtime] limit response to
     passings between starttime and endtime strings formatted HH:MM:SS.dc
   - mpid (list) optional [mpid, ...] include only the nominated
     mpids in replay
   - refid (list) optional [refid, ...] include only nominated
     transponder ids in response
   - marker (list) optional [marker, ...] include only passings
     following the nominated marker strings

Example: Request all passings, publish to default topic

	Topic:		velotrain/request
	Payload:	b''
	
	Reply topic:	velotrain/replay
	Payload:	[{"index":0, ...}, {"index":1, ...}, ...]

Request a replay of passing 254 to 'xxfd'

	Topic:		velotrain/request
	Payload:	{"serial": "xxfd", "index": 254}
	
	Reply topic:	velotrain/replay/xxfd
	Payload:	[{"index": 254, ... }]

Replay passings from transponder '123456' and start gate after midday
before index 10000 and that occur following manual markers 'one' and 'two'

	Topic:		velotrain/request
	Payload:	{"refid": ["123456", "gate"],
			 "time": ["12:00:00", null],
			 "index": [null, 10000],
			 "marker": ["one", "two"]}
	
	Reply topic:	velotrain/replay
	Payload:	[{"index":12, "text":"one","mpid":0, ...}, ...]


### replay[/serial] (publish)

Issues a JSON encoded list of passing objects, filtered according
to a provided request.


### marker (subscribe)

Insert the provided utf-8 encoded unicode payload as a manual marker
passing. If the supplied payload is empty, 'marker' is used.

Example: Insert an emoticon '🤷' (U+1F937) at the current time of day

	Topic:		velotrain/marker
	Payload:	b'\xf0\x9f\xa4\xb7'
	
	Reply topic:	velotrain/passing
	Payload:	{"index": 143, "date": "2022-07-06",
			 "time": "12:09:13.33", "mpid": 0,
			 "refid": "marker", "env": null, "moto": null,
			 "elap": null, "lap": null, "half": null,
			 "qtr": null, "200": null, "100": null,
			 "50": null, "text": "\ud83e\udd37"}

### reset (subscribe)

Start a reset process, authorised by the provided utf-8 encoded 
authkey (config option 'authkey'). For a Tag Heuer Protime decoder network,
this process may take up to three minutes to complete. For systems
with foreign timers, this request will just clear passing records.

Example: Request reset process using authkey 'qwertyuiop'

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
different mechanism, a fake top of minute trigger can be sent
with the channel of the configured sync source (config key 'sync')
at any time using the trig refid and a TOD of '0'.

Example: Insert a passing for transponder '123456' on mpid 3 at the current time

	Topic:		velotrain/timer
	Payload:	b';C3;;123456;now'
	
	Reply topic:	velotrain/rawpass
	Payload:	{"date": "2022-07-06", "env": null,
			 "refid": "123456", "mpid": 3, "name": "Pursuit B",
			 "time": "12:23:35.879", "rcv": "12:23:35.879"}

Fake a top-of-minute message to mpid 1, using the default trig:

	Topic:		velotrain/timer
	Payload:	b';C1;;255;0'

	Reply topic:	velotrain/rawpass
	Payload:	{"date": "2022-07-06", "env": null,
			 "refid": "255", "mpid": 1, "name": "Finish",
			 "time": "00:00:00.000", "rcv": "12:25:06.090"}

Insert a passing for transponder '123456' on mpid 4, 10.2345 seconds after 2 pm:

	Topic:		velotrain/timer
	Payload:	b';C4;;123456;14:00:10.2345'

	Reply topic:	velotrain/rawpass
	Payload:	{"date": "2022-07-06", "env": null,
			 "refid": "123456", "mpid": 4, "name": "200m Start",
			 "time": "14:00:10.234", "rcv": "12:27:38.988"}


### resetunit (subscribe)

Attempt to stop, start and re-synchronise the single measurement
point unit specified in the message body. Note that the synchronisation
master may not be re-started this way.

Example: Reset unit "C4"

	Topic:		velotrain/resetunit
	Payload:	b'C4'


## Requirements

   - metarace (>= 2.1.14)
   - ypmeteo


## Installation

	$ pip3 install velotrain

To use as a systemd service, edit the provided unit file
and copy to /etc/systemd/system, then enable with:

	# systemctl enable velotrain
	# systemctl start velotrain
