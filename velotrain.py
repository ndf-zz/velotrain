"""Velodrome Transponder Training Timing"""
__version__ = '1.0.3'

import logging
import os
import sys
import signal
import time
import threading
import queue
import socket
import json
import signal

import metarace
from ypmeteo import ypmeteo
from metarace import strops
from metarace import tod
from metarace import unt4
from metarace import jsonconfig
from metarace.telegraph import telegraph
from metarace.decoder.thbc import mcrf4xx
from metarace.comet import Comet

_LOGLEVEL = logging.DEBUG
_log = logging.getLogger('velotrain')
_log.setLevel(_LOGLEVEL)
_hlog = logging.getLogger('velotrain.hub')
_hlog.setLevel(_LOGLEVEL)

# add refid to low battery status after this many warnings
_LOWBATTWARN = 10
# optional configuration override file
_CONFIGFILE = 'velotrain.json'
# default decoder detection level
_PASSLEVEL = 40
# expiry threshold for decoder level status
_STATTHRESH = tod.tod('173')
# threshold for moto proximity report
_MOTOPROX = 1.0
# start logging decoder unit drift above this many seconds
_LOGDRIFT = 0.10
# automatically isolate new passings ISOTHRESH newer than last processed pass
_ISOTHRESH = tod.tod('30.0')
# expire run time after this long idle
_RUNIDLE = tod.tod('2:00')
# choke queue for no longer than ISOMAXAGE ~ 35km/h over 50m
_ISOMAXAGE = tod.tod('5.0')
# start gate trigger correction
_GATEDELAY = '0.075'
# default channel ordering
_DEFSEQ = ['C1', 'C9', 'C4', 'C6', 'C3', 'C5', 'C7', 'C8', 'C2']
# fallback track length
_DEFLAPLEN = 250.0
# default operational configuration
_CONFIG = {
    'gate': None,  # refid of start gate transponder
    'gatedelay': _GATEDELAY,  # time delay for start gate message
    'gatesrc': None,  # channel of start gate loop
    'moto': [],  # list of motorbike transponders
    'trig': '255',  # refid of sync trigger messages
    'passlevel': _PASSLEVEL,  # default read level in decoders
    'uaddr': '',  # UDP host ip (listen) address
    'uport': 2008,  # UDP host timing port
    'bcast': '255.255.255.255',  # broadcast address for timing LAN
    'basetopic': 'velotrain',  # MQTT base topic
    'sync': None,  # channel of synchronisation master unit
    'authkey': None,  # optional reset auth key
    'minspeed': 30.0,  # minimum sector speed
    'maxspeed': 90.0,  # maximum sector speed
    'mingate': 9.0,  # minimum gate start sector speed
    'maxgate': 22.5,  # maximum gate start sector speed
    'dhi': None,  # DHI address for scoreboard
    'dhiencoding': 'utf-8',  # encoding for DHI scoreboard comms
    'laplen': _DEFLAPLEN,  # lap length in m
    'mpseq': _DEFSEQ,  # ordering of measurement points
    'mps': {},  # measurement point config - default is empty
}

# default decoder configuration
_DECODERSANE = {
    'Time of Day': True,
    'GPS Sync': False,
    'Active Loop': False,
    'Detect Max': True,
    'Protocol': 0,
    'CELL Sync': False,
    'Sync Pulse': False,
    'Serial Print': False,
    'Timezone Hour': 0,
    'Timezone Min': 0,
}

# protime decoder constants
THBC_ENCODING = 'iso8859-1'
ESCAPE = b'\x1b'
HELOCMD = b'MR1'
STOPCMD = ESCAPE + b'\x13\x5c'
REPEATCMD = ESCAPE + b'\x12'
ACKCMD = ESCAPE + b'\x11'
STATCMD = ESCAPE + b'\x05'  # fetch status
CHKCMD = ESCAPE + b'\x06'  # UNKNOWN
STARTCMD = ESCAPE + b'\x07'  # start decoder
SETCMD = ESCAPE + b'\x08'  # set configuration
IPCMD = ESCAPE + b'\x09'  # set IP configuration
QUECMD = ESCAPE + b'\x10'  # fetch configuration
STALVL = ESCAPE + b'\x1e'
BOXLVL = ESCAPE + b'\x1f'
NACK = b'\x07'
CR = b'\x0d'
LF = b'\x0a'
SETTIME = ESCAPE + b'\x48'
STATSTART = b'['
PASSSTART = b'<'
MINREFID = 90000
MAXREFID = 150000

# decoder config consts
IPCONFIG_LEN = 16
CONFIG_LEN = 27
CONFIG_TOD = 0
CONFIG_GPS = 1
CONFIG_TZ_HOUR = 2
CONFIG_TZ_MIN = 3
CONFIG_485 = 4
CONFIG_FIBRE = 5
CONFIG_PRINT = 6
CONFIG_MAX = 7
CONFIG_PROT = 8
CONFIG_PULSE = 9
CONFIG_PULSEINT = 10
CONFIG_CELLSYNC = 11
CONFIG_CELLTOD_HOUR = 12
CONFIG_CELLTOD_MIN = 13
CONFIG_TONE_STA = 15
CONFIG_TONE_BOX = 17
CONFIG_TONE_MAN = 19
CONFIG_TONE_CEL = 21
CONFIG_TONE_BXX = 23
CONFIG_ACTIVE_LOOP = 14
CONFIG_SPARE = 25
CONFIG_FLAGS = {
    CONFIG_TOD: 'Time of Day',
    CONFIG_GPS: 'GPS Sync',
    CONFIG_TZ_HOUR: 'Timezone Hour',
    CONFIG_TZ_MIN: 'Timezone Min',
    CONFIG_485: 'Distant rs485',
    CONFIG_FIBRE: 'Distant Fibre',
    CONFIG_PRINT: 'Serial Print',
    CONFIG_MAX: 'Detect Max',
    CONFIG_PROT: 'Protocol',
    CONFIG_PULSE: 'Sync Pulse',
    CONFIG_PULSEINT: 'Sync Interval',
    CONFIG_CELLSYNC: 'CELL Sync',
    CONFIG_CELLTOD_HOUR: 'CELL Sync Hour',
    CONFIG_CELLTOD_MIN: 'CELL Sync Min',
    CONFIG_TONE_STA: 'STA Tone',
    CONFIG_TONE_BOX: 'BOX Tone',
    CONFIG_TONE_MAN: 'MAN Tone',
    CONFIG_TONE_CEL: 'CEL Tone',
    CONFIG_TONE_BXX: 'BXX Tone',
    CONFIG_ACTIVE_LOOP: 'Active Loop',
    CONFIG_SPARE: '[spare]'
}
OFFLINE_STAT = {
    "date": None,
    "time": None,
    "offset": None,
    "env": None,
    "count": None,
    "battery": None,
    "units": None,
    "info": "error"
}
SHUTDOWN_STAT = {
    "date": None,
    "time": None,
    "offset": None,
    "env": None,
    "count": None,
    "battery": None,
    "units": None,
    "info": "offline"
}


def thbc_sum(msgstr=b''):
    """Return sum of character values as decimal string."""
    ret = 0
    for ch in msgstr:
        ret = ret + ch
    return '{0:04d}'.format(ret).encode('ascii', 'ignore')


def val2hexval(val):
    """Convert int to decimal digit equivalent hex byte."""
    ret = 0x00
    ret |= ((val // 10) & 0x0f) << 4  # msd     97 -> 0x90
    ret |= (val % 10) & 0x0f  # lsd   97 -> 0x07
    return ret


def hexval2val(hexval):
    """Unconvert a decimal digit equivalent hex byte to int."""
    ret = 10 * (hexval >> 4)  # tens 0x97 -> 90
    ret += hexval & 0x0f  # ones 0x97 ->  7
    return ret


class prounit(object):
    """Networked protime decoder unit."""

    def __init__(self, ip, name, hub):
        self.ip = ip
        self.name = name
        self.unitno = None
        self.version = None
        self.passlevel = _PASSLEVEL
        self.config = {}
        self.ipconfig = {}
        self.__hub = hub
        self.__cksumerr = 0

    def __v3_cmd(self, cmdstr=b''):
        """Pack and send a v3 command directly."""
        crc = mcrf4xx(cmdstr)
        crcstr = bytes([(crc >> 8) & 0xff, crc & 0xff])
        self.__hub.sendto(ESCAPE + cmdstr + crcstr + b'>', self.ip)

    def __set_levels(self):
        """Set the Pass level on attached unit."""
        lvl = '{0:02d}'.format(self.passlevel).encode(THBC_ENCODING)
        self.__hub.sendto(STALVL + lvl, self.ip)
        self.__hub.sendto(BOXLVL + lvl, self.ip)

    def __serialise_config(self):
        """Convert current decoder setting into a config string"""
        obuf = bytearray(CONFIG_LEN)

        # fill in level bytes
        obuf[CONFIG_SPARE] = 0x20  # will be fixed by subsequent levelset
        obuf[CONFIG_SPARE + 1] = 0x20

        # fill in tone values
        for opt in [
                CONFIG_TONE_STA, CONFIG_TONE_BOX, CONFIG_TONE_MAN,
                CONFIG_TONE_CEL, CONFIG_TONE_BXX
        ]:
            if opt in self.config:
                obuf[opt] = val2hexval(self.config[opt] // 100)  # xx00
                obuf[opt + 1] = val2hexval(self.config[opt] % 100)  # 00xx

        # fill in single byte values
        for opt in [
                CONFIG_TZ_HOUR, CONFIG_TZ_MIN, CONFIG_PROT, CONFIG_PULSEINT,
                CONFIG_CELLTOD_HOUR, CONFIG_CELLTOD_MIN
        ]:
            if opt in self.config:
                obuf[opt] = val2hexval(self.config[opt] % 100)  # ??
        # fill in flags
        for opt in [
                CONFIG_TOD, CONFIG_GPS, CONFIG_485, CONFIG_FIBRE, CONFIG_PRINT,
                CONFIG_MAX, CONFIG_PULSE, CONFIG_CELLSYNC, CONFIG_ACTIVE_LOOP
        ]:
            if opt in self.config:
                if self.config[opt]:
                    obuf[opt] = 0x01
        return bytes(obuf)

    def set_config(self):
        """Write configuration to unit."""
        cmd = b'\x08\x08' + self.__serialise_config()
        self.__v3_cmd(cmd)
        self.__set_levels()

    def __parse_config(self, msg):
        """Decode and store configuration."""
        ibuf = bytearray(msg)
        self.config = {}
        for flag in sorted(CONFIG_FLAGS):  # import all
            # tone values
            if flag in [
                    CONFIG_TONE_STA, CONFIG_TONE_BOX, CONFIG_TONE_MAN,
                    CONFIG_TONE_CEL, CONFIG_TONE_BXX
            ]:
                self.config[flag] = 100 * hexval2val(ibuf[flag])
                self.config[flag] += hexval2val(ibuf[flag + 1])

            # single byte values
            elif flag in [
                    CONFIG_TZ_HOUR,
                    CONFIG_TZ_MIN,
                    CONFIG_PROT,
                    CONFIG_PULSEINT,
                    CONFIG_CELLTOD_HOUR,
                    CONFIG_CELLTOD_MIN,
            ]:
                self.config[flag] = hexval2val(ibuf[flag])
            # 'booleans'
            elif flag in [
                    CONFIG_TOD, CONFIG_GPS, CONFIG_485, CONFIG_FIBRE,
                    CONFIG_PRINT, CONFIG_MAX, CONFIG_PULSE, CONFIG_CELLSYNC,
                    CONFIG_ACTIVE_LOOP
            ]:
                self.config[flag] = bool(ibuf[flag])

        self.unitno = ''
        for c in msg[43:47]:
            self.unitno += chr(c + ord('0'))
        self.version = str(hexval2val(ibuf[47]))
        stalvl = hexval2val(msg[CONFIG_SPARE])
        boxlvl = hexval2val(msg[CONFIG_SPARE + 1])
        _hlog.info('%r: Unit ID: %s', self.ip, self.unitno)
        _hlog.debug('%r: Firmware Version: %r', self.ip, self.version)
        _hlog.debug('%r: Levels: STA=%r, BOX=%r', self.ip, stalvl, boxlvl)
        # Network config
        self.ipconfig['IP'] = socket.inet_ntoa(msg[27:31])
        self.ipconfig['Mask'] = socket.inet_ntoa(msg[31:35])
        self.ipconfig['Gateway'] = socket.inet_ntoa(msg[35:39])
        self.ipconfig['Host'] = socket.inet_ntoa(msg[39:43])
        _hlog.debug('%r: Host: %r', self.ip, self.ipconfig['Host'])

    def __parse_message(self, msg, ack=True):
        """Process a decoder message."""
        ret = False
        if len(msg) > 4:
            if msg[0:1] == PASSSTART:  # RFID message
                idx = msg.find(b'>')
                if idx == 37:  # Valid length
                    data = msg[1:33]
                    msum = msg[33:37]
                    tsum = thbc_sum(data)
                    if tsum == msum:  # Valid 'sum'
                        pvec = data.decode(THBC_ENCODING).split()
                        istr = ':'.join(pvec[3:6])
                        rawref = pvec[1]
                        if rawref.isdigit():
                            refint = int(rawref)
                            if refint == 255 or (refint > MINREFID and
                                                 refint < MAXREFID):  # trig
                                rstr = rawref.lstrip('0')
                                cstr = 'C1'
                                if pvec[0] == 'BOX':
                                    cstr = 'C2'
                                elif pvec[0] == 'MAN':
                                    cstr = 'C0'
                                t = tod.tod(pvec[2],
                                            index=istr,
                                            chan=cstr,
                                            refid=rstr,
                                            source=self.name)
                                self.__hub.passing(t)
                                if ack:
                                    self.__hub.ackpass(self.ip)
                                self.__cksumerr = 0
                                if pvec[5] == '2':
                                    _hlog.info('%r Low battery on %r', self.ip,
                                               rstr)
                                    t.chan = 'BATT'
                                    self.__hub.statusack(t)
                                elif pvec[5] == '3':
                                    _hlog.warning('%r Faulty battery on %r',
                                                  self.ip, rstr)
                                    t.chan = 'BATT'
                                    self.__hub.statusack(t)
                                ret = True
                            else:
                                _hlog.info('%r ignored spurious refid: %r',
                                           self.ip, rawref)
                                if ack:
                                    self.__hub.ackpass(self.ip)
                        else:
                            _hlog.info('%r ignored spurious refid: %r',
                                       self.ip, rawref)
                            if ack:
                                self.__hub.ackpass(self.ip)
                    else:
                        _hlog.warning('%r invalid checksum: %r != %r: %r',
                                      self.ip, tsum, msum, msg)
                        self.__cksumerr += 1
                        if self.__cksumerr > 3:
                            # assume error on decoder
                            _hlog.error('%r erroneous message from decoder',
                                        self.ip)
                            if ack:
                                self.__hub.ackpass(self.ip)
                else:
                    _hlog.debug('%r invalid message: %r', self.ip, msg)
            elif msg[0:1] == STATSTART:  # Status message
                data = msg[1:22]
                pvec = data.decode(THBC_ENCODING).split()
                if len(pvec) == 5:
                    rstr = ':'.join(pvec[1:])
                    t = tod.tod(pvec[0].rstrip('"'),
                                index='',
                                chan='STS',
                                refid=rstr,
                                source=self.name)
                    self.__hub.statusack(t)
                    ret = True
                else:
                    _hlog.info('%r invalid status: %r', self.ip, msg)
            elif b'+++' == msg[0:3] and len(msg) > 53:
                self.__parse_config(msg[3:])
                ret = True
            else:
                _hlog.debug('%r: %r', self.ip, msg)
        else:
            _hlog.debug('%r short msg: %r', self.ip, msg)
        return ret

    def parse(self, msg):
        """Parse all complete lines in msg, then return the residual."""
        ret = msg
        while LF in ret:
            # split on first occurrence of CR+LF
            (pct, sep, ret) = ret.partition(CR + LF)
            pct = pct + sep

            # check packet for a start char
            ind = None
            for p in b'<[+':
                nb = pct.find(p)
                if nb >= 0:
                    if ind is None or nb < ind:
                        ind = nb
            if ind is not None:
                self.__parse_message(pct[ind:])
            else:
                # LF with no start char
                _hlog.debug('%r: %r', self.ip, pct)
        return ret


class prohub(threading.Thread):
    """Protime network hub."""

    def __init__(self):
        threading.Thread.__init__(self, daemon=True)
        self.hub = {}  # map of unit associations
        self.port = None  # hub socket object
        self.portno = 2008
        self.ipaddr = ''
        self.broadcast = '255.255.255.255'
        self.passlevel = _PASSLEVEL
        self.rdbuf = {}  # per unit read buffers
        self.cqueue = queue.Queue()  # command queue
        self.running = False
        self.__tc = 0
        self.__cb = self.__defcb
        self.__statuscb = self.__defscb

    def add(self, ip, name):
        """Queue an add unit command."""
        self.cqueue.put_nowait(('ADD', ip, name))

    def __add(self, ip, name):
        """Add or replace a connection to the unit at the given IP."""
        self.__remove(ip)
        self.hub[ip] = prounit(ip, name, self)
        self.hub[ip].passlevel = self.passlevel
        _hlog.debug('Add unit: %r:%r', ip, name)
        self.__write(QUECMD, ip)

    def remove(self, ip):
        """Queue a remove unit command."""
        self.cqueue.put_nowait(('REMOVE', ip))

    def __remove(self, ip):
        """Remove the specified decider association."""
        # dump current association
        if ip in self.hub:
            del (self.hub[ip])
        if ip in self.rdbuf:
            del (self.rdbuf[ip])

    def connect(self, port=None, ipaddr=None):
        """Re-initialise the listening port."""
        try:
            if port is not None:
                self.portno = port
            if ipaddr is not None:
                self.ipaddr = ipaddr
            _hlog.debug('Listening on: %r', (self.ipaddr, self.portno))
            self.port = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.port.settimeout(0.2)
            self.port.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.port.bind((self.ipaddr, self.portno))
        except Exception as e:
            _hlog.error('%s hub connect: %s', e.__class__.__name__, e)
            self.port = None
            self.running = False

    def __shutdown(self):
        """Try to close the listening socket."""
        try:
            if self.port is not None:
                _hlog.debug('Closing listen socket')
                self.port.close()
                self.port = None
        except Exception as e:
            _hlog.error('%s hub disconnect: %s', e.__class__.__name__, e)

    def exit(self, msg=None):
        """Request thread termination."""
        self.cqueue.put_nowait(('SHUTDOWN', msg))

    def wait(self):
        """Suspend calling thread until the command queue is empty."""
        self.cqueue.join()

    def __read(self):
        """Read from the shared port."""
        (msg, addr) = self.port.recvfrom(2048)
        _hlog.debug('RECV %r: %r', addr[0], msg)

        # append message to appropriate read buffer
        mid = addr[0]
        if mid in self.hub:
            if mid not in self.rdbuf:
                self.rdbuf[mid] = msg
            else:
                self.rdbuf[mid] += msg

            if LF in self.rdbuf[mid]:
                self.rdbuf[mid] = self.hub[mid].parse(self.rdbuf[mid])
        else:
            pass

    def __write(self, msg, dst):
        """Write to the nominated unit."""
        _hlog.debug('SEND %r: %r', dst, msg)
        return self.port.sendto(msg, (dst, self.portno))

    def passing(self, p):
        """Queue a passing record."""
        self.cqueue.put_nowait(('PASSING', p))

    def pingall(self):
        """Broadcast a status request."""
        self.cqueue.put_nowait(('ALLSTAT', None, None))

    def statusack(self, p):
        """Queue a status record."""
        self.cqueue.put_nowait(('STATUSACK', p))

    def ackpass(self, ip):
        """Acknowledge a passing to the nominated unit."""
        self.cqueue.put_nowait(('WRITE', ACKCMD, ip))

    def status(self, ip):
        """Request status from the unit."""
        self.cqueue.put_nowait(('WRITE', STATCMD, ip))

    def stopsession(self, ip):
        """Request stop session to unit."""
        self.cqueue.put_nowait(('WRITE', STOPCMD, ip))

    def startsession(self, ip):
        """Request start session to unit."""
        self.cqueue.put_nowait(('WRITE', STARTCMD, ip))

    def sync(self, ip=None):
        self.cqueue.put_nowait(('SYNC', ip))

    def configset(self, ip, req):
        """Queue config set command."""
        self.cqueue.put_nowait(('CONFIG', ip, req))

    def configget(self, ip):
        """Queue config get command."""
        self.cqueue.put_nowait(('WRITE', QUECMD, ip))

    def setcb(self, cbfunc=None, statusfunc=None):
        """Set the callback function for passing messages."""
        if cbfunc is not None:
            self.__cb = cbfunc
        else:
            self.__cb = self.__defcb
        if statusfunc is not None:
            self.__statuscb = statusfunc
        else:
            self.__statuscb = self.__defscb

    def __defcb(self, t):
        """Default passing callback."""
        _hlog.debug('Passing: %r', t)

    def __defscb(self, m):
        """Default status callback."""
        _hlog.debug('Status: %r', m)

    def __configset(self, ip, req):
        """Request update of the keys in req on unit."""
        if ip in self.hub:
            # check for existence of any key, flags population
            if CONFIG_PULSE in self.hub[ip].config:
                # transfer configuration changes
                for flag in self.hub[ip].config:
                    key = CONFIG_FLAGS[flag]
                    if key in req:
                        if req[key] != self.hub[ip].config[flag]:
                            _hlog.debug('Config updated: %r: %r', ip, key)
                        self.hub[ip].config[flag] = req[key]
                # request update
                self.hub[ip].set_config()
            else:
                _hlog.info('Unit not yet connected: %r', ip)
        else:
            _hlog.info('Unknown unit: %r', ip)

    def sendto(self, cmd, ip):
        """Queue the specified command to the nominated ip."""
        self.cqueue.put_nowait(('WRITE', cmd, ip))

    def __set_time_cmd(self, t):
        """Return a set time command string for the provided time of day."""
        body = bytearray(4)
        s = int(t.timeval)
        body[0] = s // 3600  # hours
        body[1] = (s // 60) % 60  # minutes
        body[2] = s % 60  # seconds
        body[3] = 0x74
        return SETTIME + bytes(body)

    def __command(self, m):
        """Process a command out of the command queue."""
        if isinstance(m, tuple) and isinstance(m[0], str):
            self.__tc = 0
            if m[0] == 'WRITE':
                if len(m) == 3:
                    self.__write(m[1], m[2])
            elif m[0] == 'PASSING':
                if len(m) == 2:
                    self.__cb(m[1])
            elif m[0] == 'STATUSACK':
                if len(m) == 2:
                    self.__statuscb(m[1])
            elif m[0] == 'SYNC':
                # broadcast or direct a rough sync command
                dst = self.broadcast
                if m[1] is not None:
                    dst = m[1]
                t = tod.now()
                while t - t.truncate(0) > tod.tod('0.02'):
                    t = tod.now()
                _hlog.debug('Sync %s => %r', t.meridiem(), dst)
                self.__write(self.__set_time_cmd(t), dst)
            elif m[0] == 'ALLSTAT':
                self.__write(STATCMD, self.broadcast)
            elif m[0] == 'CONFIG':
                if len(m) == 3:
                    self.__configset(m[1], m[2])
            elif m[0] == 'ADD':
                if len(m) == 3:
                    self.__add(m[1], m[2])
            elif m[0] == 'REMOVE':
                if len(m) == 2:
                    self.__remove(m[1])
            elif m[0] == 'SHUTDOWN':
                self.__shutdown()
                self.running = False

    def run(self):
        """Called via threading.Thread.start()."""
        self.running = True
        _hlog.debug('Decoder hub starting')
        self.connect()
        while self.running:
            try:
                try:
                    self.__read()  # block until timeout
                except socket.timeout:
                    self.__tc += 1
                    if self.__tc > 100:
                        # send a flush/timeout passing
                        self.__tc = 0
                        self.passing(tod.now(source=None))
                while True:  # until command queue empty exception
                    m = self.cqueue.get_nowait()
                    self.cqueue.task_done()
                    self.__command(m)
            except queue.Empty:
                pass
            except socket.error as e:
                _hlog.error('%s: %s', e.__class__.__name__, e)
        _hlog.info('Exiting')


def dr2t(dist, rate):
    """Convert distance (m) and rate (km/h) to time."""
    d = float(dist)
    r = float(rate) / 3.6
    return tod.tod(d / r)


def val2strset(val):
    """Return val as a set of strings"""
    ret = set()
    if isinstance(val, list):
        for i in val:
            nv = str(i)
            if nv:
                ret.add(nv)
    else:
        nv = str(val)
        if nv:
            ret.add(nv)
    if len(ret) == 0:
        ret = None
    return ret


def val2mpidset(val):
    """Return val as a list of mpids"""
    ret = set()
    if isinstance(val, list):
        for i in val:
            mpid = strops.chan2id(i)
            if mpid >= 0:
                ret.add(mpid)
    else:
        mpid = strops.chan2id(val)
        if mpid >= 0:
            ret.add(mpid)
    if len(ret) == 0:
        ret = None
    return ret


def val2timerange(val):
    """Return val as a time range"""
    ret = None
    stime = None
    ftime = None
    if isinstance(val, list):
        if len(val) == 1:
            stime = tod.mktod(val[0]).rawtime(places=2,
                                              zeros=True,
                                              hoursep=':')
        elif len(val) == 2:
            stime = tod.mktod(val[0]).rawtime(places=2,
                                              zeros=True,
                                              hoursep=':')
            ftime = tod.mktod(val[1]).rawtime(places=2,
                                              zeros=True,
                                              hoursep=':')
            if stime is not None and ftime is not None and stime > ftime:
                t = stime
                stime = ftime
                ftime = t
        else:
            _log.debug('Invalid time range %r ignored', val)
    else:
        # single value, assume it is the start
        stime = tod.mktod(val).rawtime(places=2, zeros=True, hoursep=':')
    if stime is not None or ftime is not None:
        ret = (stime, ftime)
    return ret


def val2indexrange(val):
    """Return val as an index range"""
    ret = None
    sid = None
    fid = None
    if isinstance(val, list):
        if len(val) == 1:
            sid = strops.confopt_posint(val[0])
        elif len(val) == 2:
            sid = strops.confopt_posint(val[0])
            fid = strops.confopt_posint(val[1])
            if sid is not None and fid is not None and sid > fid:
                t = sid
                sid = fid
                fid = t
        else:
            _log.debug('Invalid index range %r ignored', val)
    else:
        # single value, assume request is for a single record
        sid = strops.confopt_posint(val)
        fid = sid
    if sid is not None or fid is not None:
        ret = (sid, fid)
    return ret


class app(object):
    """Velotrain application object."""

    def __init__(self):
        self._c = Comet()
        self._y = ypmeteo()
        self._t = telegraph()
        self._h = prohub()
        self._cbq = queue.Queue()
        self._cf = {}
        self._mps = {}
        self._mpActive = set()
        self._mpnames = {}
        self._dhi = None
        self._secmap = {}
        self._pstore = []  # store of passings in this session
        self._batteries = {}  # count of low battery warnings
        self._rlock = threading.Lock()  # reset lock
        self._resetting = False  # status flag set in reset procedure
        self._dstat = {}  # data store for decoder status c1->(level,tod)
        self._drifts = {}  # data store for decoder drift
        self._motos = {}  # store for most recent moto passing
        self._gatesrc = None  # gate triggers accepted from this mp
        self._gatedelay = None  # delay time for gate trigger
        self._tomsrc = None  # top-of-minute actions are triggered by this mp
        self._gate = None  # last gate trigger
        self._runstart = None  # start of current run
        self._lastpass = None  # last accepted passing
        self._passq = {}  # passing queue buffer
        self._secmap = {}  # sector map for configured decoders
        self._syncmaster = None  # mpid of sync master unit
        self._offset = 0  # rough system clock offset from sync master
        self._t.setcb(self._tcb)
        self._h.setcb(self._hpcb, self._hscb)
        self._acktopic = None
        self._statustopic = None
        self._passingtopic = None
        self._replaytopic = None
        self._rawpasstopic = None

    def _tcb(self, topic, message):
        """Handle a command callback from telegraph"""
        self._cbq.put(('COMMAND', topic, message))

    def _hpcb(self, t):
        """Handle a passing event callback from timer hub"""
        self._cbq.put(('RAWPASS', t))

    def _hscb(self, m):
        """Handle a status event callback from timer hub"""
        self._cbq.put(('STATUS', m))

    def _env(self):
        """Return an environment tuple (t, h, p) if available, or None."""
        ret = None
        if self._c.valid():
            ret = (self._c.t, self._c.h, self._c.p)
            if self._y.connected():
                _log.info('Weather: %r',
                          (time.time(), self._c.t, self._c.h, self._c.p,
                           self._y.t, self._y.h, self._y.p))
        elif self._y.connected():
            _log.debug('Comet unavailable - fallback to Meteo')
            ret = (self._y.t, self._y.h, self._y.p)
        return ret

    def _loadconfig(self):
        _log.debug('Reading system config')
        cf = jsonconfig.config({'velotrain': _CONFIG})

        # First overwrite with system defaults
        cf.merge(metarace.sysconf, 'velotrain')

        # Then consult specifics
        cfile = metarace.default_file(_CONFIGFILE)
        if os.path.exists(cfile):
            _log.debug('Reading config from: %r', cfile)
            try:
                with open(cfile, 'r', encoding='utf-8') as f:
                    cf.read(f)
            except Exception as e:
                _log.error('%s reading config: %s', e.__class__.__name__, e)

        # Copy matching section into config dict
        self._cf = cf.dictcopy()['velotrain']

        # Setup hub details
        self._h.broadcast = self._cf['bcast']
        self._h.ipaddr = self._cf['uaddr']
        self._h.portno = self._cf['uport']
        self._h.passlevel = self._cf['passlevel']

        # Determine which decoders are enabled
        self._mps = {}
        self._mpActive = set()
        for i in range(1, 10):
            d = strops.id2chan(i)
            if d in self._cf['mps']:
                split = self._cf['mps'][d]
                if isinstance(split, dict):
                    if 'ip' in split and split['ip'] is not None:
                        self._mps[d] = split['ip']
                        self._h.add(split['ip'], d)
                        self._mpnames[d] = d
                        if 'name' in split and isinstance(split['name'], str):
                            self._mpnames[d] = split['name']
                    if 'active' in split and split['active']:
                        self._mpActive.add(d)

        # Re-load track layout, splits and timing sectors - must be called
        # at least once before hub is started
        self._initsectors()

        # Enable sync master, if set
        if isinstance(self._cf['sync'], str) and self._cf['sync'] in self._mps:
            self._syncmaster = self._cf['sync']
            _log.info('Enabled %s:%s as sync master', self._syncmaster,
                      self._mps[self._syncmaster])
        else:
            _log.warning('Sync master not configured (%s)', self._cf['sync'])

        # Allocate the top-of-minute trigger source
        self._tomsrc = None
        for mp in self._mps:
            if mp != self._syncmaster:
                self._tomsrc = mp
                _log.info('Using %s for top-of-minute trigger', mp)
                break
        if self._tomsrc is None:
            raise RuntimeError(
                'No top-of-minute trigger configured, system inoperable')

        # Read in gate delay if set
        self._gatedelay = tod.ZERO
        gd = tod.mktod(self._cf['gatedelay'])
        if gd is not None:
            self._gatedelay = gd
            _log.debug('Gate delay set to: %s', self._gatedelay.rawtime())

        # Subscribe to control topics and set publish endpoints
        bt = strops.confopt_str(self._cf['basetopic'], '')
        if bt:
            self._t.subscribe(bt + '/marker')
            self._t.subscribe(bt + '/request')
            self._t.subscribe(bt + '/reset')
            self._t.subscribe(bt + '/resetunit')
            self._t.subscribe(bt + '/timer')
            self._acktopic = bt + '/ack'
            self._statustopic = bt + '/status'
            self._passingtopic = bt + '/passing'
            self._replaytopic = bt + '/replay'
            self._rawpasstopic = bt + '/rawpass'
        else:
            raise RuntimeError('Invalid basetopic ' +
                               repr(self._cf['basetopic']) +
                               ', system inoperable')
        # set a will for unexpected disconnect from broker
        self._t.set_will_json(obj=OFFLINE_STAT,
                              topic=self._statustopic,
                              retain=True)

        # Add dhi output, if configured
        self._dhi = None
        if self._cf['dhi'] is not None:
            if isinstance(self._cf['dhi'], list) and len(self._cf['dhi']) == 2:
                ip = self._cf['dhi'][0].strip()
                port = strops.confopt_posint(self._cf['dhi'][1], None)
                if port is not None:
                    self._dhi = (self._cf['dhi'][0], self._cf['dhi'][1])
                    _log.debug('Added DHI on TCP:%r', self._dhi)
            if self._dhi is None:
                _log.warning('Ignored invalid DHI setting: %r',
                             self._cf['dhi'])

    def _initsectors(self):
        """Initialise the sector map data structures."""
        _log.debug('Initialising track for mps: %r', self._mps)
        for d in self._mps:
            self._dstat[d] = None
            self._drifts[d] = tod.agg(0)
            self._motos[d] = None
        self._gate = None
        self._motos = {}
        self._runstart = None
        self._lastpass = None
        self._passq = {}

        _log.debug('Loading track layout')
        secsrc = {}
        laplen = strops.confopt_float(self._cf['laplen'], None)
        if laplen is None:
            _log.error('Invalid lap length %r ignored', self._cf['laplen'])
            laplen = _DEFLAPLEN
        schans = [d for d in self._mps]

        for sc in schans:
            sv = strops.chan2id(sc)
            for dc in schans:
                dv = strops.chan2id(dc)
                kv = (sv, dv)
                if sv < 0 or dv < 0:
                    secsrc[kv] = 0
                    _log.debug('Added null sector %r', kv)
                elif sv == dv:
                    secsrc[kv] = laplen  # full lap
                    _log.debug('Added full lap %r = %r m', kv, laplen)
                else:
                    smp = self._cf['mps'][sc]
                    dmp = self._cf['mps'][dc]
                    if 'offset' in smp and 'offset' in dmp:
                        soft = strops.confopt_float(smp['offset'])
                        doft = strops.confopt_float(dmp['offset'])
                        if soft is not None and doft is not None:
                            if soft < doft:
                                secsrc[kv] = doft - soft
                            else:
                                secsrc[kv] = laplen - soft + doft
                            _log.debug('Added sector %r = %r m', kv,
                                       secsrc[kv])
                        else:
                            _log.warning(
                                'Invalid offset for sector %r: %r - %r', kv,
                                smp['offset'], dmp['offset'])
                    else:
                        _log.warning('Missing data for sector %r', kv)
        _log.debug('Loaded track layout: %r', secsrc)
        _log.debug('Re-building sector map')
        self._secmap = {}
        last = None
        first = None
        prev = None
        # only configured mps listed in the sequence will be used as splits
        for d in self._cf['mpseq']:
            if d in self._mps:
                # d is a configured measurement point, add to map
                self._secmap[d] = {
                    'prev': None,
                    'next': None,
                    'slen': None,
                    'sid': None,
                    'maxtime': None,
                    'mintime': None,
                    'lap': None,
                    'half': None,
                    'qtr': None,
                    '200': None,
                    '100': None,
                    '50': None
                }
                # load split definitions
                sdef = self._cf['mps'][d]
                for split in ['lap', 'half', 'qtr', '200', '100', '50']:
                    if split == 'lap' or split in sdef:
                        if split == 'lap':
                            spid = d
                        else:
                            spid = sdef[split]
                        if isinstance(spid, str) and spid in self._mps:
                            startid = strops.chan2id(spid)
                            endid = strops.chan2id(d)
                            if (startid, endid) in secsrc:
                                splen = secsrc[(startid, endid)]
                                sm = {
                                    'src': spid,
                                    'min': dr2t(splen, self._cf['maxspeed']),
                                    'max': dr2t(splen, self._cf['minspeed']),
                                    'len': splen
                                }
                                self._secmap[d][split] = sm
                                _log.debug('Added split %r = %r m',
                                           (startid, endid), splen)
                            else:
                                _log.warning('Missing %r in track layout',
                                             (startid, endid))
                        else:
                            if spid is not None:
                                _log.warning(
                                    'Invalid source %r for split %r at mp %r',
                                    spid, split, d)
                    else:
                        _log.debug('No %r split for mp %r', split, d)
                if first is None:
                    first = d
                last = d
                if prev is not None:
                    # fill in all sector data
                    self._secmap[prev]['next'] = d  # prev->next = this
                    self._secmap[d]['prev'] = prev  # this->prev = prev
                    startid = strops.chan2id(prev)
                    endid = strops.chan2id(d)
                    if (startid, endid) in secsrc:
                        seclen = secsrc[(startid, endid)]
                        self._secmap[d]['slen'] = seclen
                        self._secmap[d]['sid'] = endid
                        self._secmap[d]['mintime'] = dr2t(
                            seclen, self._cf['maxspeed'])
                        self._secmap[d]['maxtime'] = dr2t(
                            seclen, self._cf['minspeed'])
                        _log.debug('Added split %r = %r m', (startid, endid),
                                   seclen)
                    else:
                        _log.warning('Missing sector %r in track layout',
                                     (startid, endid))
                prev = d
            else:
                # d is not a configured measurement point, ingore
                _log.debug('Skipping unconfigured mp %r', d)
        if first is not None and last is not None:
            # fill in the final sector that closes the loop
            self._secmap[last]['next'] = first
            self._secmap[first]['prev'] = last
            startid = strops.chan2id(last)
            endid = strops.chan2id(first)
            seclen = secsrc[(startid, endid)]
            self._secmap[first]['slen'] = seclen
            self._secmap[first]['sid'] = endid
            self._secmap[first]['mintime'] = dr2t(seclen, self._cf['maxspeed'])
            self._secmap[first]['maxtime'] = dr2t(seclen, self._cf['minspeed'])
            _log.debug('Added split %r = %r m', (startid, endid), seclen)
        # also add the start 'gate' as an entrance sector
        startmp = strops.confopt_str(self._cf['gatesrc'])
        if startmp in self._mps:
            endmp = self._secmap[startmp]['next']
            self._gatesrc = startmp
            startid = strops.chan2id(startmp)
            endid = strops.chan2id(endmp)
            if startid > 0 and endid > 0:
                seclen = secsrc[(startid, endid)]
                self._secmap['gate'] = {
                    'prev': None,
                    'next': endmp,
                    'slen': seclen,
                    'sid': endid,
                    'maxtime': dr2t(seclen, self._cf['mingate']),
                    'mintime': dr2t(seclen, self._cf['maxgate'])
                }
                _log.debug('Added start gate split %r - %r = %r m', startmp,
                           endmp, seclen)
            else:
                _log.warning('Ignored invalid gate split %r - %r', startmp,
                             endmp)
        else:
            _log.debug('Gate split mp %r not configured', startmp)
        _log.info('Configured %s splits: %r', len(self._secmap),
                  ', '.join(sorted(self._secmap)))

    def _reqstatus(self):
        """Publish a status message."""
        nt = tod.now()
        gtime = None
        if self._gate is not None:
            gtime = self._gate.rawtime(places=2, zeros=True, hoursep=':')
        stval = 'running'
        if self._resetting:
            stval = 'resetting'
        st = {
            'date': time.strftime('%F'),
            'time': nt.rawtime(places=2, zeros=True, hoursep=':'),
            'offset': str(self._offset),
            'env': self._env(),
            'count': len(self._pstore),
            'gate': gtime,
            'battery': [],
            'units': [],
            'info': stval,
        }
        slog = [
            'Status Count:{} Offset:{}'.format(len(self._pstore),
                                               str(self._offset))
        ]
        for r in sorted(self._batteries):
            if self._batteries[r] > _LOWBATTWARN:
                st['battery'].append(r)
        for d in self._mps:
            mpid = strops.chan2id(d)
            cname = self._mpnames[d]

            dv = None
            if d in self._drifts:
                dv = self._drifts[d].rawtime(3)
            st['units'].append({
                'mpid': mpid,
                'name': cname,
                'noise': self._dstat[d],
                'offset': dv,
            })
            slog.append('{}:{}'.format(d, self._dstat[d]))
        _log.info(' '.join(slog))
        self._t.publish_json(obj=st, topic=self._statustopic, retain=True)

    def _rawstatus(self, msg):
        """Handle raw status message from hub in main thread."""
        if msg.chan == 'STS':
            if msg.source in self._mps:
                statv = msg.refid.split(':')
                if len(statv) > 0:
                    _log.debug('Mp %r: noise=%r@%s', msg.source, statv[0],
                               msg.rawtime(0))
                    nv = strops.confopt_posint(statv[0], None)
                    self._dstat[msg.source] = nv
            else:
                _log.debug('Status %r from unconfigured mp %r', msg.refid,
                           msg.source)
        elif msg.chan == 'BATT':
            refid = msg.refid
            if refid not in (self._cf['gate'], self._cf['trig']):
                if refid not in self._batteries:
                    self._batteries[refid] = 0
                self._batteries[refid] += 1
                _log.debug('Low battery warning on %s, count=%r', refid,
                           self._batteries[refid])

    def _timeout(self):
        """Perform cleanup and queue unchoking as required."""
        if not self._resetting:
            self._cleanqueues()
            self._h.pingall()
        return None

    def _rawpassing(self, t):
        """Handle a raw passing message from hub in main thread."""
        cid = t.source
        if cid is None:
            return self._timeout()
        if self._resetting:
            if cid == self._tomsrc and t.refid == self._cf['trig']:
                self._resetting = False
                _log.info('Reset complete, resuming normal operation')
            else:
                _log.debug('Ignored passing during reset: %r@%s', cid,
                           t.rawtime(2))
            return None

        # store offset on sync master and discard unconfigured mp
        nt = tod.now()
        if cid == self._syncmaster:
            self._offset = nt.timeval - t.timeval
        elif cid not in self._mps:
            _log.info('Spurious passing: %r@%s', cid, t.rawtime(2))
            return None

        # patch invalid refid
        if not t.refid:
            _log.info('Altered refid %r to 1', t.refid)
            t.refid = '1'

        # publish the raw passing
        mpid = strops.chan2id(cid)
        cname = self._mpnames[cid]
        rp = {
            'date': time.strftime('%F'),
            'env': self._env(),
            'refid': t.refid,
            'mpid': mpid,
            'name': cname,
            'info': t.index,
            'time': t.rawtime(places=3, zeros=True, hoursep=':'),
            'rcv': nt.rawtime(places=3, zeros=True, hoursep=':'),
        }
        self._t.publish_json(obj=rp, topic=self._rawpasstopic)

        # allocate mp offset to non-trig passings
        if t.refid != self._cf['trig']:
            t.timeval += self._drifts[cid].timeval

        # then process
        if t.refid in (self._cf['gate'], self._cf['trig']):
            self._systempass(t, cid)
        else:
            # process moto as system pass then overwrite refid
            if t.refid in self._cf['moto']:
                self._systempass(t, cid)
                t.refid = 'moto'
            ps = self._prepareq(t.refid)
            ps['q'].insert(pri=t, sec=None, bib=cid)
            # todo: check if moto is expected before processing
            self._process_pq(t.refid, ps)
        return None

    def _emit_env(self):
        """Send non-critical environment data to DHI scoreboard."""
        if self._dhi is not None:
            try:
                if self._c.valid():
                    enc = self._cf['dhiencoding']
                    tv = '{0:0.1f}'.format(self._c.t)
                    hv = '{0:0.0f}'.format(self._c.h)
                    pv = '{0:0.0f}'.format(self._c.p)
                    msg = ''.join((unt4.unt4(header='DC', text=tv).pack(),
                                   unt4.unt4(header='RH', text=hv).pack(),
                                   unt4.unt4(header='BP', text=pv).pack()))
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(0.5)
                    s.connect(self._dhi)
                    s.sendall(msg.encode(enc, 'ignore'))
                    s.shutdown(socket.SHUT_RDWR)
                    s.close()
                else:
                    _log.debug('Comet data not available')
            except Exception as e:
                _log.debug('%s sending to DHI %r: %s', e.__class__.__name__,
                           self._dhi, e)

    def _cleanqueues(self):
        """Process all passing queues."""
        for refid in self._passq:
            self._process_pq(refid, self._passq[refid])
        return False

    def _sector_match(self, cid, nt, hist):
        """Determine if a new time is for a matching sector."""
        startid = self._secmap[cid]['prev']
        comlen = None

        # special case check for gate override
        if startid == self._gatesrc and self._gate is not None:
            oktogo = False
            gs = self._secmap['gate']
            if hist['lc'] is not None and hist['lc'] == startid:
                # passing over gate loop, compare passings
                if self._gate > hist['lt']:
                    secelap = nt - self._gate
                    if secelap > gs['mintime'] and secelap < gs['maxtime']:
                        oktogo = True
                ## TODO: Test the false gate v hist comparison here, this can
                ##       happen when you have a rider on track and gate start,
                ##       requires two transponders.
            else:
                # no history OR out of order, let gate override
                secelap = nt - self._gate
                if secelap > gs['mintime'] and secelap < gs['maxtime']:
                    oktogo = True

            if oktogo:  # overwrite is safe, modify history for the trig
                hist['lc'] = startid
                hist['lt'] = self._gate
                hist[startid] = self._gate
                return True  # and short-circuit

        # normal case: passing at speed over the sector
        if hist['lc'] is not None and hist['lc'] == startid:
            sd = self._secmap[cid]
            secelap = nt - hist['lt']
            if secelap > sd['mintime'] and secelap < sd['maxtime']:
                return True

        # all others are degenerate or isolated.
        return False

    def _replay(self, serial=None, filters=None):
        """Replay selected passings to replay topic."""
        rep = []

        plen = len(self._pstore)
        i = 0
        while i < plen:
            sid = i
            fid = plen

            # apply marker filtering
            if filters['marker'] is not None:
                # find start of next matching run
                while i < plen:
                    r = self._pstore[i]
                    i += 1
                    if r['refid'] == 'marker' and r['text'] in filters[
                            'marker']:
                        break
                    else:
                        sid = i
                # find end of matching run
                while i < plen:
                    r = self._pstore[i]
                    if r['refid'] == 'marker':
                        fid = i
                        break
                    else:
                        i += 1
                        fid = i
            if sid < fid:
                # apply index filter if applied
                rs = sid
                rf = fid
                if filters['index'] is not None:
                    if filters['index'][0] is not None:
                        if filters['index'][0] > rs:
                            rs = min(filters['index'][0], plen)
                    if filters['index'][1] is not None:
                        if filters['index'][1] < rf:  # endpoint is inclusive
                            rf = min(filters['index'][1] + 1, plen)
                if rs < rf:
                    _log.debug('Replay range: %r - %r, %r / %r', rs, rf,
                               rf - rs, plen)
                    j = rs
                    while j < rf:
                        r = self._pstore[j]
                        ok = True
                        if filters['time'] is not None:
                            if filters['time'][0] is not None and r[
                                    'time'] < filters['time'][0]:
                                ok = False
                            elif filters['time'][1] is not None and r[
                                    'time'] > filters['time'][1]:
                                ok = False
                        if filters['mpid'] is not None and r[
                                'mpid'] not in filters['mpid']:
                            ok = False
                        elif filters['refid'] is not None and r[
                                'refid'] not in filters['refid']:
                            ok = False
                        if ok:
                            rep.append(self._pstore[j])
                        j += 1
            i = fid

        pt = self._replaytopic
        if serial is not None:
            pt += '/' + serial
        _log.info('Replaying %r passings to %r', len(rep), pt)
        self._t.publish_json(obj=rep, topic=pt)

    def _clearhub(self):
        """Clear passing history."""
        ret = False
        if not self._rlock.acquire(False):
            _log.info('Clear/Reset already in progress')
            return False
        try:
            _log.info('Clear passing history')
            self._resetting = True
            # clear passing index & reset data structures
            self._pstore = []
            self._batteries = {}
            self._initsectors()
            self._resetting = False
            ret = True
        except Exception as e:
            _log.error('%s in Clear: %s', e.__class__.__name__, e)
        finally:
            self._rlock.release()
        return ret

    def _resetunit(self, unit):
        """Stop, start and sync a single unit."""
        if unit in self._mps and unit != self._syncmaster:
            d = self._mps[unit]
            _log.debug('Stop and reset %r:%r', unit, d)
            self._h.configget(d)
            self._h.wait()
            self._h.configset(d, _DECODERSANE)
            self._h.stopsession(d)
            self._h.startsession(d)
            self._h.wait()
            time.sleep(0.1)
            self._h.sync(d)
            _log.debug('Unit restarted: %r:%r', unit, d)
        else:
            _log.info('Unable to reset %r', unit)

    def _resethub(self):
        """Clear passing history and reset attached decoders."""
        ret = False
        if not self._rlock.acquire(False):
            _log.info('Clear/Reset already in progress')
            return False
        try:
            _log.info('Starting reset procedure, operation paused')
            self._resetting = True
            # stop & reset all attached decoders to home state
            for mp in self._mps:
                d = self._mps[mp]
                _log.debug('Stop and reset %r:%r', mp, d)
                self._h.stopsession(d)
                self._h.wait()
                time.sleep(0.1)
                self._h.configget(d)
                self._h.wait()
                time.sleep(0.1)
                self._h.configset(d, _DECODERSANE)  # re-write config
                self._h.wait()
                time.sleep(0.1)
            # clear passing index & reset data structures
            self._pstore = []
            self._batteries = {}
            self._initsectors()

            # blocking wait for a clear block to top of minute
            resid = int(tod.now().as_seconds()) % 60
            while resid > 40:
                _log.debug('Reset waiting [%r]', resid)
                time.sleep(float(62 - resid))
                resid = int(tod.now().as_seconds()) % 60
            # set the trig time
            nt = tod.tod(60 + 60 * (int(tod.now().as_seconds()) // 60))
            (hr, mn, sc) = nt.rawtime(0, zeros=True, hoursep=':',
                                      minsep=':').split(':')
            _log.info('Reset sync time: %r hrs %r min %r sec', hr, mn, sc)

            # set sync time on all slaved decoders
            confchg = {
                "Sync Pulse": False,
                "Active Loop": False,
                "CELL Sync Hour": int(hr),
                "CELL Sync Min": int(mn),
                "CELL Sync": True
            }
            for mp in self._mps:
                if mp != self._syncmaster:
                    if mp in self._mpActive:
                        confchg['Active Loop'] = True
                    else:
                        confchg['Active Loop'] = False
                    d = self._mps[mp]
                    _log.debug('Active Loop option on %r: %r', mp,
                               confchg['Active Loop'])
                    _log.debug('Udpate sync time on %r:%r', mp, d)
                    self._h.configset(d, confchg)
            if self._syncmaster is not None:
                d = self._mps[self._syncmaster]
                # prepare sync master
                _log.debug('Starting sync master %r:%r', self._syncmaster, d)
                self._h.startsession(d)
                self._h.wait()
                self._h.sync()
                self._h.wait()
                self._h.configset(d, {
                    "Sync Pulse": True,
                    "Active Loop": False
                })
                self._h.wait()
                ret = True
            else:
                _log.warning('No sync master set, using rough sync')
                self._h.startsession(self._h.broadcast)
                self._h.wait()
                time.sleep(0.1)
                self._h.sync()
                self._h.wait()
                ret = False
        except Exception as e:
            _log.error('%s in Reset: %s', e.__class__.__name__, e)
        finally:
            self._rlock.release()
        return ret

    def _process_pq(self, refid, p):
        """Process the contents of the passing queue in p."""

        # extract the sorted list of queued passings into proclist
        proclist = [j[0] for j in p['q']]  # todlist is (pri,sec)

        # extract passings until queue is empty or choked
        for j in proclist:
            cid = j.refid  # end of sector
            mpid = strops.chan2id(cid)
            cname = self._mpnames[cid]
            if self._sector_match(cid, j, p):
                _log.debug('Sector match %r: %s@%s', refid, cid, j.rawtime(2))
                # fetch sector data
                sm = self._secmap[cid]

                # check runtime elapsed
                elap = None
                if self._runstart is not None and j >= self._runstart:
                    if self._lastpass is not None and j >= self._lastpass:
                        if j - self._lastpass < _RUNIDLE:
                            elap = (j - self._runstart).round(2).rawtime(2)

                # check moto proximity
                moto = None
                if cid in self._motos and self._motos[cid] is not None:
                    mt = self._motos[cid]
                    dt = tod.agg(j) - mt
                    _log.debug('Moto comp: j=%s, mt=%s, dt=%s/%s',
                               j.rawtime(4), mt.rawtime(4), dt.rawtime(4),
                               dt.__class__.__name__)
                    if dt > -0.1 and dt < _MOTOPROX:
                        moto = dt.round(2).rawtime(2)
                po = {
                    'index': None,
                    'date': time.strftime('%F'),
                    'time': j.rawtime(places=3, zeros=True, hoursep=':'),
                    'mpid': mpid,
                    'refid': refid,
                    'env': self._env(),
                    'moto': moto,
                    'elap': elap,
                    'lap': None,
                    'half': None,
                    'qtr': None,
                    '200': None,
                    '100': None,
                    '50': None,
                    'text': cname,
                }
                for split in ['lap', 'half', 'qtr', '200', '100', '50']:
                    # overwrite split if possible
                    if split in sm and sm[split] is not None:
                        spsrc = sm[split]
                        sc = spsrc['src']
                        if sc in p and p[sc] is not None:
                            selp = (j - p[sc]).round(2)
                            if selp > spsrc['min'] and selp < spsrc['max']:
                                po[split] = str(selp.as_seconds(2))

                # remove first elem from passing queue
                p['q'].remove(cid, once=True)

                # save to history - this is now a processed sector
                p['lt'] = j
                p['lc'] = cid
                p[cid] = j
                p['choke'] = None

                # update lastpass if required
                if self._lastpass is None or j > self._lastpass:
                    self._lastpass = j

                # issue to telegraph and continue
                self._passing(po)
            else:
                # the head of the queue doesn't match required data
                if self._isolated_match(cid, j, p):
                    _log.debug('Isolated match %r: %s@%s', refid, cid,
                               j.rawtime(2))
                    # fetch sector data
                    sm = self._secmap[cid]

                    # reset own runstart
                    p['rs'] = None

                    # expire shared runstart and reset as required
                    if self._runstart is not None:
                        if self._lastpass is None or (
                                j > self._lastpass
                                and j - self._lastpass >= _RUNIDLE):
                            self._runstart = None
                    if self._runstart is None:
                        self._runstart = j

                    # check runtime elapsed
                    elap = None
                    if self._runstart is not None and j >= self._runstart:
                        if self._lastpass is not None and j >= self._lastpass:
                            if j - self._lastpass < _RUNIDLE:
                                elap = (j - self._runstart).round(2).rawtime(2)

                    # check moto proximity
                    moto = None
                    if cid in self._motos and self._motos[cid] is not None:
                        mt = self._motos[cid]
                        dt = tod.agg(j) - mt
                        _log.debug('Moto comp: j=%s, mt=%s, dt=%s/%s',
                                   j.rawtime(4), mt.rawtime(4), dt.rawtime(4),
                                   dt.__class__.__name__)
                        if dt > -0.1 and dt < _MOTOPROX:
                            moto = dt.rawtime(2)
                    po = {
                        'index': None,
                        'date': time.strftime('%F'),
                        'time': j.rawtime(places=3, zeros=True, hoursep=':'),
                        'mpid': mpid,
                        'refid': refid,
                        'env': self._env(),
                        'moto': moto,
                        'elap': elap,
                        'lap': None,
                        'half': None,
                        'qtr': None,
                        '200': None,
                        '100': None,
                        '50': None,
                        'text': cname,
                    }
                    for split in ['lap', 'half', 'qtr', '200', '100', '50']:
                        # overwrite split if possible
                        if split in sm and sm[split] is not None:
                            spsrc = sm[split]
                            sc = spsrc['src']
                            if sc in p and p[sc] is not None:
                                selp = (j - p[sc]).round(2)
                                if selp > spsrc['min'] and selp < spsrc['max']:
                                    po[split] = str(selp.as_seconds(2))
                    # remove first from queue
                    p['q'].remove(cid, once=True)

                    # save to history - this is now a processed passing
                    p['lt'] = j
                    p['lc'] = cid
                    p['rs'] = j
                    p[cid] = j

                    # update last passing if required
                    if self._lastpass is None or j > self._lastpass:
                        self._lastpass = j

                    # don't unchoke yet - there may be multiple stale passes
                    # issue to all clients and continue
                    self._passing(po)
                else:
                    _log.debug('Queue choked %r: %s@%s', refid, cid,
                               j.rawtime(2))
                    p['choke'] = cid
                    break

    def _passing(self, passob):
        """Emit a processed passing object."""
        idx = len(self._pstore)
        passob['index'] = idx
        self._pstore.append(passob)
        _log.info('Passing %r: %s %s@%s %s %s', idx,
                  strops.id2chan(passob['mpid']), passob['refid'],
                  passob['time'], passob['moto'], passob['text'])
        self._t.publish_json(obj=passob, topic=self._passingtopic)

    def _isolated_match(self, cid, nt, hist):
        """Determine if this passing is isolated."""
        # special case: no history, or way too old
        if hist['lc'] is None or (nt - hist['lt']) > _ISOTHRESH:
            return True

        # otherwise use course age measure (overflow isolates)
        if hist['choke'] is not None:
            age = tod.now() - nt
            if age > _ISOMAXAGE:
                return True

        # choke at least once before isolating
        return False

    def _marker(self, mark=None):
        """Insert a manual marker message into the passing list."""
        self._cleanqueues()
        nt = tod.now()

        # check runtime elapsed
        elap = None
        if self._runstart is not None and nt >= self._runstart:
            if self._lastpass is not None and nt >= self._lastpass:
                if nt - self._lastpass < _RUNIDLE:
                    elap = (nt - self._runstart).rawtime(2)
        po = {
            'index': None,
            'date': time.strftime('%F'),
            'time': nt.rawtime(places=3, zeros=True, hoursep=':'),
            'mpid': 0,
            'refid': 'marker',
            'env': self._env(),
            'moto': None,
            'elap': elap,
            'lap': None,
            'half': None,
            'qtr': None,
            '200': None,
            '100': None,
            '50': None,
            'text': mark
        }
        # markers do not extend runtime
        self._passing(po)

    def _systempass(self, t, chan):
        """Process a system passing message."""
        if t.refid == self._cf['trig']:
            # store, check and log channel drift
            tom = tod.agg(60 * int(round(float(t.as_seconds(2)) / 60.0)))
            self._drifts[chan] = tom - t
            if abs(self._drifts[chan]) > _LOGDRIFT:
                _log.info('Offset: %s@%s > %s', chan,
                          self._drifts[chan].rawtime(3), _LOGDRIFT)
            # trigger top-of-minute tasks
            if chan == self._tomsrc:
                # dump any stale passings before emitting status
                self._timeout()
                self._reqstatus()
                self._emit_env()
        elif t.refid in self._cf['moto']:
            _log.debug('Moto: %s@%s', chan, t.rawtime(2))
            self._motos[chan] = t.truncate(3)
        elif t.refid == self._cf['gate']:
            if chan == self._gatesrc:
                self._cleanqueues()
                _log.debug('Gate trigger: %s@%s', chan, t.rawtime(2))

                # apply gate transponder delay to tod reading
                self._gate = t - self._gatedelay

                # gate trigger overrides run start and last passing
                self._runstart = self._gate
                if self._lastpass is None or self._runstart > self._lastpass:
                    self._lastpass = self._runstart

                po = {
                    'index': None,
                    'date': time.strftime('%F'),
                    'time': self._gate.rawtime(places=3,
                                               zeros=True,
                                               hoursep=':'),
                    'mpid': 0,
                    'refid': 'gate',
                    'env': self._env(),
                    'moto': None,
                    'elap': '0.00',
                    'lap': None,
                    'half': None,
                    'qtr': None,
                    '200': None,
                    '100': None,
                    '50': None,
                    'text': 'Start Gate',
                }
                self._passing(po)
            else:
                _log.warning('Spurious gate trigger: %s@%s', chan,
                             t.rawtime(2))

    def _prepareq(self, refid):
        """Return a passing queue for refid, initialised if required."""
        if refid not in self._passq:
            nq = {}
            mps = ['lt', 'lc', 'choke', 'rs']
            mps.extend(self._mps)
            for d in mps:
                nq[d] = None
            nq['q'] = tod.todlist('PQ')
            self._passq[refid] = nq
        return self._passq[refid]

    def _checkrequest(self, msg):
        """Check a request body for filters then request a replay."""
        # note this will always try to emit something to replay, even if
        # input parameters are completely wrong
        serial = None
        filter = {
            'index': None,
            'time': None,
            'mpid': None,
            'refid': None,
            'marker': None
        }
        try:
            req = json.loads(msg)
            if isinstance(req, dict):
                # extract request serial
                if 'serial' in req:
                    serial = str(req['serial'])
                    _log.debug('Requested replay to serial: %r', serial)
                # check filters
                for filt in ('refid', 'marker'):
                    if filt in req:
                        filter[filt] = val2strset(req[filt])
                if 'mpid' in req:
                    filter['mpid'] = val2mpidset(req['mpid'])
                if 'time' in req:
                    filter['time'] = val2timerange(req['time'])
                if 'index' in req:
                    filter['index'] = val2indexrange(req['index'])
                # time: [starttime, endtime]
                _log.debug('Request filter: %r', filter)
            else:
                _log.warning('Invalid request object: %r', req)
        except Exception as e:
            _log.warning('%s reading request: %s', e.__class__.__name__, e)
        self._replay(serial, filter)

    def _foreigntimer(self, msg):
        """Read in a telegraphed timer message."""
        # 'INDEX;SOURCE;CHANNEL;REFID;TIMEOFDAY'
        t = None
        tv = msg.split(';')
        if len(tv) == 5:
            t = tod.mktod(tv[4])
        if t is not None:
            t.index = tv[0]
            t.source = tv[1]
            t.chan = tv[2]
            t.refid = tv[3]
            self._rawpassing(t)
        else:
            _log.warning('Ignored invalid foreign timer: %r', msg)

    def _command(self, topic, msg):
        """Process a command from telegraph."""
        _log.debug('Command %r', topic)
        req = topic.split('/')[-1].lower()
        if req == 'request':
            self._checkrequest(msg)
        elif req == 'marker':
            mark = 'Manual Marker'
            if msg:
                mf = msg.translate(strops.PRINT_UTRANS).strip()
                if mf:
                    mark = mf
            self._marker(mark)
        elif req == 'reset':
            auth = True
            if self._cf['authkey'] is not None:
                if msg != self._cf['authkey']:
                    auth = False
            if auth:
                self._resethub()
            else:
                _log.warning('Invalid reset authorisation key')
        elif req == 'resetunit':
            self._resetunit(msg)
        elif req == 'timer':
            self._foreigntimer(msg)
        else:
            _log.debug('Ignored invalid command')

    def _sigterm(self, signum, frame):
        self._cbq.put(('SHUTDOWN', signum, None))

    def run(self):
        _log.info('Starting')
        self._loadconfig()

        # start threads
        self._y.start()
        self._c.start()
        self._t.start()
        self._h.start()

        # catch TERM signal
        signal.signal(signal.SIGTERM, self._sigterm)

        # loop on the cbq
        self._running = True
        try:
            while self._running:
                m = self._cbq.get()
                self._cbq.task_done()
                if m[0] == 'RAWPASS':
                    self._rawpassing(m[1])
                elif m[0] == 'STATUS':
                    self._rawstatus(m[1])
                elif m[0] == 'COMMAND':
                    self._command(m[1], m[2])
                elif m[0] == 'SHUTDOWN':
                    self._running = False
                else:
                    pass
        finally:
            SHUTDOWN_STAT['time'] = tod.now().rawtime(2)
            SHUTDOWN_STAT['date'] = time.strftime('%F')
            self._t.publish_json(obj=SHUTDOWN_STAT,
                                 topic=self._statustopic,
                                 retain=True)
            self._y.exit()
            self._c.exit()
            self._h.exit()
            self._t.wait()
            self._t.exit()
            self._t.join()
        self._h.join()
        return 0


def main():
    # attach a console log handler to the root logger
    ch = logging.StreamHandler()
    ch.setLevel(_LOGLEVEL)
    fh = logging.Formatter(metarace.LOGFORMAT)
    ch.setFormatter(fh)
    logging.getLogger().addHandler(ch)

    # initialise the base library
    metarace.init()

    # Create and start timing app
    a = app()
    return a.run()


if __name__ == '__main__':
    sys.exit(main())
