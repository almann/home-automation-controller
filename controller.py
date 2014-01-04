#!/usr/bin/env python

import sys
import time
import datetime
import json
import traceback
import zlib
import glob
import re
import collections

import multiprocessing      as mp
import Queue                as queue

import RPi.GPIO             as io

import boto.dynamodb2       as ddb
import boto.dynamodb.types  as ddb_types

import boto.dynamodb2.table
import boto.dynamodb2.fields 

import boto.sqs             as sqs

import logging
from   logging              import warning as warn
from   logging              import debug
from   logging              import info
from   logging              import error

_AWS_REGION                 = "us-west-2"

_AC_RELAY_PIN               = 17
_AC_RELAY_MIN_TOGGLE_TIME   = 5.0
_PIR_PIN                    = 18
_RED_LED_PIN                = 23
_GREEN_LED_PIN              = 24

_SLEEP_TIME                 = 0.05
_LOG_TIME                   = 5.00
_LOG_FREQ                   = int(_LOG_TIME / _SLEEP_TIME)

_TEMPERATURE_SLEEP_TIME     = 0.50
_TEMPERATURE_TOLERANCE      = 0.25
_TEMPERATURE_WINDOW_SIZE    = int(_LOG_TIME / _TEMPERATURE_SLEEP_TIME) * 2

_IDLE_AC_TURNOFF_TIME       = 180.0

_MAX_EVENTS                 = 300

def now_str() :
    return str(datetime.datetime.now())

def now_secs() :
    return time.time()

class State(object) :
    '''Controller state'''
    def __init__(self, state_table_name, unit_id) :
        self.__table = ddb.table.Table(
            state_table_name,
            schema = [ddb.fields.HashKey('id')],
            connection = ddb.connect_to_region(_AWS_REGION)
        )
        self.__id = unit_id
        # bootstrap get--assume single thread/no concurrent writers
        self.__item = self.__table.get_item(consistent = True, id = self.__id)
        self.__item['id'] = self.__id
        
        state_data = self.__item.get('state', None)
        if state_data is None :
            self.__state = {}
        else :
            self.__state = json.loads(zlib.decompress(state_data.value))
    def __update_state(self) :
        self.__item['state'] = ddb_types.Binary(zlib.compress(json.dumps(self.__state)))
        self.__item['mtime'] = now_str()
        self.__item.save(overwrite = True)
    def __update_value_field(self, field, value) :
        if self.__state.get(field, None) != value :
            self.__state[field] = value
            self.__state['%s_time' % field] = now_secs()
            self.__update_state()
    def __update_state_field(self, field, flag) :
        state_label = 'ACTIVE' if flag else 'IDLE'
        self.__update_value_field(field, state_label)
    def update_motion(self, has_motion) :
        self.__update_state_field('pir_state', has_motion)
    def update_lamp(self, is_on) :
        self.__update_state_field('lamp_state', is_on)
    def update_temperature(self, temperature) :
        self.__update_value_field('temperature', temperature)
    def add_temperature_event(self, temperature) :
        events = self.__state.get('temperature_events', None)
        if events is None :
            events = []
            self.__state['temperature_events'] = events
        events.append(dict(temperature = temperature, time = now_secs()))
        if len(events) > _MAX_EVENTS :
            del events[0]
        self.__update_state()
    @property
    def lamp_last_updated_secs(self) :
        return now_secs() - self.__state.get('lamp_state_time', 0.0)
    @property
    def pir_last_updated_secs(self) :
        return now_secs() - self.__state.get('pir_state_time', 0.0)
    @property
    def pir_state(self) :
        return self.__state.get('pir_state', None)
    @property
    def temperature(self) :
        return self.__state.get('temperature', None)
    @property
    def temperature_event_last_updated_secs(self) :
        events = self.__state.get('temperature_events',[])
        last_updated = 0.0
        if len(events) > 0 :
            last_updated = events[-1]['time']
        return now_secs() - last_updated

class Toggle(object) :
    '''Represents an output to a GPIO pin'''
    def __init__(self, pin, min_time) :
        io.setup(pin, io.OUT)
        io.output(pin, io.LOW)
        self.__pin = pin
        self.__on = False
        self.__min_time = min_time
        self.__last = now_secs()
    def toggle(self) :
        return self.enable(not self.__on)
    def enable(self, on) :
        duration = now_secs() - self.__last

        if duration < self.__min_time :
            return False
        if on == self.__on :
            return True
        io.output(self.__pin, io.HIGH if on else io.LOW)
        self.__last = now_secs()
        self.__on = on
        return True
    @property
    def is_enabled(self) :
        return self.__on

def led_toggle(pin) :
    return Toggle(pin, 0.0)

def ac_toggle(pin) :
    return Toggle(pin, _AC_RELAY_MIN_TOGGLE_TIME)

_TEMP_DATA_PAT = re.compile(r't=(\d+)')

def read_w1_temp() :
    fnames = glob.glob('/sys/bus/w1/devices/28-*/w1_slave')
    if len(fnames) == 0 :
        raise Exception, 'No Wire-1 slaves detected'
    debug('Found %d W1 slave files' % len(fnames))
    with file(fnames[0], 'rb') as f :
        header = f.readline().strip()
        if not header.endswith('YES') :
            return None
        data = f.readline().strip()
        temp_match = _TEMP_DATA_PAT.search(data)
        if not temp_match :
            raise Exception, 'Invalid data from temperature sensor: %s' % data
        temp_c = int(temp_match.group(1)) / 1000.0
        temp_f = temp_c * 1.8 + 32.0
        return temp_f

def temperature_proc_func(temperature_queue, termination_flag) :
    '''Seperate process for reading temperature'''
    while not termination_flag.value :
        try :
            temperature_queue.put(read_w1_temp())
            time.sleep(_TEMPERATURE_SLEEP_TIME)
        except :
            error('Temperature loop error', exc_info = True)

def command_proc_func(sqs_queue_name, control_queue, termination_flag) :
    '''Command queue for SQS commands'''
    info('Initializing SQS command processor...')
    sqs_conn = sqs.connect_to_region(_AWS_REGION)
    sqs_queue = sqs_conn.get_queue(sqs_queue_name)
    sqs_queue.set_message_class(sqs.message.RawMessage)
    while not termination_flag.value :
        messages = sqs_queue.get_messages(10, wait_time_seconds = 20)
        info('Read %d messages from SQS' % len(messages))
        for message in messages :
            command = message.get_body()
            control_queue.put(command)
            sqs_queue.delete_message(message)

def spawn_process(target, args) :
    proc = mp.Process(target = target, args = args)
    proc.start()
    return proc

class Processes(object) :
    def __init__(self) :
        self.__termination_flag = mp.Value('l', 0)
        self.__processes = []
    def spawn(self, target, args) :
        self.__processes.append(spawn_process(target, args))
    @property
    def termination_flag(self) :
        return self.__termination_flag
    def finish(self) :
        self.__termination_flag.value = 1
        for process in self.__processes :
            process.join()

def drain_queue(data_queue) :
    values = []
    try :
        while True :
            values.append(data_queue.get_nowait())
    except queue.Empty :
        pass
    return values

def main(args) :
    if len(args) != 4 :
        print >> sys.stderr, 'USAGE: %s TABLE QUEUE ID' % args[0]
        sys.exit(2)
    # logging
    logging.basicConfig(
        format = '%(asctime)s %(levelname)-8s%(funcName)-20s %(message)s',
        level = logging.INFO
    )

    # args
    state_table_name = args[1]
    command_queue_name = args[2]
    unit_id = args[3]


    processes = Processes()
    # spawn off command process
    control_queue = mp.Queue()
    processes.spawn(
        target = command_proc_func,
        args = (command_queue_name, control_queue, processes.termination_flag)
    )
    # spawn off temperature process
    temperature_queue = mp.Queue()
    processes.spawn(
        target = temperature_proc_func,
        args = (temperature_queue, processes.termination_flag)
    )

    # operational table
    info('Initializing DDB backed operational state...')
    state = State(state_table_name, unit_id)

    # GPIO setup
    try :
        io.setmode(io.BCM)
        io.setup(_PIR_PIN, io.IN)
            
        error_led = led_toggle(_RED_LED_PIN)
        pir_led = led_toggle(_GREEN_LED_PIN)
        lamp = ac_toggle(_AC_RELAY_PIN)

        # command jump table via the class/lexical closure
        class CommandProcessor(object) :
            def cmd_lamp(self, val) :
                if val == 'on' :
                    if lamp.enable(True) :
                        info('Turning on lamp via command')
                        state.update_lamp(True)
                elif val == 'off' :
                    if lamp.enable(False) :
                        info('Turning off lamp via command')
                        state.update_lamp(False)
                else :
                    warn('Invalid lamp command value: %s' % val)
        command_processor = CommandProcessor()

        # event loop
        event_count = 0
        motion_count = 0
        info('Temperature window size: %d' % _TEMPERATURE_WINDOW_SIZE)
        temperatures = collections.deque(maxlen = _TEMPERATURE_WINDOW_SIZE)
        info('Starting main event loop...')
        while True :
            try :
                event_count += 1

                if io.input(_PIR_PIN) :
                    motion_count += 1
                    pir_led.enable(True)
                    # turn on lamp
                    if not lamp.is_enabled :
                        if lamp.enable(True) :
                            info('Turning on lamp via motion')
                            state.update_lamp(True)
                elif event_count == 1 :
                    pir_led.enable(False)

                temperatures.extend(drain_queue(temperature_queue))

                # get commands
                command_strs = drain_queue(control_queue)
                for command_str in command_strs :
                    info('Received command: %s' % command_str)
                    command = json.loads(command_str)
                    command_unit_id = command.get('id', None)
                    if command_unit_id == unit_id :
                        command_type = command.get('type', None)
                        if command_type == None :
                            warn('No command type specified')
                        else :
                            command_func = getattr(command_processor, 'cmd_%s' % command_type, None)
                            if command_func is None :
                                warn('Unknown command: %s' % command_type)
                            else :
                                command_func(command['value'])
                    else :
                        info('Ignoring command for unit: %s' % command_unit_id)
                
                # flush event
                if event_count >= _LOG_FREQ :
                    info('Detected %d/%d events in %0.2f seconds' \
                            % (motion_count, event_count, _LOG_TIME))
                    state.update_motion(motion_count > 0)
                    # turn off lamp
                    if state.pir_state == 'IDLE' \
                            and state.pir_last_updated_secs > _IDLE_AC_TURNOFF_TIME \
                            and state.lamp_last_updated_secs > _IDLE_AC_TURNOFF_TIME \
                            and lamp.is_enabled:
                        info('Idle for %0.2fs, shutting off lamp' % state.pir_last_updated_secs)
                        lamp.enable(False)
                        state.update_lamp(False)

                    event_count = 0
                    motion_count = 0

                    # record temperature
                    if len(temperatures) > 0 :
                        temperature = sum(temperatures) / len(temperatures)
                        info('Detected temperature: %0.2f F' % temperature)
                        if state.temperature is None \
                                or abs(state.temperature - temperature) > _TEMPERATURE_TOLERANCE :
                            info('Updating temperature state')
                            state.update_temperature(temperature)

                if error_led.is_enabled :
                    info('Event loop succeeded, switching error LED off')
                    error_led.enable(False)
                time.sleep(_SLEEP_TIME)
            except :
                error('Event loop error', exc_info = True)
                error_led.enable(True)
                if sys.exc_info()[0] is KeyboardInterrupt :
                    raise sys.exc_info()[1]
    finally :
        processes.finish()
        io.cleanup()

if __name__ == '__main__' :
    main(sys.argv)
