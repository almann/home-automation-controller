#!/usr/bin/env python

import sys
import time
import datetime
import json
import traceback
import zlib

import multiprocessing      as mp
import Queue                as queue

import RPi.GPIO             as io

import boto.dynamodb2       as ddb
import boto.dynamodb.types  as ddb_types

import boto.dynamodb2.table
import boto.dynamodb2.fields 

import boto.sqs             as sqs

_AWS_REGION = "us-west-2"

_AC_RELAY_PIN = 4
_AC_RELAY_MIN_TOGGLE_TIME = 5.0
_PIR_PIN = 18
_RED_LED_PIN = 23
_GREEN_LED_PIN = 24
_SLEEP_TIME = 0.05
_LOG_TIME = 5.0
_LOG_FREQ = int(_LOG_TIME / _SLEEP_TIME)

_IDLE_AC_TURNOFF_TIME = 180.0

_MAX_EVENTS = 100

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
    def __update_state_field(self, field, flag) :
        state_label = 'ACTIVE' if flag else 'IDLE'
        if self.__state.get(field, None) != state_label :
            self.__state[field] = state_label
            self.__state['%s_time' % field] = now_secs()
            self.__update_state()
    def update_motion(self, has_motion) :
        self.__update_state_field('pir_state', has_motion)
    def update_lamp(self, is_on) :
        self.__update_state_field('lamp_state', is_on)
    @property
    def pir_last_updated_secs(self) :
        return now_secs() - self.__state.get('pir_state_time', 0.0)
    @property
    def pir_state(self) :
        return self.__state.get('pir_state', None)

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

def p_command_func(sqs_queue_name, control_queue, termination_flag) :
    '''Command queue for SQS commands'''
    sqs_conn = sqs.connect_to_region(_AWS_REGION)
    sqs_queue = sqs_conn.get_queue(sqs_queue_name)
    sqs_queue.set_message_class(sqs.message.RawMessage)
    print >> sys.stderr, 'Initializing Command Queue...'
    while not termination_flag.value :
        messages = sqs_queue.get_messages(10, wait_time_seconds = 20)
        print >> sys.stderr, 'Read %d messages from SQS' % len(messages)
        for message in messages :
            command = message.get_body()
            control_queue.put(command)
            sqs_queue.delete_message(message)


def main(args) :
    if len(args) != 4 :
        print >> sys.stderr, 'USAGE: %s TABLE QUEUE ID' % args[0]
        sys.exit(2)
    # args
    state_table_name = args[1]
    command_queue_name = args[2]
    unit_id = args[3]

    control_queue = mp.Queue()
    termination_flag = mp.Value('l', 0)

    # spawn off command process
    p_command = mp.Process(
        target = p_command_func,
        args = (command_queue_name, control_queue, termination_flag)
    )
    p_command.start()

    # operational table
    state = State(state_table_name, unit_id)

    # GPIO setup
    try :
        io.setmode(io.BCM)
        io.setup(_PIR_PIN, io.IN)
            
        error_led = led_toggle(_RED_LED_PIN)
        pir_led = led_toggle(_GREEN_LED_PIN)
        lamp = ac_toggle(_AC_RELAY_PIN)

        # event loop
        event_count = 0
        motion_count = 0
        while True :
            try :
                event_count += 1

                if io.input(_PIR_PIN) :
                    motion_count += 1
                    pir_led.enable(True)
                    # turn on lamp
                    if not lamp.is_enabled :
                        ok = lamp.enable(True)
                        state.update_lamp(True)

                elif event_count == 1 :
                    pir_led.enable(False)

                # get commands
                try :
                    command_str = control_queue.get_nowait()
                    print >> sys.stderr, 'Received command: %s' % command_str
                    command = json.loads(command_str)
                    # TODO clean this up
                    if command['type'] == 'light' :
                        if command['value'] == 'on' :
                            lamp.enable(True)
                        elif command['value'] == 'off' :
                            lamp.enable(False)
                except queue.Empty :
                    pass
                
                # flush event
                if event_count >= _LOG_FREQ :
                    print >> sys.stderr, 'Detected %d/%d events in %0.2f seconds' \
                            % (motion_count, event_count, _LOG_TIME)
                    state.update_motion(motion_count > 0)
                    # turn off lamp
                    if state.pir_state == 'IDLE' \
                            and state.pir_last_updated_secs > _IDLE_AC_TURNOFF_TIME \
                            and lamp.is_enabled:
                        print >> sys.stderr, 'Idle for %0.2fs, shutting off lamp' % state.pir_last_updated_secs 
                        lamp.enable(False)
                        state.update_lamp(False)

                    event_count = 0
                    motion_count = 0

                error_led.enable(False)
                time.sleep(_SLEEP_TIME)
            except :
                traceback.print_exc()
                error_led.enable(True)
                if sys.exc_info()[0] is KeyboardInterrupt :
                    raise sys.exc_info()[1]
    finally :
        termination_flag.value = 1
        p_command.join()
        io.cleanup()

if __name__ == '__main__' :
    main(sys.argv)
