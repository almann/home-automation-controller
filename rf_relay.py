#!/usr/bin/env python

import sys
import time
import json
import subprocess

import multiprocessing      as mp
import Queue                as queue

import boto.sqs             as sqs

import logging
from   logging              import warning as warn
from   logging              import debug
from   logging              import info
from   logging              import error

_AWS_REGION                 = "us-west-2"

_SLEEP_TIME                 = 0.05

def command_looper(sqs_queue_name, control_queue) :
    info('Initializing SQS command processor...')
    info('Connecting to: %s' % sqs_queue_name)
    sqs_conn = sqs.connect_to_region(_AWS_REGION)
    sqs_queue = sqs_conn.get_queue(sqs_queue_name)
    sqs_queue.set_message_class(sqs.message.RawMessage)
    def command_proc_loop() :
        messages = sqs_queue.get_messages(10, wait_time_seconds = 20)
        info('Read %d messages from SQS' % len(messages))
        for message in messages :
            command = message.get_body()
            control_queue.put(command)
            sqs_queue.delete_message(message)
    return command_proc_loop

def spawn_process(target, args) :
    proc = mp.Process(target = target, args = args)
    proc.start()
    return proc

def process_loop(looper, termination_flag, sleep_time, *args) :
    loop_func = looper(*args)
    while not termination_flag.value :
        try :
            loop_func()
            if sleep_time is not None :
                time.sleep(sleep_time)
        except :
            error('%s loop error' % loop_func.__name__, exc_info = True)

class Processes(object) :
    def __init__(self) :
        self.__termination_flag = mp.Value('l', 0)
        self.__processes = []
    def spawn(self, target, args) :
        self.__processes.append(spawn_process(target, args))
    def spawn_loop(self, looper, sleep_time, args) :
        self.spawn(process_loop, args = (looper, self.__termination_flag, sleep_time) + args)
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
    if len(args) != 5 :
        print >> sys.stderr, 'USAGE: %s CODES-FILE CODESEND-COMMAND QUEUE ID' % args[0]
        sys.exit(2)
    # logging
    logging.basicConfig(
        format = '%(asctime)s %(levelname)-8s%(funcName)-20s %(message)s',
        level = logging.INFO
    )

    # args
    codes_file_name = args[1]
    codesend_cmd = args[2]
    command_queue_name = args[3]
    unit_id = args[4]

    processes = Processes()

    # spawn off command process
    control_queue = mp.Queue()
    processes.spawn_loop(
        looper = command_looper,
        sleep_time = None,
        args = (command_queue_name, control_queue)
    )

    try :
        info('RF code sending command: %s' % codesend_cmd)
        info('Loading code file: %s' % codes_file_name)
        with file(codes_file_name, 'rb') as codes_file:
            codes = json.load(codes_file)

        # event loop
        info('Starting main event loop...')
        while True :
            try :
                # get commands
                command_strs = drain_queue(control_queue)
                for command_str in command_strs :
                    info('Received command: %s' % command_str)
                    command = json.loads(command_str)
                    command_unit_id = command.get('id', None)
                    if command_unit_id != unit_id :
                        info('Ignoring command for unit: %s' % command_unit_id)
                        continue

                    command_type = command.get('type', None)
                    if command_type == None :
                        warn('No command type specified')
                        continue
                    
                    info('Received command type: %r' % command_type)
                    if not isinstance(command_type, (int, long)) or command_type < 0 or command_type >= len(codes):
                        warn('Invalid command type')
                        continue

                    value = command['value']
                    info('Received command value: %r' % value)
                    if value == 'on':
                        idx = 0
                    elif value == 'off':
                        idx = 1
                    else:
                        warn('Unknown command value: %r' % value)
                        continue
                    
                    code = codes[command_type][idx]
                    info('Executing RF code: %d' % code)
                    subprocess.check_call([codesend_cmd, str(code)])
                
                time.sleep(_SLEEP_TIME)
            except :
                error('Event loop error', exc_info = True)
                if sys.exc_info()[0] is KeyboardInterrupt :
                    raise sys.exc_info()[1]
    finally :
        processes.finish()

if __name__ == '__main__' :
    main(sys.argv)
