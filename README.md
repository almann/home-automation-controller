Home Automation Controller
==========================
This is a simple project for a home automation controller for the Raspberry Pi.
It uses the Python GPIO bindings to communicate with sensors, LEDs, and A/C relays to control things like lamps.

This software relies on:

* Boto Library for AWS (operational state, remote commands)
* GPIO Library for Raspberry Pi

To use the controller script:

1. Create a DynamoDB table with a string hash key named `id`
2. Create an SQS queue.

Running the command is as follows:

    sudo python controller.py TABLE-NAME QUEUE-NAME ID

`TABLE-NAME` is the DynamoDB state table, the `ID` provided will store operational state as a binary field in DynamoDB named `state`.  This binary field is ZLIB compressed JSON.  The contoller uses this state to manage itself, but really it is intended for another application to dashboard.  The SQS queue named `QUEUE-NAME` is used to dispatch commands to the controller.  These commands are JSON and are formatted as:

    {"id":"banana", "type":"light", "value":"on"}

The above command sent to SQS would be interpreted by the controller if it were initialized with the `ID` of `banana`.  The reason for the `id` field is multiplexing commands over SNS to SQS for a network of these controllers throughout the house.  Another concept to be added later might be a `group` for rooms with sets of controllers.

Note that root is required to interface with the GPIO pins.
