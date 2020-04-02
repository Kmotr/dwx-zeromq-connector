# -*- coding: utf-8 -*-

"""
    DWX_ZeroMQ_Connector_v2_0_2_RC8.py
    --
    @author: Darwinex Labs (www.darwinex.com)
    
    Copyright (c) 2017-2019, Darwinex. All rights reserved.
    
    Licensed under the BSD 3-Clause License, you may not use this file except 
    in compliance with the License. 
    
    You may obtain a copy of the License at:    
    https://opensource.org/licenses/BSD-3-Clause
"""

from threading import Thread
from time import sleep

# IMPORT zmq library
# import zmq, time
import zmq
from pandas import DataFrame, Timestamp


# DEFAULT ORDER DICT
def generate_default_order_dict():
    return ({'action': 'OPEN',
             'type': 0,
             'symbol': 'EURUSD',
             'price': 0.0,
             'stop_loss': 500,  # SL/TP in POINTS, not pips.
             'take_profit': 500,
             'comment': 'DWX_Python_to_MT',
             'lot_size': 0.01,
             'magic': 123456,
             'ticket': 0})


# DEFAULT DATA REQUEST DICT
def generate_default_data_dict():
    return ({'action': 'DATA',
             'symbol': 'EURUSD',
             'timeframe': 1440,  # M1 = 1, M5 = 5, and so on..
             'start': '2018.12.21 17:00:00',  # timestamp in MT4 recognized format
             'end': '2018.12.21 17:05:00'})


# DEFAULT HIST REQUEST DICT
def generate_default_hist_dict():
    return ({'action': 'HIST',
             'symbol': 'EURUSD',
             'timeframe': 1,  # M1 = 1, M5 = 5, and so on..
             'start': '2018.12.21 17:00:00',  # timestamp in MT4 recognized format
             'end': '2018.12.21 17:05:00'})


class DWXZeroMQConnector:
    """
    Setup ZeroMQ -> MetaTrader Connector
    """

    def __init__(self,
                 client_id='DLabs_Python',  # Unique ID for this client
                 host='localhost',  # Host to connect to
                 protocol='tcp',  # Connection protocol
                 push_port=32768,  # Port for Sending commands
                 pull_port=32769,  # Port for Receiving responses
                 sub_port=32770,  # Port for Subscribing for prices
                 delimiter=';',  # String delimiter
                 pull_handlers=None,  # Handlers to process data received through PULL port.
                 sub_handlers=None,  # Handlers to process data received through SUB port.
                 _verbose=False):

        # Strategy state (if this is False, ZeroMQ will not listen for data)
        if sub_handlers is None:
            sub_handlers = []
        if pull_handlers is None:
            pull_handlers = []
        self._ACTIVE = True

        # Client ID
        self._client_id = client_id

        # ZeroMQ Host
        self._host = host

        # Connection Protocol
        self._protocol = protocol

        # ZeroMQ Context
        self._zmq_context = zmq.Context()

        # TCP Connection URL Template
        self._url = self._protocol + "://" + self._host + ":"

        # Ports for PUSH, PULL and SUB sockets respectively
        self._push_port = push_port
        self._pull_port = pull_port
        self._sub_port = sub_port

        # Handlers for received data (pull and sub ports)
        self._pull_handlers = pull_handlers
        self._sub_handlers = sub_handlers

        # Create Sockets
        self._push_socket = self._zmq_context.socket(zmq.PUSH)
        self._push_socket.setsockopt(zmq.SNDHWM, 1)

        self._pull_socket = self._zmq_context.socket(zmq.PULL)
        self._pull_socket.setsockopt(zmq.RCVHWM, 1)

        self._sub_socket = self._zmq_context.socket(zmq.SUB)

        # Bind PUSH Socket to send commands to MetaTrader
        self._push_socket.connect(self._url + str(self._push_port))
        print("[INIT] Ready to send commands to METATRADER (PUSH): " + str(self._push_port))

        # Connect PULL Socket to receive command responses from MetaTrader
        self._pull_socket.connect(self._url + str(self._pull_port))
        print("[INIT] Listening for responses from METATRADER (PULL): " + str(self._pull_port))

        # Connect SUB Socket to receive market data from MetaTrader
        self._sub_socket.connect(self._url + str(self._sub_port))

        # Initialize POLL set and register PULL and SUB sockets
        self._poller = zmq.Poller()
        self._poller.register(self._pull_socket, zmq.POLLIN)
        self._poller.register(self._sub_socket, zmq.POLLIN)

        # Start listening for responses to commands and new market data
        self._string_delimiter = delimiter

        # BID/ASK Market Data Subscription Threads ({SYMBOL: Thread})
        self._MarketData_Thread = None

        # Begin polling for PULL / SUB data
        self._MarketData_Thread = Thread(target=self.poll_data, args=self._string_delimiter)
        self._MarketData_Thread.start()

        # Market Data Dictionary by Symbol (holds tick data) or Instrument (holds OHLC data)
        self._Market_Data_DB = {}  # {SYMBOL: {TIMESTAMP: (BID, ASK)}}
        # {SYMBOL: {TIMESTAMP: (TIME, OPEN, HIGH, LOW, CLOSE, TICKVOL, SPREAD, VOLUME)}}

        # Temporary Order STRUCT for convenience wrappers later.
        self.temp_order_dict = generate_default_order_dict()

        # Thread returns the most recently received DATA block here
        self._thread_data_output = None

        # Verbosity
        self._verbose = _verbose

    ##########################################################################

    """
    Set state (to enable/disable strategy manually)
    """

    def set_state(self, new_state=False):

        self._ACTIVE = new_state
        print("\n**\n[KERNEL] Setting state to {} - Deactivating Threads.. please wait a bit.\n**".format(new_state))

    ##########################################################################

    """
    Function to send commands to MetaTrader (PUSH)
    """

    def remote_send(self, socket, data):

        try:
            socket.send_string(data, zmq.DONTWAIT)
        except zmq.error.Again:
            print("\nResource timeout.. please try again.")
            sleep(0.000000001)

    ##########################################################################

    def _get_response_(self):
        return self._thread_data_output

    ##########################################################################

    def _set_response_(self, _resp=None):
        self._thread_data_output = _resp

    ##########################################################################

    def _valid_response_(self, input_='zmq'):

        # Valid data types
        _types = (dict, DataFrame)

        # If _input = 'zmq', assume self._zmq._thread_data_output
        if isinstance(input_, str) and input_ == 'zmq':
            return isinstance(self._get_response_(), _types)
        else:
            return isinstance(input_, _types)

    ##########################################################################

    """
    Function to retrieve data from MetaTrader (PULL or SUB)
    """

    def remote_recv(self, socket):

        try:
            msg = socket.recv_string(zmq.DONTWAIT)
            return msg
        except zmq.error.Again:
            print("\nResource timeout.. please try again.")
            sleep(0.000001)

        return None

    ##########################################################################

    # Convenience functions to permit easy trading via underlying functions.

    # OPEN ORDER
    def new_trade(self, order=None):

        if order is None:
            order = generate_default_order_dict()

        # Execute
        self.send_command(**order)

    # MODIFY ORDER
    def modify_trade_by_ticket(self, ticket, stop_loss, take_profit):  # in points

        try:
            self.temp_order_dict['_action'] = 'MODIFY'
            self.temp_order_dict['_SL'] = stop_loss
            self.temp_order_dict['_TP'] = take_profit
            self.temp_order_dict['_ticket'] = ticket

            # Execute
            self.send_command(**self.temp_order_dict)

        except KeyError:
            print("[ERROR] Order Ticket {} not found!".format(ticket))

    # CLOSE ORDER
    def close_trade_by_ticket(self, ticket):

        try:
            self.temp_order_dict['action'] = 'CLOSE'
            self.temp_order_dict['ticket'] = ticket

            # Execute
            self.send_command(**self.temp_order_dict)

        except KeyError:
            print("[ERROR] Order Ticket {} not found!".format(ticket))

    # CLOSE PARTIAL
    def close_partial_by_ticket(self, ticket, lot_size):

        try:
            self.temp_order_dict['action'] = 'CLOSE_PARTIAL'
            self.temp_order_dict['ticket'] = ticket
            self.temp_order_dict['lot_size'] = lot_size

            # Execute
            self.send_command(**self.temp_order_dict)

        except KeyError:
            print("[ERROR] Order Ticket {} not found!".format(ticket))

    # CLOSE MAGIC
    def close_trades_by_magic(self, magic):

        try:
            self.temp_order_dict['action'] = 'CLOSE_MAGIC'
            self.temp_order_dict['magic'] = magic

            # Execute
            self.send_command(**self.temp_order_dict)

        except KeyError:
            pass

    # CLOSE ALL TRADES
    def close_all_trades(self):

        try:
            self.temp_order_dict['action'] = 'CLOSE_ALL'

            # Execute
            self.send_command(**self.temp_order_dict)

        except KeyError:
            pass

    # GET OPEN TRADES
    def get_open_trades(self):

        try:
            self.temp_order_dict['action'] = 'GET_OPEN_TRADES'

            # Execute
            self.send_command(**self.temp_order_dict)

        except KeyError:
            pass

    ##########################################################################
    """
    Function to construct messages for sending DATA commands to MetaTrader
    """

    def send_marketdata_request(self,
                                symbol='EURUSD',
                                timeframe=1,
                                start='2019.01.04 17:00:00',
                                end=Timestamp.now().strftime('%Y.%m.%d %H:%M:00')):
        # _end='2019.01.04 17:05:00'):

        _msg = "{};{};{};{};{}".format('DATA',
                                       symbol,
                                       timeframe,
                                       start,
                                       end)
        # Send via PUSH Socket
        self.remote_send(self._push_socket, _msg)

    ##########################################################################
    """
    Function to construct messages for sending HIST commands to MetaTrader
    """

    def send_markethist_request(self,
                                symbol='EURUSD',
                                timeframe=1,
                                start='2019.01.04 17:00:00',
                                end=Timestamp.now().strftime('%Y.%m.%d %H:%M:00')):
        # _end='2019.01.04 17:05:00'):

        _msg = "{};{};{};{};{}".format('HIST',
                                       symbol,
                                       timeframe,
                                       start,
                                       end)
        # Send via PUSH Socket
        self.remote_send(self._push_socket, _msg)

    ##########################################################################
    """
    Function to construct messages for sending TRACK_PRICES commands to MetaTrader
    """

    def send_trackprices_request(self,
                                 symbols=None):
        if symbols is None:
            symbols = ['EURUSD']
        _msg = 'TRACK_PRICES'
        for s in symbols:
            _msg = _msg + ";{}".format(s)

        # Send via PUSH Socket
        self.remote_send(self._push_socket, _msg)

    ##########################################################################
    """
    Function to construct messages for sending TRACK_RATES commands to MetaTrader
    """

    def send_trackrates_request(self,
                                instruments=None):
        if instruments is None:
            instruments = [('EURUSD_M1', 'EURUSD', 1)]
        _msg = 'TRACK_RATES'
        for i in instruments:
            _msg = _msg + ";{};{}".format(i[1], i[2])

        # Send via PUSH Socket
        self.remote_send(self._push_socket, _msg)

    ##########################################################################
    """
    Function to construct messages for sending Trade commands to MetaTrader
    """

    def send_command(self, action='OPEN', type=0,
                     symbol='EURUSD', price=0.0,
                     stop_loss=50, take_profit=50, comment="Python-to-MT",
                     lot_size=0.01, magic=123456, ticket=0):

        _msg = "{};{};{};{};{};{};{};{};{};{};{}".format('TRADE', action, type,
                                                         symbol, price,
                                                         stop_loss, take_profit, comment,
                                                         lot_size, magic,
                                                         ticket)

        # Send via PUSH Socket
        self.remote_send(self._push_socket, _msg)

        """
         compArray[0] = TRADE or DATA
         compArray[1] = ACTION (e.g. OPEN, MODIFY, CLOSE)
         compArray[2] = TYPE (e.g. OP_BUY, OP_SELL, etc - only used when ACTION=OPEN)
         
         For compArray[0] == DATA, format is: 
             DATA|SYMBOL|TIMEFRAME|START_DATETIME|END_DATETIME
         
         // ORDER TYPES: 
         // https://docs.mql4.com/constants/tradingconstants/orderproperties
         
         // OP_BUY = 0
         // OP_SELL = 1
         // OP_BUYLIMIT = 2
         // OP_SELLLIMIT = 3
         // OP_BUYSTOP = 4
         // OP_SELLSTOP = 5
         
         compArray[3] = Symbol (e.g. EURUSD, etc.)
         compArray[4] = Open/Close Price (ignored if ACTION = MODIFY)
         compArray[5] = SL
         compArray[6] = TP
         compArray[7] = Trade Comment
         compArray[8] = Lots
         compArray[9] = Magic Number
         compArray[10] = Ticket Number (MODIFY/CLOSE)
         """
        # pass

    ##########################################################################

    """
    Function to check Poller for new reponses (PULL) and market data (SUB)
    """

    def poll_data(self,
                  string_delimiter=';'):

        while self._ACTIVE:

            sockets = dict(self._poller.poll())

            # Process response to commands sent to MetaTrader
            if self._pull_socket in sockets and sockets[self._pull_socket] == zmq.POLLIN:

                try:

                    msg = self._pull_socket.recv_string(zmq.DONTWAIT)

                    # If data is returned, store as pandas Series
                    if msg != '' and msg is not None:

                        try:
                            _data = eval(msg)

                            self._thread_data_output = _data
                            if self._verbose:
                                print(_data)  # default logic
                            # invokes data handlers on pull port
                            for hnd in self._pull_handlers:
                                hnd.onPullData(_data)

                        except Exception as ex:
                            _exstr = "Exception Type {0}. Args:\n{1!r}"
                            _msg = _exstr.format(type(ex).__name__, ex.args)
                            print(_msg)

                except zmq.error.Again:
                    pass  # resource temporarily unavailable, nothing to print
                except ValueError:
                    pass  # No data returned, passing iteration.
                except UnboundLocalError:
                    pass  # _symbol may sometimes get referenced before being assigned.

            # Receive new market data from MetaTrader
            if self._sub_socket in sockets and sockets[self._sub_socket] == zmq.POLLIN:

                try:
                    msg = self._sub_socket.recv_string(zmq.DONTWAIT)

                    if msg != "":
                        _timestamp = str(Timestamp.now('UTC'))[:-6]
                        _symbol, _data = msg.split(" ")
                        if len(_data.split(string_delimiter)) == 2:
                            _bid, _ask = _data.split(string_delimiter)
                            if self._verbose:
                                print("\n[" + _symbol + "] " + _timestamp + " (" + _bid + "/" + _ask + ") BID/ASK")
                                # Update Market Data DB
                            if _symbol not in self._Market_Data_DB.keys():
                                self._Market_Data_DB[_symbol] = {}
                            self._Market_Data_DB[_symbol][_timestamp] = (float(_bid), float(_ask))

                        elif len(_data.split(string_delimiter)) == 8:
                            _time, _open, _high, _low, _close, _tick_vol, _spread, _real_vol = _data.split(
                                string_delimiter)
                            if self._verbose:
                                print(
                                    "\n[" + _symbol + "] " + _timestamp + " (" + _time + "/" + _open + "/" + _high + "/" + _low + "/" + _close + "/" + _tick_vol + "/" + _spread + "/" + _real_vol + ") TIME/OPEN/HIGH/LOW/CLOSE/TICKVOL/SPREAD/VOLUME")
                                # Update Market Rate DB
                            if _symbol not in self._Market_Data_DB.keys():
                                self._Market_Data_DB[_symbol] = {}
                            self._Market_Data_DB[_symbol][_timestamp] = (
                                int(_time), float(_open), float(_high), float(_low), float(_close), int(_tick_vol),
                                int(_spread), int(_real_vol))
                        # invokes data handlers on sub port
                        for hnd in self._sub_handlers:
                            hnd.onSubData(msg)

                except zmq.error.Again:
                    pass  # resource temporarily unavailable, nothing to print
                except ValueError:
                    pass  # No data returned, passing iteration.
                except UnboundLocalError:
                    pass  # _symbol may sometimes get referenced before being assigned.

    ##########################################################################

    """
    Function to subscribe to given Symbol's BID/ASK feed from MetaTrader
    """

    def subscribe_marketdata(self, symbol, delimiter=';'):

        # Subscribe to SYMBOL first.
        self._sub_socket.setsockopt_string(zmq.SUBSCRIBE, symbol)

        if self._MarketData_Thread is None:
            self._MarketData_Thread = Thread(target=self.poll_data, args=delimiter)
            self._MarketData_Thread.start()

        print("[KERNEL] Subscribed to {} MARKET updates. See self._Market_Data_DB.".format(symbol))

    """
    Function to unsubscribe to given Symbol's BID/ASK feed from MetaTrader
    """

    def unsubscribe_marketdata(self, symbol):

        self._sub_socket.setsockopt_string(zmq.UNSUBSCRIBE, symbol)
        print("\n**\n[KERNEL] Unsubscribing from " + symbol + "\n**\n")

    """
    Function to unsubscribe from ALL MetaTrader Symbols
    """

    def unsubscribe_all_marketdata(self):

        self.set_state(False)
        self._MarketData_Thread = None

    ##########################################################################
