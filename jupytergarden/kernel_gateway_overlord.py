# @todo change timeout of ws requests
##
from icecream import ic
from datetime import timedelta
import os
# import json
from tornado import gen
from tornado.escape import json_encode, json_decode, url_escape
from tornado.websocket import websocket_connect
from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.options import options, parse_command_line, define
from time import sleep
from uuid import uuid4
from brish import CmdResult
##
class JupyterResult(CmdResult):
    out_data: dict
    err_data: dict
    extra_data: dict

    def __init__(self, *args, out_data=None, err_data=None, extra_data=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.out_data = out_data
        self.err_data = err_data
        self.extra_data = extra_data
##
gateway_base_url = os.getenv('JUPYTERGARDEN_GATEWAY_HTTP_URL', 'http://localhost:7331')
ic(gateway_base_url)
gateway_base_ws_url = os.getenv('JUPYTERGARDEN_GATEWAY_WS_URL', 'ws://localhost:7331')
ic(gateway_base_ws_url)

async def kernel_id_get(kernel_name=None, kernel_id=None, auth_username='fakeuser', auth_password='fakepass'):

    client = AsyncHTTPClient()
    if not kernel_id:
        assert kernel_name != None

        response = await client.fetch(
            '{}/api/kernels'.format(gateway_base_url),
            method='POST',
            auth_username=auth_username,
            auth_password=auth_password,
            body=(json_encode({'name' : kernel_name}))
        )
        kernel = json_decode(response.body)
        kernel_id = kernel['id']
        print(f'Created kernel: {kernel_id}')

    return kernel_id


async def kernel_ws_get(kernel_id=None,
                        auth_username='fakeuser',
                        auth_password='fakepass',
                        connect_timeout=0,
                        request_timeout=0,
                        **kwargs):
    kernel_id = await kernel_id_get(kernel_id=kernel_id,
                              auth_username=auth_username,
                                    auth_password=auth_password, **kwargs)

    # :arg float connect_timeout: Timeout for initial connection in seconds,
    #   default 20 seconds (0 means no timeout)
    # :arg float request_timeout: Timeout for entire request in seconds,
    #   default 20 seconds (0 means no timeout)
    ws_req = HTTPRequest(url='{}/api/kernels/{}/channels'.format(
        gateway_base_ws_url,
        url_escape(kernel_id)
    ),
                         auth_username=auth_username,
                         auth_password=auth_password,
                         connect_timeout=connect_timeout,
                         request_timeout=request_timeout,
                         )
    ws = await websocket_connect(ws_req)
    print('Connected to kernel websocket')
    return ws

def kernel_ws_close(ws):
    ws.close()

async def kernel_ws_eval(ws, code, session='', close_after=True, isDbg=False, logger=None):
    try:
        msg_id = uuid4().hex

        # I could not find any timeout functionality for this in [[~/anaconda/lib/python3.7/site-packages/tornado/websocket.py]]
        ws.write_message(json_encode({
            'header': {
                'username': '',
                'version': '5.0',
                'session': session, # @idk what use this has, changing it does NOT isolate the variables
                'msg_id': msg_id,
                'msg_type': 'execute_request'
            },
            'parent_header': {},
            'channel': 'shell',
            'content': {
                'code': code,
                'silent': False,
                'store_history': False,
                'user_expressions' : {},
                'allow_stdin' : False
            },
            'metadata': {},
            'buffers': {}
        }))

        # Look for stream output for the print in the execute
        stdout = ""
        stderr = ""
        out_data = dict()
        err_data = dict()
        extra_data = dict()
        retcode = 991010
        termianl_messages_received = {"idle": False, "execute_reply": False}
        while True:
            msg_json = await ws.read_message()

            if msg_json == None:
                logger and logger.warning("Empty message received from the websocket")
                continue
                # err_data['status'] = "empty_msg"
                # return JupyterResult(991011, stdout, stderr, code, "", out_data=out_data, err_data=err_data)


            msg = json_decode(msg_json)
            msg_type = msg['msg_type']
            print('Received message type:', msg_type)
            if msg_type == 'error':
                err_data['error_msg'] = msg['content']
                retcode = 991011

            parent_msg_id = msg['parent_header']['msg_id']
            if parent_msg_id == msg_id:
                if isDbg:
                    ic(msg)

                if msg_type == 'stream':
                    text = msg['content']['text']
                    stream_name = msg['content']['name']
                    if stream_name == 'stdout':
                        stdout = text
                    elif stream_name == 'stderr':
                        stderr = text
                elif msg_type == 'execute_reply':
                    status = msg['content']['status']
                    if status == 'ok':
                        retcode = 0

                    err_data['status'] = status

                    termianl_messages_received["execute_reply"] = True
                elif msg_type == 'execute_result':
                    out_data = msg['content']['data']

                elif msg_type == 'display_data':
                    # @idk what this message type is, really, so for now I am just tackling it on

                    extra_data['display_data'] = msg['content']['data']

                elif msg_type == 'status' and msg['content']['execution_state'] == 'idle':
                    # This idle status message indicates that IOPub messages associated with a given request have all been received.
                    # https://jupyter-client.readthedocs.io/en/latest/messaging.html#:~:text=%20after%20processing%20the%20request%20and%20publishing%20associated%20iopub%20messages%2C%20if%20any%2C%20the%20kernel%20publishes%20a%20status%3A%20idle%20message.%20this%20idle%20status%20message%20indicates%20that%20iopub%20messages%20associated%20with%20a%20given%20request%20have%20all%20been%20received.
                    termianl_messages_received["idle"] = True

                if termianl_messages_received["idle"] and termianl_messages_received["execute_reply"]:
                    return JupyterResult(retcode, stdout, stderr, code, "", out_data=out_data, err_data=err_data, extra_data=extra_data)

            else:
                if isDbg:
                    ic("not my parent", msg)

    finally:
        if close_after:
            kernel_ws_close(ws)
