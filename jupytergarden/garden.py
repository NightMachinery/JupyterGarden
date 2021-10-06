import logging
import os
import time
import brish
from brish import z, zp, UninitializedBrishException, zn, bool_from_str
from pynight.common_async import force_async, async_max_workers_set
from pynight.common_fastapi import FastAPISettings, EndpointLoggingFilter1, request_path_get, check_ip
from pynight.common_telegram import log_tlg
from .kernel_gateway_overlord import *
from tornado.websocket import WebSocketClosedError

import traceback
import re
from typing import Optional
from collections.abc import Iterable

from fastapi import FastAPI, Response, Request

settings = FastAPISettings()
app = FastAPI(openapi_url=settings.openapi_url)

logger = logging.getLogger("uvicorn")  # alt: from uvicorn.config import logger

isDbg = os.environ.get(
    "JUPYTERGARDEN_DEBUGME", False
)  # we can't reuse 'DEBUGME' or it will pollute the downstream
if isDbg:
    logger.info("Debug mode enabled")

skip_paths = ("/eval/nolog/", "/api/v1/eval/nolog/")
logging.getLogger("uvicorn.access").addFilter(EndpointLoggingFilter1(isDbg=isDbg, logger=logger, skip_paths=skip_paths))
###
max_workers_default = 16
try:
    max_workers = int(os.environ.get("JUPYTERGARDEN_N", max_workers_default))
except:
    max_workers = max_workers_default

executor = async_max_workers_set(max_workers)
###
@app.get("/")
def read_root():
    return {"Hello": "JupyterGarden"}


@app.post("/test/")
def test(body: dict):
    return body
###
pattern_magic = re.compile(r"(?im)^%GARDEN_(\S+)\s+((?:.|\n)*)$") # @duplicateCode/86da52eced14bf6baa394f50a9601812

session_to_kernel_ids = dict()
kernel_id_to_ws = dict()

@app.post("/eval/")
@app.post("/eval/nolog/")
async def eval_api(body: dict, request: Request):
    global session_to_kernel_ids
    global kernel_id_to_ws

    reuse_ws = False # old web sockets can timeout on the gateway server, it seems
    try:
        ##
        if isDbg:
            ic(body)
            # print(request.__dict__)
        ##
        ip, first_seen = check_ip(request, logger=logger)
        req_path = request_path_get(request)


        version = int(body.get("version", "1"))
        kernel_name = body.get("kernel_name", "julia-1.6")
        kernel_id = body.get("kernel_id", "")
        kernel_session = body.get("kernel_session", "")
        session = body.get("session", "")
        session_full = None
        if session:
            session_full = (kernel_name, session)

        if not kernel_id and session_full:
            kernel_id = session_to_kernel_ids.get(session_full, None)

        auth_username = 'fakeuser'
        auth_password = 'fakepass'
        cmd = body.get("cmd", "")
        json_output = bool_from_str(
            body.get("json_output", False)
        )  # old API had this named 'verbose'
        ##
        nolog = (
            not isDbg and ip == "127.0.0.1" and
            (bool_from_str(body.get("nolog", "")) or (req_path in skip_paths))
        )

        log_level = int(body.get("log_level", 1))
        if isDbg:
            log_level = max(log_level, 100)
        ##
        log = f"{ip} - cmd: {cmd}, session: {session}, kernel_id: {kernel_id}"
        nolog or logger.info(log) # @noflycheck
        first_seen and log_tlg(log)

        if cmd == "":
            return Response(content="Empty command received.", media_type="text/plain")

        ## @duplicateCode/86da52eced14bf6baa394f50a9601812
        magic_matches = pattern_magic.match(cmd)
        if magic_matches is not None:
            magic_head = magic_matches.group(1)
            magic_exp = magic_matches.group(2)
            log = f"Magic received: {magic_head}"
            logger.info(log)
            if magic_head == "ALL":
                for kernel_id, ws in kernel_id_to_ws:
                    try:
                        kernel_ws_close(ws)
                        log+=f"\nClosed {kernel_id}'s associated websocket."
                    except:
                        log+=f"\Could not close {kernel_id}'s associated websocket. Perhaps it was already closed."

                session_to_kernel_ids = dict()
                kernel_id_to_ws = dict()
            else:
                log += "\nUnknown magic!"
                logger.warning("Unknown magic!")

            return Response(content=log, media_type="text/plain")
        ##
        kernel_id_actualized = await kernel_id_get(kernel_name=kernel_name, kernel_id=kernel_id, auth_username=auth_username, auth_password=auth_password)
        if session_full:
            session_to_kernel_ids[session_full] = kernel_id_actualized

        while True:
            ws = kernel_id_to_ws.get(kernel_id_actualized, None)
            if not ws:
                ws = await kernel_ws_get(kernel_id=kernel_id_actualized, auth_username=auth_username, auth_password=auth_password)
                if reuse_ws:
                    kernel_id_to_ws[kernel_id_actualized] = ws

            try:
                res = await kernel_ws_eval(ws, cmd,
                                           session=kernel_session,
                                           close_after=(not reuse_ws),
                                           isDbg=isDbg,
                                           logger=logger)
                break
            except WebSocketClosedError:
                logger.warning("Encountered WebSocketClosedError")
                kernel_id_to_ws[kernel_id_actualized] = None
            except:
                break

        if (json_output):
            return {
                "cmd": cmd,
                "session": session,
                "kernel_id": kernel_id_actualized,
                "kernel_name": kernel_name,
                "out": res.out,
                "err": res.err,
                "out_data": res.out_data,
                "err_data": res.err_data,
                "extra_data": res.extra_data,
                "retcode": res.retcode,
            }
        else:
            return Response(content=res.outerr, media_type="text/plain")

    except:
        logger.warning(f"eval failed:\n{traceback.format_exc()}")
