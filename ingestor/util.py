import asyncio

import mlzlog


class ReconnectionError(RuntimeError):
    """Failed to reconnect."""


def stop_everything(loop, exit_code, reason, code=1):
    mlzlog.log.info("stopping due to %s", reason)
    for task in asyncio.all_tasks(loop):
        task.cancel()
    exit_code[0] = code


def error_fallback(loop, context, exit_code):
    """Fallback exception handler."""
    exc = context.get("exception")
    if isinstance(exc, ReconnectionError):
        stop_everything(loop, exit_code, "failed reconnect")
        return
    msg = "Uncaught exception in Task"
    if (f := context.get("future")) is not None:
        funcname = f.get_coro().__qualname__
        msg += f' "{f.get_name()}" running function {funcname}()'
    mlzlog.log.error(msg, exc_info=exc)
    stop_everything(loop, exit_code, "uncaught exception")
