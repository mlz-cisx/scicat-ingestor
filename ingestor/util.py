import asyncio

import mlzlog


def stop_everything(loop, reason):
    mlzlog.log.info("stopping due to %s", reason)
    for task in asyncio.all_tasks(loop):
        task.cancel()


def error_fallback(loop, context):
    """Fallback exception handler."""
    msg = "Uncaught exception in Task"
    if (f := context.get("future")) is not None:
        funcname = f.get_coro().__qualname__
        msg += f' "{f.get_name()}" running function {funcname}()'
    mlzlog.log.error(msg, exc_info=context.get("exception"))
    stop_everything(loop, "uncaught exception")
