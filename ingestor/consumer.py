import asyncio
import random
from datetime import UTC, datetime

import rstream
from rstream.recovery import RecoveryStrategy

from .util import ReconnectionError, stop_everything


class BackoffStrategy(RecoveryStrategy):
    """Backoff and try to reconnect.

    Modified BackOffRecoveryStrategy that stops ingestor execution when max_tries is reached
    instead of just giving up.
    """

    def __init__(self, log, max_retries=10, max_wait=50, jitter=2):
        super().__init__(enable=True, max_retries=max_retries)
        self.log = log
        self.max_wait = max_wait
        self.jitter = jitter

    async def recover(self, entity, stream, error, attempt, recovery_fun):
        if attempt > self._max_retries:
            self.log.info("Reconnecting failed, giving up")
            raise ReconnectionError("unsucessful reconnect")
        self.log.debug("Trying to reconnect to stream: %s. Attempt:%s, reason: %s",
                       stream, attempt, error.__class__.__name__)
        # calculate the backoff time
        backoff_time = min(self.max_wait, 2 ** (attempt - 1))

        # add safe jitter with some randomization
        delay = random.uniform(1, self.jitter)  # noqa: S311
        backoff_time = backoff_time + delay
        self.log.info("Reconnection attempt %d in %.1fs.", attempt, backoff_time)
        await asyncio.sleep(backoff_time)
        try:
            # check if the stream still exists
            stream_exist = await entity.stream_exists(stream)
            if stream_exist:
                self.log.debug("Attempting to reconnect to stream %s, attempt %d",
                               stream, attempt)
                await entity.clean_list(stream)
                await recovery_fun()
                self.log.info("Reconnected to %s successfully on attempt %d.",
                              stream, attempt)
                return
            self.log.info("Stream: %s does not exist. Giving up. Attempt:%d, reason:%s",
                          stream, attempt, error.__class__.__name__)
            stop_everything(asyncio.get_event_loop(), "missing stream")
        except Exception as e:
            self.log.error("Error reconnecting to %s (attempt %d): %r",  # noqa: TRY400
                           stream, attempt, e)
            # retry again
            await self.recover(entity, stream, e, attempt + 1, recovery_fun)


class Backend:
    async def setup(self):
        """Place setup code here.

        Called before the first message.
        """

    async def on_message(self, msg_raw, message_context):
        raise NotImplementedError

    async def close(self):
        """Close any resources that are needed."""


SUBNAME = "scicat-ingestor"


class RabbitConsumer:
    def __init__(self, cfg, backend, log):
        self.cfg = cfg
        self.log = log
        self.backend = backend
        self.consumer = None
        self.dlx_producer = None

    async def on_message(self, msg_raw, message_context):
        try:
            await self.backend.on_message(msg_raw, message_context)

        except Exception as e:
            try:
                self.log.exception(
                    "Error processing message, moving message to DLX")
                await self.publish_to_dls(msg_raw, message_context, e)
            except Exception as e:
                self.log.exception(
                    "Failed to process AND failed to DLX. Message will NOT be acknowledged!")
                raise e

        await self.consumer.store_offset(
            stream=self.cfg.stream,
            subscriber_name=SUBNAME,
            offset=message_context.offset,
        )  # store after every message right now

    async def get_offset(self):
        try:
            last_offset = await self.consumer.query_offset(
                stream=self.cfg.stream, subscriber_name=SUBNAME,
            )
            return last_offset + 1
        except rstream.OffsetNotFound:
            return 0
        except rstream.StreamDoesNotExist as e:
            raise ValueError(f"stream {self.cfg.stream} not found or invalid") from e

    async def on_connection_closed(self, disconnection_info: rstream.OnClosedErrorInfo) -> None:
        self.log.warning(
            "connection has been closed from stream: %s for reason: %s",
            disconnection_info.streams,
            disconnection_info.reason,
        )

    async def run(self):
        # for msg in testmsg():
        #     await self.on_message(msg, None)
        #     break
        try:
            r = self.cfg
            self.log.debug("Creating consumer on %s:%d, (VHost: %r) with user %s)",
                           r.host, r.port, r.vhost, r.user)
            self.consumer = rstream.Consumer(
                host=r.host,
                port=r.port,
                vhost=r.vhost,
                username=r.user,
                password=r.pw,
                on_close_handler=self.on_connection_closed,
                recovery_strategy=BackoffStrategy(self.log),
                load_balancer_mode=True,  # TODO: does not work without, what it do?
            )

            self.dlx_producer = rstream.Producer(
                host=r.host,
                port=r.port,
                vhost=r.vhost,
                username=r.user,
                password=r.pw,
            )
            try:
                await self.consumer.start()
                await self.dlx_producer.start()
            except OSError:
                self.log.exception("Connecting to RabbitMQ failed")
                return
            try:
                await self.backend.setup()
            except Exception:
                self.log.exception("Error in setup of backend")
                return
            try:
                offset = await self.get_offset()
            except OSError:
                self.log.exception("unable to get stream offset")
                return
            self.log.info(
                "consumer started, subscribing to %s as %s", r.stream, SUBNAME)
            await self.consumer.subscribe(
                stream=r.stream,
                subscriber_name=SUBNAME,  # TODO: from cfg or static?
                callback=self.on_message,
                decoder=rstream.amqp_decoder,
                offset_specification=rstream.ConsumerOffsetSpecification(
                    rstream.OffsetType.OFFSET, offset,
                    # rstream.OffsetType.NEXT, None
                ),
            )
            await self.consumer.run()
        except Exception:
            self.log.exception("Error during startup")
        finally:
            self.log.info("shutting down")
            if self.consumer:
                await self.consumer.close()
            if self.dlx_producer:
                await self.dlx_producer.close()
            await self.backend.close()

    async def publish_to_dls(self, msg_raw, message_context, error):
        """Publish failed message to dead-letter stream."""
        if self.dlx_producer is None:
            self.log.error(
                "Producer not initialized, cannot publish to dead letter stream")
            raise RuntimeError(
                "Producer not initialized, cannot publish to dead letter stream")
        try:
            message = rstream.AMQPMessage(
                body=msg_raw.body,
                application_properties={
                    "x-error": str(error),
                    "x-failed-at": datetime.now(tz=UTC).isoformat(),
                    "x-original-offset": message_context.offset,
                },
            )
            await self.dlx_producer.send(
                stream=self.cfg.dlx_stream,
                message=message,
            )
            self.log.info("Published message to DLX stream")
        except Exception as e:
            self.log.exception("Failed to publish message to DLX stream")
            raise e
