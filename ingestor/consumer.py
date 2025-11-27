import asyncio
import sys
from datetime import UTC, datetime

import rstream


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
            self.log.exception("Error processing message")
            await self.publish_to_dls(msg_raw, message_context, e)

        finally:
            await self.consumer.store_offset(
                stream=self.cfg.stream,
                subscriber_name=SUBNAME,
                offset=message_context.offset,
            )  # store after every message right now

    async def get_offset(self):
        try:
            return await self.consumer.query_offset(
                stream=self.cfg.stream, subscriber_name="scicat-ingestor",
            )
        except rstream.OffsetNotFound:
            return 1
        except rstream.StreamDoesNotExist as e:
            raise ValueError("invalid stream name") from e

    async def on_connection_closed(self, disconnection_info: rstream.OnClosedErrorInfo) -> None:
        self.log.warning(
            "connection has been closed from stream: %s for reason: %s",
            disconnection_info.streams,
            disconnection_info.reason,
        )
        await asyncio.sleep(2)
        for stream in disconnection_info.streams:
            backoff = 2
            count = 0
            while True:
                try:
                    self.log.debug("trying to reconnect to stream: %s", stream)
                    if self.consumer is not None:
                        await self.consumer.reconnect_stream(stream)
                    break
                except Exception as ex:
                    if count > 10:
                        # TODO: or try infinitely often?
                        self.log.exception("reconnection failed, giving up.")
                        sys.exit(-1)
                    backoff = min(backoff * 2, 60)
                    count += 1
                    self.log.warning(
                        "Reconnecting failed: %r (retrying in %ds)", ex, backoff)
                    await asyncio.sleep(backoff)

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
            except Exception:
                self.log.exception("Connecting to RabbitMQ failed")
            try:
                await self.backend.setup()
            except Exception:
                self.log.exception("Error in setup of backend")
            offset = await self.get_offset()
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
            return
        try:
            message = rstream.AMQPMessage(
                body=msg_raw,
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
        except Exception:
            self.log.exception("Failed to publish message to DLX stream")
