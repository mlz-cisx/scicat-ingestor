
import mlzlog
import rstream

from .message import testmsg


class Backend:
    async def on_message(msg_raw, message_context):
        raise NotImplementedError()

    async def close(self):
        pass


class RabbitConsumer:
    def __init__(self, cfg, backend, log):
        self.cfg = cfg
        self.log = log
        self.backend = backend
        self.consumer = None

    async def on_message(self, msg_raw, message_context):
        await self.backend.on_message(msg_raw, message_context)
        # await self.store_offset()  # store after every message right now

    async def get_offset(self):
        try:
            return await self.consumer.query_offset(
                stream=self.cfg.stream, subscriber_name="scicat-ingestor"
            )
        except rstream.OffsetNotFound:
            return 1
        except rstream.StreamDoesNotExist as e:
            raise ValueError("invalid stream name") from e

    async def run(self):
        for msg in testmsg():
            await self.on_message(msg, None)
            break
        return
        try:
            pass
            r = self.cfg
            self.consumer = rstream.Consumer(
                host=r.host,
                port=r.port,
                vhost=r.vhost,
                username=r.user,
                password=r.pw,
                # on_close_handler=,
                load_balancer_mode=True,  # TODO: does not work without, what it do?
            )
            try:
                await self.consumer.start()
            except Exception as e:
                self.log.error("Connecting to RabbitMQ failed: %r", e)
            # offset = await self.get_offset()
            await self.consumer.subscribe(
                stream=r.stream,
                subscriber_name="scicat-ingestor",  # TODO: from cfg or static?
                callback=self.on_message,
                decoder=rstream.amqp_decoder,
                offset_specification=rstream.ConsumerOffsetSpecification(
                    # rstream.OffsetType.OFFSET, offset
                    rstream.OffsetType.NEXT, None
                ),
            )
            await self.consumer.run()
        finally:
            if self.consumer:
                await self.consumer.close()
            await self.backend.close()
