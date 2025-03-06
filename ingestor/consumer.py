
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

    async def run(self):
        for msg in testmsg():
            await self.on_message(msg, None)
        try:
            pass
            # r = self.cfg.rmq
            # print(r.host, r.port, r.vhost, r.user, r.pw)
            # self.consumer = rstream.Consumer(
            #     host=r.host,
            #     port=r.port,
            #     vhost=r.vhost,
            #     username=r.user,
            #     password=r.pw,
            #     # on_close_handler=,
            #     load_balancer_mode=True,  # TODO: does not work without, what it do?
            # )
            # try:
            #     await self.consumer.start()
            # except Exception as e:
            #     self.log.error("Connecting to RabbitMQ failed: %r", e)
            # await self.consumer.subscribe(
            #     stream=r.stream,
            #     subscriber_name="scicat-ingestor",  # TODO: from cfg or static?
            #     callback=self.on_message,
            #     decoder=rstream.amqp_decoder,
            #     offset_specification=rstream.ConsumerOffsetSpecification(rstream.OffsetType.NEXT, None),  # TODO: offset tracking
            # )
            # await self.consumer.run()
        finally:
            if self.consumer:
                await self.consumer.close()
            await self.backend.close()
