import asyncio

import mlzlog
import rstream

from .client import SciCatClient
from .message import Message, testmsg, parse_message


class Ingestor:
    def __init__(self, cfg, log):
        self.cfg = cfg
        self.client = SciCatClient(cfg.sc, mlzlog.getLogger("client"))
        self.log = log
        self.consumer = None

    async def on_message(self, msg_raw, message_context):
        await self.client.login(self.client.config.user, self.client.config.pw)  # TODO in client

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
            await self.client.close()
