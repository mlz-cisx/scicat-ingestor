import json
from dataclasses import asdict, dataclass

import aio_pika as ap


@dataclass
class Message:
    dataset_id: str
    datablock_id: str
    filepath: str


class RmqClient:
    def __init__(self, config, log):
        self.config = config
        self.log = log
        self.enabled = True

    async def setup(self):
        if self.config.workqueue is None:
            self.log.info("no queue for file copies configured.")
            self.enabled = False
            return
        self.connection = await ap.connect_robust(
            host=self.config.host,
            port=self.config.port,
            virtualhost=self.config.vhost,
            login=self.config.user,
            password=self.config.pw,
        )
        self.channel = await self.connection.channel()
        self.channel.declare_queue(queue=self.config.workqueue, durable=True)

    async def queue_copies(self, event, dataset, files):
        if not self.enabled:
            return
        messages = [
            Message(
                dataset_id=dataset.pid,
                datablock_id=files[0],
                filepath=file[1],
            )
            for file in files
        ]
        for message in messages:
            await self.channel.default_exchange.publish(
                ap.Message(
                    body=json.dumps(asdict(message)).encode("utf-8"),
                    routing_key=self.config.workqueue,
                    delivery_mode=ap.DeliverMode.PERSISTENT
                ))

    async def close(self):
        await self.connection.close()
