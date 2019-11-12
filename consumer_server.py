from pykafka import KafkaClient
from pykafka.simpleconsumer import OffsetType
import logging

# Get logs from the pykafka.broker
logging.getLogger("pykafka.broker").setLevel('ERROR')

client = KafkaClient(hosts="localhost:9092")
topic = client.topics[b'service-calls']

# Create a Consumer
consumers = topic.get_balanced_consumer(
    consumer_group=b'sf-kafka',
    auto_commit_enable=False,
    auto_offset_reset=OffsetType.EARLIEST,
    zookeeper_connect='localhost:2181'
)

# Print Messages
for m in consumers:
    if m is not None:
        print(m.offset, m.value)