import time
import json
from confluent_kafka import Producer
from confluent_kafka import Consumer


class KafkaClient:
    def __init__(self) -> None:
        self.producer = Producer(self.read_ccloud_config("client.properties"))
        props = self.read_ccloud_config("client.properties")
        props["group.id"] = "dummy-consumer-6"
        props["auto.offset.reset"] = "earliest"
        self.consumer = Consumer(props)
        self.consumer.subscribe(["daily_stocks_data2"])

    def read_ccloud_config(self, config_file):
        conf = {}
        with open(config_file) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    conf[parameter] = value.strip()
        return conf


    def send_data(self, data):
        self.producer.produce("daily_stocks_data2", value=data)
        self.producer.flush()

    
    def retrieve_data(self):

        # try:
        msg = self.consumer.poll(3000.0)
        if msg is not None and msg.error() is None:
            return json.loads(msg.value().decode("utf-8"))
        else:
            return "No message to consume"
        # except KeyboardInterrupt:
        #     pass
        # finally:
        #     self.consumer.close()


if __name__ == "__main__":
    kafka_client = KafkaClient()
    # kafka_client.send(json.dumps({"key": "data"}))   
    kafka_client.retrieve_data()