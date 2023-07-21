from fastapi import FastAPI, HTTPException
from confluent_kafka import Consumer, KafkaException
import socket
import json
import datetime
import uvicorn
from pydantic import BaseModel
import threading
from dotenv import load_dotenv
import os
import signal
import sys


def signal_handler(sig, frame):
    print("Ctrl+C pressed. Exiting...")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
# import time

load_dotenv()
start_udp = int(os.getenv('START_UDP'))
server_ip = os.getenv('SERVER_IP')

udp_ports = [start_udp + i for i in range(6)]


udp_port_mapper = {
    'PCM01': udp_ports[0],
    'PCM02': udp_ports[1],
    'PCM03': udp_ports[2],
    'PCM04': udp_ports[3],
    'PCM05': udp_ports[4],
    'PCM06': udp_ports[5]
}

pcmlist = ['PCM01', 'PCM02', 'PCM03', 'PCM04', 'PCM05', 'PCM06']

lock = threading.Lock()


class TelemetrySender:
    def __init__(self, teleplot_addr, topic):
        self.teleplot_addr = teleplot_addr
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.topic = topic

    def __del__(self):
        self.udp_socket.close()

    @staticmethod
    def time_to_milliseconds(input_time):
        layout = "%d/%m/%Y %H:%M:%S.%f"  # "%d/%m/%Y %H:%M:%S.%f" "%Y-%m-%d %H:%M:%S.%f"
        try:
            t = datetime.datetime.strptime(input_time, layout)
        except ValueError as err:
            raise ValueError("time_to_milliseconds Error: " + str(err))
        return int(t.timestamp() * 1000)

    def send_telemetry(self, param, value, time_id):
        timestamp = self.time_to_milliseconds(time_id)
        msg = f'{self.topic}?{param}:{timestamp}:{value}|g'
        self.udp_socket.sendto(msg.encode(), self.teleplot_addr)

    def close(self):
        self.udp_socket.close()


class KafkaConsumer:
    def __init__(self, bootstrap_servers, topic, telemetry_sender, selected_params):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = None
        self.telemetry_sender = telemetry_sender
        self.selected_params = selected_params
        self.stop_flag = False

    def initialize(self):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'consumer_' + str(datetime.datetime.now().timestamp()),
            'auto.offset.reset': 'earliest'  # 'latest'
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([self.topic])

    def poll_messages(self):
        try:
            while not self.stop_flag:
                msg = self.consumer.poll(100.0)

                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                key = msg.key().decode('utf-8')
                if key not in self.selected_params:
                    continue

                value = msg.value().decode('utf-8')
                value = json.loads(value)
                self.telemetry_sender.send_telemetry(
                    key, value["Processed"], value["Time_id"])
        except Exception as e:
            print(f'KafkaError: {str(e)}')

    def close(self):
        if not self.stop_flag:
            self.stop_flag = True
        if self.consumer:
            self.consumer.close()
            self.consumer = None


class ConsumerController:
    def __init__(self):
        self.consumers = []
        self.consumers_lock = threading.Lock()

    def start_consumer(self, bootstrap_servers: str, topic: str, udp_port: int, selected_params: set):
        try:
            telemetry_sender = TelemetrySender(
                (server_ip, udp_port), topic)
            consumer = KafkaConsumer(
                bootstrap_servers, topic, telemetry_sender, selected_params)
            consumer.initialize()
            with self.consumers_lock:
                self.consumers.append(consumer)
            consumer.poll_messages()

        except KafkaException as e:
            print(f'KafkaError: {str(e)}')
            with self.consumers_lock:
                for consumer in self.consumers:
                    consumer.close()

    def stop_consumer(self):
        with self.consumers_lock:
            for consumer in self.consumers:
                consumer.close()
            self.consumers = []


class PlotRequest(BaseModel):
    bootstrap_servers: str
    topic: str
    parameters: list


app = FastAPI()
consumer_controller = ConsumerController()


@app.get("/ping")
def ping():
    return {"message": "pong"}


@app.post("/plots")
def receive_plots(plot_request: PlotRequest):
    try:
        with consumer_controller.consumers_lock:
            for consumer in consumer_controller.consumers:
                if consumer.topic == plot_request.topic:
                    raise HTTPException(
                        status_code=400, detail=f'Consumer for {plot_request.topic} already exists.')
        global pcmlist
        for pcm in pcmlist:
            if pcm in plot_request.topic:
                pcm_id = pcm
                break

        thread = threading.Thread(target=consumer_controller.start_consumer, args=(
            plot_request.bootstrap_servers, plot_request.topic, udp_port_mapper[pcm_id], set(plot_request.parameters)))
        thread.start()
        return {"message": f'Consumer for {plot_request.topic} is starting shortly.'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cleanup")
def stop_consumer_endpoint():
    consumer_controller.stop_consumer()
    return {"message": "All the kafka consumers stopped successfully."}


@app.get("/stop_topic")
def stop_topic(topic: str):
    with consumer_controller.consumers_lock:
        for consumer in consumer_controller.consumers.copy():
            if consumer.topic == topic:
                consumer.close()
                consumer_controller.consumers.remove(consumer)
                return {"message": f'Consumer for {topic} stopped successfully.'}
    return {"message": f'Consumer for {topic} not found.'}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
