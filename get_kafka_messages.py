from kafka import KafkaConsumer
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

listOfTopics = ['ext_device_10121']

# Initialize Kafka consumer
try:
    consumer = KafkaConsumer(bootstrap_servers='zonos.engrid.in:9092',auto_offset_reset='earliest')
    consumer.subscribe(listOfTopics)
except Exception as e:
    logger.error(f"Error connecting to Kafka: {e}")
    exit(1)

# Consume messages
for msg in consumer:
    try:
        print("++++++++")
        print(f"Topic: {msg.topic}")
        print("++++++++")
        print(f"Key: {msg.key.decode('utf-8')}")
        print("--------")
        print(f"Value: {msg.value.decode('utf-8')}")
        print("--------")
        
        # Add your custom message processing logic here
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
