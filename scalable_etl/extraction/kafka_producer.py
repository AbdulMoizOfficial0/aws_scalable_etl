from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstra_servers='localhost:9092')

csv_data_source = CSV