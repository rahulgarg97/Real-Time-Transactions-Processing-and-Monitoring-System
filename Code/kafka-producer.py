from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random
import csv
import json

KAFKA_TOPIC_NAME_CONS = "transactionrequest"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    with open('transactiondata.csv', newline='') as csvfile:
      csvreader = csv.reader(csvfile, delimiter=' ')
      next(csvreader)
      for row in csvreader:
        print(row)
        transaction = {}
        transaction['transaction_id'] = row[0]
        transaction['user_id'] = row[1]
        transaction['card_number'] = row[2]
        transaction['amount'] = row[3]
        transaction['description'] = row[4]
        transaction['transaction_type'] = row[5]
        transaction['vendor'] = row[6]
        
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, transaction)
        time.sleep(1)