from kafka import KafkaConsumer

consumer = KafkaConsumer('messages',
                         bootstrap_servers=['127.0.0.1:9092'],
                         auto_offset_reset='latest',
                         enable_auto_commit=False,
                         api_version=(0, 10, 2),
                         value_deserializer=lambda x: x.decode('utf-8')
                        )

for message in consumer:
    print("Received message: ", message.value)