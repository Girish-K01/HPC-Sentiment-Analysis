from kafka.admin import NewTopic,KafkaAdminClient

# Create a Kafka administration client
admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'],api_version=(0,11,5),value_serializer=lambda x: dumps(x).encode('utf-8'))

# Define the topic configuration
topic_name = "sentiment-analysis-topic"
topic_partitions = 1
topic_replication_factor = 1

# Create the topic
topic = NewTopic(name=topic_name, num_partitions=topic_partitions, replication_factor=topic_replication_factor)
admin_client.create_topics([topic])
print(f"Topic '{topic_name}' created.")