from ensurepip import bootstrap
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.cluster import ClusterMetadata

admin_client = KafkaAdminClient(
    bootstrap_servers='localhost:9092',
    client_id="Kafka Admin"
)

topics_desc = admin_client.describe_topics(topics=['SalesTransactions'])#topics=['SalesTransactions']
print(topics_desc)