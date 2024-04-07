from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CassandraConnectionTest") \
    .config("spark.cassandra.connection.host", "cass") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

# Test connectivity by reading from a Cassandra table
df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="emp", keyspace="reporting") \
    .load()

# Show the DataFrame
df.show()

# Stop the SparkSession
spark.stop()