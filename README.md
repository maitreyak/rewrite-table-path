# rewrite-table-path

# Build pyspark iceberg image 
cd ./py-spark-iceberg-image
docker build -t spark-iceberg-docker:6.0 .

# Run docker compose on the root dir. (Fill out aws creds in the docker-compose.yaml)
docker compose up

# Use the notebook RewriteTablePath.ipynb to validate RewriteTablePath iceberg proc.
