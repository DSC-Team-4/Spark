# Base image
FROM bitnami/spark:latest

# Install requests library
RUN apt-get update && apt-get install -y python3-pip && pip3 install requests

# Clean up
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Set the entrypoint
# ENTRYPOINT ["/entrypoint.sh"]
# CMD ["spark-shell"]