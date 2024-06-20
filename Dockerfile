# Base image
FROM bitnami/spark:latest

# Switch to root user to install packages
USER root

# Install required packages
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install requests pytz

# Clean up
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the non-root user
USER 1001
