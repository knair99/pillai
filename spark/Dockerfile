# Dockerfile for Spark master or worker

# Base image for Spark (adjust this according to the base image you're using)
FROM bitnami/spark:3.1.2

# Install Python and pip if not already installed
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Install psycopg2-binary
RUN pip3 install psycopg2-binary

# (Optional) Install any other dependencies your Spark job might need
# RUN pip3 install <other_dependencies>

# Set environment variables if needed (e.g., SPARK_HOME, JAVA_HOME, etc.)

# Copy any necessary scripts or files
# COPY your-script.py /path/in/container/

# Set the entrypoint or command if different from the default
# CMD ["/entrypoint.sh"]