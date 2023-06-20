# Use an official Python runtime as a parent image
FROM apache/airflow:2.1.0-python3.8

# Set the working directory in the container to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir psycopg2-binary pandas openpyxl

# Change to the script dirdectory
WORKDIR /app/dags

# Copy the Python script
COPY Airflow2.py .


# Run the command to start Airflow webserver and scheduler in the backgroundi