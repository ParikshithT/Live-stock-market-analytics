# Use the official Python image
FROM python:3.9-bullseye

# Set the working directory
WORKDIR /app

# Install Java and other dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# Verify that Java is installed
RUN java -version

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable
ENV NAME World

# Run the application
# CMD ["airflow", "webserver"]
CMD ["python", "./scripts/run_all.py"]
