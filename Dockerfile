# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip
RUN pip install PyYAML
RUN pip install -e .

# Copy the rest of the application code into the container at /app
#COPY . .

# Expose the port your application listens on (if it's a web app)
#EXPOSE 8000

# Define the command to run your application
CMD ["/bin/bash"]
