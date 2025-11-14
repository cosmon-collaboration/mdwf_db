# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Clone the public Git repository
RUN git clone https://github.com/cosmon-collaboration/mdwf_db.git
# Copy the requirements file into the container at /app
#COPY .. .

WORKDIR /app/mdwf_db
# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip
RUN pip install PyYAML
RUN pip install -e .

# Now set the working directory to be the data base, one can mount exteral volume here
WORKDIR /home/mdwf_db
# Copy the rest of the application code into the container at /app
#COPY . .

# Expose the port your application listens on (if it's a web app)
#EXPOSE 8000

# Define the command to run your application
CMD ["/bin/bash"]
