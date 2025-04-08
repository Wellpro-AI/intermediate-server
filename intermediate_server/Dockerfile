# Use the official lightweight Python image
FROM python:3.11

# Set the working directory inside the container
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application files
COPY . .

# Expose the required port
EXPOSE 8080

# Start the FastAPI application using Uvicorn
CMD ["uvicorn", "intermediate:app", "--host", "0.0.0.0", "--port", "8080"]
