# Use an official Python runtime as the base image
FROM python:3.12.3

# Install Firefox and Geckodriver for Selenium
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl firefox-esr \
        fonts-dejavu \
        fonts-liberation \
        fonts-freefont-ttf \
        libdbus-glib-1-2 \
        libasound2 \
        libx11-xcb1 \
        libxt6 \
        libxrender1 \
        libxrandr2 \
        libgbm1 \
        libnss3 \
        libxss1 \
    && rm -fr /var/lib/apt/lists/* \
    && curl -L https://github.com/mozilla/geckodriver/releases/download/v0.36.0/geckodriver-v0.36.0-linux64.tar.gz | tar xz -C /usr/local/bin \
    && apt-get purge -y ca-certificates curl

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file to the working directory
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the container
COPY . .

# Define the entrypoint and default command
ENTRYPOINT ["python3"]
CMD ["extract_from_X.py"]