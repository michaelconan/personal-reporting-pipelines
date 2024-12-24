# Slim base airflow image with pegged Python version
FROM apache/airflow:slim-2.10.4-python3.12

# Copy local files and folders
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run scheduler and webserver on same service,
# use bash to allow multiple commands with airflow entrypoint script
# CMD 'bash -c "airflow scheduler & airflow webserver"'