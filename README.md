# Elasticsearch to CSV Exporter

This project provides a script for extracting data from an Elasticsearch index and exporting it to CSV files. The script uses scroll API to fetch large amounts of data, flatten nested documents, and write the data into multiple CSV files based on a configurable size limit.

## Features

- **Elasticsearch Data Extraction**: Uses Elasticsearch's Scroll API to extract data from a specified index.
- **Data Flattening**: Handles nested fields and arrays in Elasticsearch documents and flattens them into simple key-value pairs.
- **CSV Export**: Writes the extracted and flattened data to CSV files, ensuring that no file exceeds a configurable size limit (in MB).
- **Logging**: Logs important events and errors, providing insight into the progress and performance of the extraction process.

## Requirements

- Python 3.x
- Elasticsearch Python Client
- `.env` file with connection details for Elasticsearch

## Installation

1. Clone the repository to your local machine:
    ```bash
    git clone https://github.com/yourusername/GetDatasFromElastic.git
    cd GetDatasFromElastic
    ```

2. Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Create a `.env` file with the following environment variables for Elasticsearch connection:
    ```ini
    ES_HOST=<your_elasticsearch_host>
    ES_USERNAME=<your_elasticsearch_username>
    ES_PASSWORD=<your_elasticsearch_password>
    ES_CERT_PATH=<path_to_your_cert_file>
    ```

## Usage

### Running the Script

After setting up the `.env` file, you can run the script using:

```bash
python export_to_csv.py
