import os
from elasticsearch import Elasticsearch
import csv
import time
import logging
from dotenv import load_dotenv
import json

load_dotenv()

# Temel Loglama Ayarları
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Elasticsearch Bağlantı Bilgileri (Ortam değişkenlerinden alınacak)
ES_HOST = os.getenv('ES_HOST')
ES_USERNAME = os.getenv('ES_USERNAME')
ES_PASSWORD = os.getenv('ES_PASSWORD')
ES_CERT_PATH = os.getenv('ES_CERT_PATH')
ES_INDEX = # ES Index Name ex: 'notificationfinal*' 
ES_SCROLL_SIZE = 10000 # Scroll boyutu, ihtiyaca göre ayarlanabilir.

# CSV Dosyası Adı (Temel)
CSV_FILE_PREFIX = # File Prefix Name ex: 'notificationfinal'
CSV_FILE_EXTENSION = '.csv'

# Hedef Dosya Boyutu (MB)
TARGET_FILE_SIZE_MB = 100
TARGET_FILE_SIZE_BYTES = TARGET_FILE_SIZE_MB * 1024 * 1024

# TÜM OLASI ALAN ADLARI (ELASTICSEARCH MAPPING'DEN ALINAN)
ALL_POSSIBLE_FIELDNAMES = [
  # Your Column Names
]

def connect_elasticsearch():
    """Elasticsearch'e bağlanır."""
    try:
        es = Elasticsearch([ES_HOST],
                            http_auth=(ES_USERNAME, ES_PASSWORD),
                            use_ssl=True,
                            verify_certs=True,
                            port=9200,
                            ca_certs=ES_CERT_PATH,
                            timeout=120)
        if es.ping():
            logging.info("Elasticsearch bağlantısı başarılı.")
            return es
        else:
            logging.error("Elasticsearch bağlantısı başarısız!")
            return None
    except Exception as e:
        logging.error(f"Elasticsearch bağlantı hatası: {e}")
        return None

def flatten_document(doc):
    """Nested dokümanı düzleştirir."""
    flattened = {}
    for key, value in doc.items():
        # key is your array field ex:signals
        if key == 'signals':
            flattened[key] = value  
        elif isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flattened[f"{key}.{sub_key}"] = sub_value
        else:
            flattened[key] = value
    return flattened

def write_to_csv(data, fieldnames, file_number, current_file_size):
    """Verileri CSV dosyasına yazar ve dosya boyutunu kontrol eder."""
    filename = f"{CSV_FILE_PREFIX}-{file_number}{CSV_FILE_EXTENSION}"
    file_exists = os.path.isfile(filename)

    # Dosya boyutunu kontrol et
    if file_exists and current_file_size >= TARGET_FILE_SIZE_BYTES:
        file_number += 1
        filename = f"{CSV_FILE_PREFIX}-{file_number}{CSV_FILE_EXTENSION}"
        file_exists = False # Yeni dosya oluşturulacak, header yazılacak

    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            if not file_exists:
                writer.writeheader()

            writer.writerow(data)

        # Dosya boyutunu güncelle
        current_file_size = os.path.getsize(filename)
        return file_number, current_file_size #Dosya numarasını ve boyutunu geri döndürüyoruz

    except Exception as e:
        logging.error(f"CSV dosyasına yazma hatası: {e}")
        return file_number, current_file_size

def extract_data_and_write_to_csv(es, index):
    """Elasticsearch'ten verileri çeker ve CSV dosyalarına yazar."""
    try:
        # İlk scroll isteği
        scroll_response = es.search(index=index, scroll='2m', size=ES_SCROLL_SIZE, body={"query": {"match_all": {}}})
        sid = scroll_response['_scroll_id']
        total_hits = scroll_response['hits']['total']['value']
        logging.info(f"Toplam {total_hits} kayıt bulundu. Scroll ID: {sid}")

        # Değişkenler
        file_number = 1
        current_file_size = 0

        # İlk veri parçasını işle
        results = scroll_response['hits']['hits']
        if results:
            # Alan adlarını SABİT listeden alıyoruz
            fieldnames = ALL_POSSIBLE_FIELDNAMES
            # Düzleştirme işlemini burada yapıyoruz
            for hit in results: #tek tek yazıyoruz artık
                flattened_data = flatten_document(hit['_source'])
                file_number, current_file_size = write_to_csv(flattened_data, fieldnames, file_number, current_file_size)
        else:
            logging.warning("İlk scroll isteğinde sonuç bulunamadı.")
            return # Veri yoksa çık

        processed_count = len(results)
        start_time = time.time()

        # Scroll ile verileri çekmeye devam et
        while processed_count < total_hits:
            scroll_response = es.scroll(scroll_id=sid, scroll='1m')
            sid = scroll_response['_scroll_id']
            results = scroll_response['hits']['hits']

            if not results:
                logging.info("Scroll sona erdi.")
                break

            # Düzleştirme işlemini burada yapıyoruz
            for hit in results: #tek tek yazıyoruz artık
                flattened_data = flatten_document(hit['_source'])
                file_number, current_file_size = write_to_csv(flattened_data, fieldnames, file_number, current_file_size)
            processed_count += len(results)

            if processed_count % 10000 == 0: #Her 10000 kayıtta bir log yaz
                elapsed_time = time.time() - start_time
                logging.info(f"{processed_count}/{total_hits} kayıt işlendi. Geçen süre: {elapsed_time:.2f} saniye.")

        elapsed_time = time.time() - start_time
        logging.info(f"Toplam {processed_count} kayıt işlendi. Toplam geçen süre: {elapsed_time:.2f} saniye.")

        # Scroll'u temizle
        es.clear_scroll(scroll_id=sid)
        logging.info("Scroll temizlendi.")


    except Exception as e:
        logging.error(f"Veri çekme/yazma hatası: {e}")


if __name__ == '__main__':
    es = connect_elasticsearch()
    if es:
        extract_data_and_write_to_csv(es, ES_INDEX)
    else:
        logging.error("Elasticsearch bağlantısı kurulamadığı için işlem yapılamadı.")

    logging.info("İşlem tamamlandı.")
