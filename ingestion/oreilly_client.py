import os
import time
import uuid
import json
import tempfile
import requests
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from utils.logger import get_logger
from utils.snowflake import get_snowflake_connection
from utils.config import settings

logger = get_logger(__name__)
base_url = 'https://learning.oreilly.com/api/v2/search/'

# class OReillyCLient:
#     __init__          base url, limit, session
#     get_watermark     reads from META.PIPELINE_WATERMARKS
#     fetch_page        single API call, returns raw response
#     extract           pagination loop, handles stop conditions
#     load              inserts collected records into Bronze
#     update_watermark  writes new max(date_added) to META
#     run               orchestrates all of the above in order

class OReillyClient:
    def __init__(self):
        self.limit = 200
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=3)
        session.mount("https://", adapter)
        self.session = session
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        self.session.cookies.set("orm-jwt", settings.oreilly_jwt_token)

        
    def get_watermark(self):
        try:
            con = get_snowflake_connection()
            last_page = con.cursor().execute(
                "SELECT last_page " \
                "FROM META.PIPELINE_WATERMARKS " \
                "WHERE pipeline_name = 'oreilly_search'").fetchone()
        finally:
            con.close()

        if last_page is None:
            return None
        return last_page[0]

    def fetch_page(self, page:int):
        params = {
            "limit": self.limit,
            "formats": "book",
            "page": page,
            "sort": "date_added",
            "order": "asc"
        }

        try:
            response = self.session.get(base_url, params=params)
            response.raise_for_status()
            logger.info(f"Response succesful with status code: {response.status_code}")
        
            data = response.json()

            if not data:
                logger.info(f"Request returned empty dict.")
                return {}
            
            if "detail" in data:
               logger.warning(f"Maximum number of request reached.")
               return {}
            
            return data

        except requests.exceptions.HTTPError as e:
            logger.error(f"Error with status code {response.status_code}")
            raise

        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error {e}")
            raise

        except requests.exceptions.Timeout as e:
            logger.error(f"Request timed out {e}")
            raise

        except requests.exceptions.RequestException as e:
            logger.error(f"General error {e}")
            raise

    def extract(self):
        records = []
        request_count = 0
        last_page  = self.get_watermark()
        current_page = last_page + 1 if last_page is not None else 0
        
        while True:
            if request_count == 5:
                logger.info("Cap reached")
                break

            if request_count > 0:
                time.sleep(60)

            request_count += 1
            logger.info(f"Fetching page number: {current_page}")
            response = self.fetch_page(current_page)

            if not response:
                logger.info("Empty response, stopping")
                break

            results = response.get("results", [])

            if not results:
                break

            records.extend(results)

            if not response.get("next"):
                logger.info("No more pages available")
                break

            current_page += 1

        return records, current_page

    def load(self, records:list):
        if not records:
            logger.info("No new records to add")
            return

        rows = []
        con = None
        ingested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        batch_id = str(uuid.uuid4())
        record_count = len(records)
        source = "oreilly_search"
        
        for record in records:
            raw_payload = json.dumps(record)
            watermark = record["date_added"]
            rows.append({"raw_payload":raw_payload,
                        "ingested_at": ingested_at,
                        "watermark": watermark,
                        "source": source,
                        "batch_id": batch_id,
                        "record_count": record_count
                        })

        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as fp:
                for row in rows:
                    fp.write(json.dumps(row) + "\n")

                tmp_path = fp.name
                
                con = get_snowflake_connection()
                con.cursor().execute("USE SCHEMA OREILLY_DB.BRONZE")
                con.cursor().execute(f"PUT file://{tmp_path} @OREILLY_DB.BRONZE.RAW_STAGE AUTO_COMPRESS=TRUE")
                con.cursor().execute("""
                                    COPY INTO BRONZE.RAW_SEARCH_RESULTS (raw_payload, ingested_at, watermark, source, batch_id, record_count)
                                    FROM (
                                    SELECT PARSE_JSON($1:raw_payload),
                                        $1:ingested_at::TIMESTAMP,
                                        $1:watermark::TIMESTAMP,
                                        $1:source::VARCHAR,
                                        $1:batch_id::VARCHAR,
                                        $1:record_count::INT
                                    FROM @OREILLY_DB.BRONZE.RAW_STAGE
                                    )
                                    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
                                    ON_ERROR = 'CONTINUE'
                                    PURGE = TRUE
                                    """)
            logger.info(f"Total of record loaded into Bronze layer: {record_count} with source: {source}")

        finally:
            os.remove(tmp_path)
            if con:
                con.close()

    def update_watermark(self, watermark):
        try:
            con = get_snowflake_connection()
            con.cursor().execute("MERGE INTO META.PIPELINE_WATERMARKS AS target " \
                "USING (SELECT 'oreilly_search' AS pipeline_name, %s AS last_page, %s AS updated_at) AS source " \
                "ON target.pipeline_name = source.pipeline_name " \
                "WHEN MATCHED THEN UPDATE SET " \
                "last_page = source.last_page, " \
                "updated_at = source.updated_at " \
                "WHEN NOT MATCHED THEN INSERT "
                "(pipeline_name, last_page, updated_at) " \
                "VALUES (source.pipeline_name, source.last_page, source.updated_at)", (watermark, datetime.now())
            )
            logger.info("Watermark on META updated successfully")
        
        except Exception as e:
            logger.error(f"Failed to update watermark: {e}")
            raise
        
        finally:
            con.close()

    def run(self):
        try:
            records, page = self.extract()
            if not records:
                logger.info("No new records, skipping load and watermark update")
                return

            new_watermark = page
            self.load(records)
            self.update_watermark(new_watermark)
            logger.info("Pipeline run completed successfully")

        except Exception as e:
            logger.error(f"Pipeline run failed: {e}")
            raise