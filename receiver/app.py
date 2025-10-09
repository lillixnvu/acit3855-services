import connexion
from connexion import NoContent
from datetime import datetime
import httpx
import time
import yaml
import logging
import logging.config

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

def report_search_readings(body):
    search_report = body["search_readings"]
    for search in search_report:
        trace_id = time.time_ns()
        
        logger.info(f'Received event search_reading with trace id {trace_id}')
        
        data = {
            "trace_id": trace_id,
            "store_id": body["store_id"],
            "store_name": body["store_name"],
            "reporting_timestamp": body["reporting_timestamp"],
            "product_id": search["product_id"],
            "search_count": search["search_count"],
            "recorded_timestamp": search["recorded_timestamp"]
        }

        url = app_config['eventstore1']['url']
        response = httpx.post(url, json=data)
        
        logger.info(f'Response for event search_reading (id: {trace_id}) has status {response.status_code}')
        
    return NoContent, 201


def report_sold_readings(body):
    purchase_report = body["purchase_readings"]
    for purchase in purchase_report:
        trace_id = time.time_ns()
        
        logger.info(f'Received event purchase_reading with trace id {trace_id}')
        
        data = {
            "trace_id": trace_id,
            "store_id": body["store_id"],
            "store_name": body["store_name"],
            "reporting_timestamp": body["reporting_timestamp"],
            "product_id": purchase["product_id"],
            "purchase_count": purchase["purchase_count"],
            "recorded_timestamp": purchase["recorded_timestamp"]
        }
        
        url = app_config['eventstore2']['url']
        response = httpx.post(url, json=data)
        
        logger.info(f'Response for event purchase_reading (id: {trace_id}) has status {response.status_code}')
        
    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("grocery_api.yml")

if __name__ == "__main__":
    app.run(port=8080)