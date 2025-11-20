import connexion
from connexion import NoContent
import yaml
import logging
import logging.config
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import json
import os
import httpx

with open('/config/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('/config/log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())

logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


def get_stats():
    logger.info("Request for statistics received")
    
    if not os.path.exists(app_config['datastore']['filename']):
        logger.error("Statistics do not exist")
        return {"message": "Statistics do not exist"}, 404
    
    with open(app_config['datastore']['filename'], 'r') as f:
        stats = json.load(f)
    
    logger.debug(f"Statistics: {stats}")
    logger.info("Request completed")
    
    return stats, 200


def populate_stats():
    logger.info("Periodic processing started")
    
    if os.path.exists(app_config['datastore']['filename']):
        with open(app_config['datastore']['filename'], 'r') as f:
            stats = json.load(f)
        last_updated = stats['last_updated']
    else:
        stats = {
            'num_search_readings': 0,
            'max_search_count': 0,
            'num_purchase_readings': 0,
            'max_purchase_count': 0,
            'last_updated': '2000-01-01T00:00:00.000Z'
        }
        last_updated = stats['last_updated']
    
    current_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]
    
    search_url = app_config['eventstores']['search']['url']
    logger.info(f"Search URL: {search_url}")
    search_response = httpx.get(
        search_url,
        params={'start_timestamp': last_updated, 'end_timestamp': current_timestamp}
    )

    
    if search_response.status_code == 200:
        search_readings = search_response.json()
        logger.info(f"Received {len(search_readings)} search readings")
        
        stats['num_search_readings'] += len(search_readings)
        for reading in search_readings:
            if reading['search_count'] > stats['max_search_count']:
                stats['max_search_count'] = reading['search_count']
    else:
        logger.error(f"Failed to get search readings. Status: {search_response.status_code}")
    
    purchase_url = app_config['eventstores']['purchase']['url']
    purchase_response = httpx.get(
        purchase_url,
        params={'start_timestamp': last_updated, 'end_timestamp': current_timestamp}
    )
    
    if purchase_response.status_code == 200:
        purchase_readings = purchase_response.json()
        logger.info(f"Received {len(purchase_readings)} purchase readings")
        
        stats['num_purchase_readings'] += len(purchase_readings)
        for reading in purchase_readings:
            if reading['purchase_count'] > stats['max_purchase_count']:
                stats['max_purchase_count'] = reading['purchase_count']
    else:
        logger.error(f"Failed to get purchase readings. Status: {purchase_response.status_code}")
    
    stats['last_updated'] = current_timestamp
    
    with open(app_config['datastore']['filename'], 'w') as f:
        json.dump(stats, f, indent=2)
    
    logger.debug(f"Updated statistics: {stats}")
    logger.info("Periodic processing ended")


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(
        populate_stats,
        'interval',
        seconds=app_config['scheduler']['interval']
    )
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("/config/grocery_api.yml")

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, host="0.0.0.0")