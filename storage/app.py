import connexion
from connexion import NoContent
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import SearchReading, PurchaseReading
import yaml
import logging
import logging.config
from sqlalchemy import select

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

db_conf = app_config['datastore']
user = db_conf['user']
password = db_conf['password']
hostname = db_conf['hostname']
port = db_conf['port']
db = db_conf['db']

ENGINE = create_engine(
    f'mysql+pymysql://{user}:{password}@{hostname}:{port}/{db}'
)

def make_session():
    return sessionmaker(bind=ENGINE)()


def add_search_readings(body):
    session = make_session()
    sr = SearchReading(
        trace_id=body["trace_id"],
        store_id=body["store_id"],
        store_name=body["store_name"],
        product_id=body["product_id"],
        search_count=body["search_count"],
        recorded_timestamp=body["recorded_timestamp"]
    )
    session.add(sr)
    session.commit()
    session.close()
    
    logger.debug(f'Stored event search_reading with trace id {body["trace_id"]}')
    
    return NoContent, 201


def add_purchase_readings(body):
    session = make_session()
    pr = PurchaseReading(
        trace_id=body["trace_id"],
        store_id=body["store_id"],
        store_name=body["store_name"],
        product_id=body["product_id"],
        purchase_count=body["purchase_count"],
        recorded_timestamp=body["recorded_timestamp"]
    )
    session.add(pr)
    session.commit()
    session.close()
    
    logger.debug(f'Stored event purchase_reading with trace id {body["trace_id"]}')

    return NoContent, 201


def get_search_readings(start_timestamp, end_timestamp):
    session = make_session()

    start = datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=None)
    end = datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=None)

    statement = select(SearchReading).where(
        SearchReading.date_created >= start
    ).where(
        SearchReading.date_created < end
    )

    results = [result.to_dict() for result in session.execute(statement).scalars().all()]
    session.close()

    logger.info(f"Found {len(results)} purchase readings between {start} and {end}")

    return results, 200

def get_purchase_readings(start_timestamp, end_timestamp):
    session = make_session()

    start = datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=None)
    end = datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=None)

    statement = select(PurchaseReading).where(
        PurchaseReading.date_created >= start
    ).where(
        PurchaseReading.date_created < end
    )

    results = [result.to_dict() for result in session.execute(statement).scalars().all()]
    session.close()

    logger.info(f"Found {len(results)} purchase readings between {start} and {end}")

    return results, 200

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("grocery_api.yml")

if __name__ == "__main__":
    app.run(port=8090)