from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from models import SearchReading, PurchaseReading, Base
import yaml
import logging
import logging.config

with open('/config/log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

with open('/config/app_conf.yml', 'r') as f:
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

# ENGINE = create_engine("mysql+pymysql://lillian:Password@localhost:3307/grocery")

Base.metadata.create_all(ENGINE)

