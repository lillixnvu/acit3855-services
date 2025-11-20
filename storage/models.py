from sqlalchemy import String, Integer, DateTime
from sqlalchemy.sql import func
from sqlalchemy.orm import DeclarativeBase, mapped_column
import uuid

 
class Base(DeclarativeBase):
    pass

class SearchReading(Base):
    __tablename__ = "search_reading"

    id = mapped_column(Integer, primary_key=True)
    trace_id = mapped_column(String(100), nullable=False)  
    store_id = mapped_column(String(100), nullable=False)
    store_name = mapped_column(String(250), nullable=False)
    product_id = mapped_column(String(100), nullable=False)
    search_count = mapped_column(Integer, nullable=False)
    recorded_timestamp = mapped_column(String(100), nullable=False)
    date_created = mapped_column(DateTime, default=func.now())

    def to_dict(self):
        return {
            'id': self.id,
            'trace_id': self.trace_id,
            'store_id': self.store_id,
            'store_name': self.store_name,
            'product_id': self.product_id,
            'search_count': self.search_count,
            'recorded_timestamp': self.recorded_timestamp,
            'date_created': self.date_created.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        }

class PurchaseReading(Base):
    __tablename__ = "purchase_reading"

    id = mapped_column(Integer, primary_key=True)
    trace_id = mapped_column(String(100), nullable=False)  
    store_id = mapped_column(String(100), nullable=False)
    store_name = mapped_column(String(250), nullable=False)
    product_id = mapped_column(String(100), nullable=False)
    purchase_count = mapped_column(Integer, nullable=False)
    recorded_timestamp = mapped_column(String(100), nullable=False)
    date_created = mapped_column(DateTime, default=func.now())
    
    def to_dict(self):
        return {
            'id': self.id,
            'trace_id': self.trace_id,
            'store_id': self.store_id,
            'store_name': self.store_name,
            'product_id': self.product_id,
            'purchase_count': self.purchase_count,
            'recorded_timestamp': self.recorded_timestamp,
            'date_created': self.date_created.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        }


