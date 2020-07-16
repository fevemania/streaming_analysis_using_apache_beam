import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Sequence, Column, Integer, Unicode, DateTime, Numeric, Float
import pdb
from sqlalchemy.schema import CreateTable

def init_connection_engine():
    db_config = {
        # [START cloud_sql_postgres_sqlalchemy_limit]
        # Pool size is the maximum number of permanent connections to keep.
        "pool_size": 5,
        # Temporarily exceeds the set pool_size if no connections are available.
        "max_overflow": 2,
        # The total number of concurrent connections for your application will be
        # a total of pool_size and max_overflow.
        # [END cloud_sql_postgres_sqlalchemy_limit]

        # [START cloud_sql_postgres_sqlalchemy_backoff]
        # SQLAlchemy automatically uses delays between failed connection attempts,
        # but provides no arguments for configuration.
        # [END cloud_sql_postgres_sqlalchemy_backoff]

        # [START cloud_sql_postgres_sqlalchemy_timeout]
        # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
        # new connection from the pool. After the specified amount of time, an
        # exception will be thrown.
        "pool_timeout": 30,  # 30 seconds
        # [END cloud_sql_postgres_sqlalchemy_timeout]

        # [START cloud_sql_postgres_sqlalchemy_lifetime]
        # 'pool_recycle' is the maximum number of seconds a connection can persist.
        # Connections that live longer than the specified amount of time will be
        # reestablished
        "pool_recycle": 1800,  # 30 minutes
        # [END cloud_sql_postgres_sqlalchemy_lifetime]
    }

    return init_tcp_connection_engine(db_config)

def init_tcp_connection_engine(db_config):
    # [START cloud_sql_postgres_sqlalchemy_create_tcp]
    # Remember - storing secrets in plaintext is potentially unsafe. Consider using
    # something like https://cloud.google.com/secret-manager/docs/overview to help keep
    # secrets secret.
    db_user = 'XXX' 
    db_pass = 'YYY' 
    db_name = 'prac'
    db_host = 'localhost:5432' 

    # Extract host and port from db_host
    host_args = db_host.split(":")
    db_hostname, db_port = host_args[0], int(host_args[1])

    pool = sqlalchemy.create_engine(
        # Equivalent URL:
        # postgres+pg8000://<db_user>:<db_pass>@<db_host>:<db_port>/<db_name>
        sqlalchemy.engine.url.URL(
            drivername="postgres+psycopg2",
            username=db_user,  # e.g. "my-database-user"
            password=db_pass,  # e.g. "my-database-password"
            host=db_hostname,  # e.g. "127.0.0.1"
            port=db_port,  # e.g. 5432
            database=db_name  # e.g. "my-database-name"
        ),
        # ... Specify additional properties here.
        # [END cloud_sql_postgres_sqlalchemy_create_tcp]
        **db_config
        # [START cloud_sql_postgres_sqlalchemy_create_tcp]
    )
    # [END cloud_sql_postgres_sqlalchemy_create_tcp]

    return pool

engine = init_connection_engine()

Base = declarative_base()

class Test201510191900(Base):
   #__tablename__ = 'test201510191900'
    __tablename__ = 'new_york_yellow_taxi_trips_20151019'

   #id = Column(Integer, Sequence('test201510191900_id_seq'), primary_key=True)
    id = Column(Integer, primary_key=True)
    vendor_id = Column(Unicode, nullable=False)
    pickup_datetime = Column(DateTime)
    dropoff_datetime = Column(DateTime)
    passenger_count = Column(Integer)
    trip_distance = Column(Float)
    pickup_longitude = Column(Numeric)
    pickup_latitude = Column(Numeric)
    rate_code = Column(Integer)
    store_and_fwd_flag = Column(Unicode)
    dropoff_longitude = Column(Numeric)
    dropoff_latitude = Column(Numeric)
    payment_type = Column(Unicode)
    fare_amount = Column(Float)
    extra = Column(Float)
    mta_tax = Column(Float)
    tip_amount = Column(Float)
    tolls_amount = Column(Float)
    imp_surcharge = Column(Float)
    total_amount = Column(Float)

class NewYork(Base):
    __tablename__ = 'new_york_yellow_taxi_aggregate'

    id = Column(Integer, primary_key=True)
    window_start = Column(DateTime)
    vendor_id = Column(Unicode, nullable=False)
    passenger_count = Column(Integer)

if __name__ == '__main__':
    Base.metadata.create_all(engine)
