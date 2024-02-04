
import os
import sys
import snowflake.snowpark
from snowflake.snowpark import Session

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


ACCOUNT = os.getenv('SNOWSQL_ACCOUNT')
USER = os.getenv('SNOWSQL_USER')
# PASSWORD = os.getenv('SNOWSQL_PASSWORD')
PRIVATE_KEY_FILE = '/Users/spyros/Documents2/snowflake_keys/rsa_key.p8'

def connect():
    # load private key
    with open(PRIVATE_KEY_FILE, "rb") as key:
        p_key = serialization.load_pem_private_key(
            key.read(),
            password=None,
            backend=default_backend()
        )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    connection_params = {
        "user": USER,
        # "password": PASSWORD,
        "private_key": pkb,
        "account": ACCOUNT,
        "warehouse": 'XSMALL',
        "database": 'TEST',
        "schema": 'PUBLIC'
    }

    session = Session.builder.configs(connection_params).create()

    return session

def setup(session):
    result1 = session.sql("""
        CREATE OR REPLACE TABLE sample_product_data(
            id INT,
            parent_id INT,
            category_id INT,
            name VARCHAR,
            serial_number VARCHAR,
            key INT,
            "3rd" INT)"""
                          ).collect()
    
    result2 = session.sql("""
        INSERT INTO sample_product_data VALUES
        (1, 0, 5, 'Product 1', 'prod-1', 1, 10),
        (2, 1, 5, 'Product 1A', 'prod-1-A', 1, 20),
        (3, 1, 5, 'Product 1B', 'prod-1-B', 1, 30),
        (4, 0, 10, 'Product 2', 'prod-2', 2, 40),
        (5, 4, 10, 'Product 2A', 'prod-2-A', 2, 50),
        (6, 4, 10, 'Product 2B', 'prod-2-B', 2, 60),
        (7, 0, 20, 'Product 3', 'prod-3', 3, 70),
        (8, 7, 20, 'Product 3A', 'prod-3-A', 3, 80),
        (9, 7, 20, 'Product 3B', 'prod-3-B', 3, 90),
        (10, 0, 50, 'Product 4', 'prod-4', 4, 100),
        (11, 10, 50, 'Product 4A', 'prod-4-A', 4, 100),
        (12, 10, 50, 'Product 4B', 'prod-4-B', 4, 100)
    """).collect()

    result3 = session.sql("SELECT count(*) FROM sample_product_data").collect()

    row = snowflake.snowpark.Row()

    return


def main():
    try:
        session = connect()
        setup(session)
        return 0
    finally:
        try:
            session.close()
        except:
            pass


if __name__ == '__main__':
    sys.exit(main())




