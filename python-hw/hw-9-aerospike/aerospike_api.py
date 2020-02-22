import logging
import aerospike
from aerospike import exception as ex
import sys
from aerospike import predicates as p

AEROSPIKE_HOST = '127.0.0.1'
AEROSPIKE_PORT = 3000

NAMESPACE = 'test'
SET = 'otus'
PHONE_NUMBER_INDEX = 'test_otus_phone_number_idx'

PHONE_NUMBER = 'phone_number'
LIFETIME_VALUE = 'lifetime_value'

config = {
    'hosts': [(AEROSPIKE_HOST, AEROSPIKE_PORT)]
}

client = None
try:
    client = aerospike.client(config).connect()
    client.index_integer_create(NAMESPACE, SET, PHONE_NUMBER, PHONE_NUMBER_INDEX)
except ex.IndexFoundError as e:
    logging.error('Error: {0} [{1}]'.format(e.msg, e.code))
except ex.AerospikeError as e:
    logging.error('Failed to connect to the cluster with', config['hosts'])
    sys.exit(1)


def add_customer(customer_id, phone_number, lifetime_value):
    key = (NAMESPACE, SET, customer_id)
    try:
        client.put(key, {
            PHONE_NUMBER: phone_number,
            LIFETIME_VALUE: lifetime_value
        })
    except ex.AerospikeError as e:
        logging.error("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)


def get_ltv_by_id(customer_id):
    key = (NAMESPACE, SET, customer_id)
    try:
        (key, metadata, record) = client.get(key)
        return record[LIFETIME_VALUE]
    except ex.RecordNotFound:
        logging.error("Record not found:", key)
    except ex.AerospikeError as e:
        logging.error("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)


def get_ltv_by_phone(phone_number):
    query = client.query(NAMESPACE, SET)
    query.select(PHONE_NUMBER, LIFETIME_VALUE)
    query.where(p.equals(PHONE_NUMBER, phone_number))
    records = query.results({'total_timeout': 2000})
    logging.debug(f'Found {len(records)} for phone_number={phone_number}. Will return the first one.')
    if len(records) != 1:
        return -1
    try:
        lifetime_value = records[0][2][LIFETIME_VALUE]
        return lifetime_value
    except:
        logging.error(f'Cant extract lifetime_value from the record={records}')


for i in range(0, 1000):
    add_customer(i, i, i + 1)

for i in range(0, 1000):
    assert (i + 1 == get_ltv_by_id(i)), 'No LTV by ID ' + str(i)
    assert (i + 1 == get_ltv_by_phone(i)), 'No LTV by phone ' + str(i)

if client is not None:
    client.close()
