import random
import asyncio
import time  # Add this line to import time
from faker import Faker
from acouchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from couchbase.exceptions import CouchbaseException

fake = Faker()

# Couchbase connection setup
async def connect_to_couchbase():
    cluster = Cluster(
        'couchbase://98.82.180.180',
        ClusterOptions(
            PasswordAuthenticator('Administrator', 'CQnJC2Z#fzSYNcwl')
        )
    )
    bucket = cluster.bucket('SimCardInfo')
    await bucket.on_connect()
    collection = bucket.default_collection()
    return collection

# Async function to generate a batch of records and insert them into Couchbase
async def generate_and_insert_batch(collection, batch_size, process_id):
    docs = []
    for i in range(batch_size):
        row = {
            "customerId": f"CUST{random.randint(1000, 9999)}",
            "firstName": fake.first_name(),
            "lastName": fake.last_name(),
            "address_zipCode": fake.zipcode(),
            "address_state": fake.state_abbr(),
            "address_city": fake.city(),
            "address_street": fake.street_address(),
            "phone": fake.phone_number(),
            "email": fake.email(),
            "deviceType": random.choice(["Smartphone", "Tablet", "Laptop"]),
            "lastSyncTime": fake.date_time_this_year().isoformat(),
            "osVersion": random.choice(["iOS 15", "Android 11", "Windows 10"]),
            "model": random.choice(["iPhone 13", "Galaxy S21", "Pixel 5", "iPad Pro"]),
            "iccid": f"8914800000{random.randint(1000000000, 9999999999)}",
            "imsi": f"310410{random.randint(100000000, 999999999)}",
            "msisdn": fake.phone_number(),
            "activationDate": fake.date_this_year().strftime("%Y-%m-%d"),
            "simId": f"SIM-{random.randint(100000, 999999)}",
            "expirationDate": fake.date_this_year().strftime("%Y-%m-%d"),
            "deviceId": f"device{random.randint(1000, 9999)}",
            "manufacturer": random.choice(["Apple", "Samsung", "Google"]),
            "paymentDueDate": fake.date_this_year().strftime("%Y-%m-%d"),
            "autoPayEnabled": random.choice([True, False]),
            "billingCycleStart": fake.date_this_year().strftime("%Y-%m-%d"),
            "billingCycleEnd": fake.date_this_year().strftime("%Y-%m-%d"),
            "currentCharges": round(random.uniform(50.0, 200.0), 2),
            "currentNetwork": random.choice(["Verizon", "AT&T", "T-Mobile"]),
            "roamingStatus": random.choice([True, False]),
            "homeNetwork": random.choice(["Verizon", "AT&T", "T-Mobile"]),
            "networkType": random.choice(["4G", "5G"]),
            "signalStrength": random.choice(["Excellent", "Good", "Fair", "Weak"]),
            "lastNetworkSwitch": fake.date_time_this_year().isoformat()
        }

        key = f"simcard_{row['customerId']}_{process_id}_{i}"
        docs.append((key, row))

    try:
        # Insert all documents in batch asynchronously
        tasks = [collection.upsert(key, doc) for key, doc in docs]
        await asyncio.gather(*tasks)
    except CouchbaseException as e:
        print(f"Error during insertion: {e}")
    
    print(f"Process {process_id}: Finished inserting {batch_size} records")

# Main function to generate dataset asynchronously using asyncio
async def generate_large_dataset_async(num_records, batch_size):
    collection = await connect_to_couchbase()
    
    num_batches = num_records // batch_size
    tasks = [generate_and_insert_batch(collection, batch_size, i) for i in range(num_batches)]

    # Add remainder if any
    remainder = num_records % batch_size
    if remainder:
        tasks.append(generate_and_insert_batch(collection, remainder, 'remainder'))

    await asyncio.gather(*tasks)

if __name__ == '__main__':
    # Number of records to generate
    num_records = 1000000
    batch_size = 10000  # Adjust this based on performance tests

    # Run the asyncio loop
    start_time = time.time()  # Track start time
    asyncio.run(generate_large_dataset_async(num_records, batch_size))
    print(f"Finished loading {num_records} records into Couchbase in {time.time() - start_time} seconds")
