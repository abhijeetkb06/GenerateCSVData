import random
import pandas as pd
from faker import Faker
from multiprocessing import Pool, cpu_count
import os

# Function to generate a chunk of records
def generate_chunk(chunk_size, process_id):
    fake = Faker()  
    data = []
    for i in range(chunk_size):
        if i % 100000 == 0 and i > 0:
            print(f"Process {process_id}: Generated {i} records out of {chunk_size}")
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
        data.append(row)
    return data

# Main function to generate dataset using multiprocessing
def generate_large_dataset_multiprocessing(num_records):
    num_cores = cpu_count()
    chunk_size = num_records // num_cores
    pool = Pool(processes=num_cores)

    print(f"Generating {num_records} records using {num_cores} cores...")

    # Generate data in parallel chunks
    chunks = [(chunk_size, i) for i in range(num_cores)]  # Pass chunk size and process id
    results = pool.starmap(generate_chunk, chunks)  # Using starmap to pass multiple arguments

    # Flatten the list of lists into a single list
    data = [item for sublist in results for item in sublist]

    # Handle any remainder records
    remainder = num_records % num_cores
    if remainder:
        print(f"Generating the remaining {remainder} records...")
        data.extend(generate_chunk(remainder, "remainder"))

    pool.close()
    pool.join()

    return data

if __name__ == '__main__':
    # Number of records to generate
    num_records = 100000

    # Generate the dataset in parallel using multiprocessing
    dataset = generate_large_dataset_multiprocessing(num_records)

    # Convert to DataFrame
    df = pd.DataFrame(dataset)

    # Save as CSV file in the current directory
    output_file_path = os.path.join(os.getcwd(), "sim_card_info_1_million_multiprocessing.csv")
    df.to_csv(output_file_path, index=False)

    print(f"1 million records saved to {output_file_path}")