import random
import pandas as pd
from faker import Faker

# Initialize Faker for realistic data generation
fake = Faker()

# Function to generate 1 million records of simulated data
def generate_large_dataset(num_records):
    data = []
    for i in range(1, num_records + 1):
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
        
        # Print progress every 100,000 records
        if i % 100000 == 0:
            print(f"Generated {i} records out of {num_records}")
    
    return data

# Generate 1 million records
num_records = 1000000
print(f"Generating {num_records} records...")

# Create the dataset
dataset = generate_large_dataset(num_records)

# Convert to DataFrame
df = pd.DataFrame(dataset)

# Save as CSV file in the same directory where the script is located
output_file_path = "sim_card_info_1_million.csv"
df.to_csv(output_file_path, index=False)

print(f"1 million records saved to {output_file_path}")
