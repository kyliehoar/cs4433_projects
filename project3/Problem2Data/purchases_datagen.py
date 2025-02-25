import pandas as pd
import random
import string

NUM_CUST = 50000
NUM_PURCHASE = 5000000

def gen_customers(num_cust=NUM_CUST):
    customers = []
    names = []
    ages = []
    addresses = []
    salaries = []

    for iterator in range(num_cust):
        customers.append(iterator)
        length = random.randint(10, 20)  # Random length between 10 and 20
        names.append(''.join(random.choices(string.ascii_letters + string.digits, k=length)))
        ages.append(random.randint(18,100))
        address_length = random.randint(15, 30)  # Random length between 15 and 30
        addresses.append(''.join(random.choices(string.ascii_letters + string.digits + ' ', k=address_length)).strip())
        salaries.append(random.randint(1000,10000))

    data_frame = pd.DataFrame({
        "CustID": customers,
        "Name": names,
        "Age": ages,
        "Address": addresses,
        "Salary": salaries
    })

    return data_frame

def gen_purchases(num_purchase=NUM_PURCHASE, num_cust=NUM_CUST):
    transactions = []
    customers = []
    amounts = []
    items = []
    descriptions = []

    for iterator in range(num_purchase):
        transactions.append(iterator)
        customers.append(random.randint(0,(num_cust - 1)))
        amounts.append(round(random.uniform(10,2000), 2))
        items.append(random.randint(1,15))
        descriptions.append(''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(20, 50))))

    data_frame = pd.DataFrame({
        "TransID": transactions,
        "CustID": customers,
        "TransTotal": amounts,
        "TransNumItems": items,
        "TransDesc": descriptions
    })

    return data_frame

if __name__ == "__main__":

    df_customers = gen_customers()
    df_customers.to_csv("Customers.csv", index=False)
    print("Customers.csv generated successfully")

    df_purchases = gen_purchases()
    df_purchases.to_csv("Purchases.csv", index=False)
    print("Purchases.csv generated successfully")
