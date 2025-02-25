import pandas as pd
from faker import Faker
import random

NUM_PEOPLE = 500

def gen_meta_event (num_people=NUM_PEOPLE, sick_ratio=SICK_RATIO) -> pd.DataFrame:
    ids = []
    names = []
    tables = []
    tests = []

    fake = Faker()

    for iterator in range(num_people):
        ids.append(iterator)
        names.append(fake.name())
        tables.append(random.randint(0,50))
        tests.append("sick" if random.random() < 0.30 else "not-sick")

    data_frame = pd.DataFrame({
        "pi.id": ids,
        "pi.name": names,
        "pi.table-i": tables,
        "pi.test": tests
    })

    data_frame.to_csv("Meta-Event.csv", index=False)
    print("Meta-Event.csv generated successfully")

    df_no_disc = data_frame.drop(columns=["pi.test"])
    df_no_disc.rename(columns={"pi.table-i": "pi.table-j"}, inplace=True)
    df_no_disc.to_csv("Meta-Event-No-Disclosure.csv", index=False)
    print("Meta-Event-No-Disclosure.csv generated successfully")

    df_sick = data_frame[data_frame["pi.test"] == "sick"]
    df_sick = df_sick.drop(columns=["pi.name", "pi.table-i"])
    df_sick.to_csv("Reported-Illnesses.csv", index=False)
    print("Reported-Illnesses.csv generated successfully")

if __name__ == "__main__":
    gen_meta_event()
