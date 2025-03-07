from pyspark import SparkContext

# Initialize Spark Context
SC = SparkContext("local", "Query5")

# Load data from correct files
noDisclosureData = SC.textFile("/Users/eloisasalcedo-marx/IdeaProjects/project2/Meta-Event-No-Disclosure.csv")
reportedIllData = SC.textFile("/Users/eloisasalcedo-marx/IdeaProjects/project2/Reported-Illnesses.csv")

# Split data into columns
noDisclosureData = noDisclosureData.map(lambda line: line.split(","))
reportedIllData = reportedIllData.map(lambda line: line.split(","))

# Convert illness list into a set of sick people (ID)
sickPeople = set(reportedIllData.map(lambda row: row[0].strip()).collect())


# Remove the header row from noDisclosureData
header = noDisclosureData.first()
noDisclosureData = noDisclosureData.filter(lambda row: row != header)


# Filter for sick people and extract table IDs
sickTables = noDisclosureData.filter(lambda row: row[0] in sickPeople).map(lambda row: row[2].strip()).distinct()

# Persist the sick tables RDD for efficiency
sickTables.persist()

# Broadcast the sick tables to optimize filtering
sick_tables_bc = SC.broadcast(set(sickTables.collect()))

# Find healthy people who were at those tables and extract **both ID and Name**
healthy_at_risk = noDisclosureData.filter(
    lambda row: row[0] not in sickPeople and row[2].strip() in sick_tables_bc.value
).map(lambda row: (row[0], row[1]))  # (ID, Name)

# Persist result
healthy_at_risk.persist()

# Collect and display results
result = healthy_at_risk.collect()

# Print results (IDs and Names)
print("Healthy people at risk (ID, Name):", result)

healthy_at_risk.map(lambda row: ",".join(row)).saveAsTextFile("/Users/eloisasalcedo-marx/IdeaProjects/project2/healthy_at_risk_output")

SC.stop()
