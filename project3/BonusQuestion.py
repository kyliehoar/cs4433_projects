from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize Spark
spark = SparkSession.builder \
    .appName("MLlib_TransTotal_Prediction") \
    .master("local[*]") \
    .getOrCreate()

# Load the dataset
dataset = spark.read.csv("/Users/eloisasalcedo-marx/IdeaProjects/project2/Scalafolder/Purchases.csv",
                         header=True, inferSchema=True)

# Encode CustID as a numerical feature
indexer = StringIndexer(inputCol="CustID", outputCol="CustID_indexed")
dataset = indexer.fit(dataset).transform(dataset)

# Select relevant columns
dataset = dataset.select("TransNumItems", "CustID_indexed", "TransTotal")

# Assemble features using multiple relevant columns
assembler = VectorAssembler(inputCols=["TransNumItems", "CustID_indexed"], outputCol="features")
dataset = assembler.transform(dataset).select("features", "TransTotal")

# Rename transaction total to "label"
dataset = dataset.withColumnRenamed("TransTotal", "label")

# Split the data into training (80%) and testing (20%)
train_data, test_data = dataset.randomSplit([0.8, 0.2], seed=42)

# Initialize and train the Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="label")
model = lr.fit(train_data)

# Print model coefficients & intercept
print(f"Coefficients: {model.coefficients}")
print(f"Intercept: {model.intercept}")

# Make Predictions
predictions = model.transform(test_data)
predictions.show()

# Evaluate the Model
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse}")

# Save the trained model
model.save("trans_total_model")

# Load the model back
from pyspark.ml.regression import LinearRegressionModel
loaded_model = LinearRegressionModel.load("trans_total_model")

# Make new predictions with the loaded model
new_predictions = loaded_model.transform(test_data)
new_predictions.show()

# Stop Spark Session
spark.stop()
