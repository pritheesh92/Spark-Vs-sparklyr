from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import numpy as np

data = sqlContext.read\
    .format('com.databricks.spark.csv')\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("s3://msba6330-spark/OP_DTL_GNRL_PGYR2016_P06302017.csv")
	
data.limit(10).toPandas()	

data.printSchema()

#five numerical columns

data.describe().toPandas().transpose()

#
physicians_sub = data.select([c for c in data.columns if c in [
    "Covered_Recipient_Type",
    "Teaching_Hospital_ID",
    "Physician_Profile_ID",
    "Recipient_City",
    "Recipient_State",
    "Recipient_Country",
    "Physician_Primary_Type",
    "Physician_Specialty",
    "Physician_License_State_code1",
    "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Countr",
    "Total_Amount_of_Payment_USDollars",
    "Date_of_Payment",
    "Nature_of_Payment_or_Transfer_of_Value",
    "Physician_Ownership_Indicator",
    "Contextual_Information",
    "Record_ID",
    "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1",
    "Product_Category_or_Therapeutic_Area_1",
    "City_of_Travel",
    "State_of_Travel",
    "Country_of_Travel",
    "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1"]])
	
	#filter on Covered_Recipient_Type
data_filtered = data_filtered[data_filtered['Covered_Recipient_Type']\
                               == "Covered Recipient Physician"]
data_filtered = data_filtered[data_filtered["Recipient_Country"]\
                               == "United States"]
data_filtered.printSchema()		

pmt_cnt_by_priType = data_filtered.select("Physician_Primary_Type")\
    .groupBy("Physician_Primary_Type")\
    .count()\
    .sort(desc("count"))
pmt_cnt_by_priType.limit(5).toPandas()

pmt_cnt_by_spec = data_filtered.select("Physician_Specialty")\
    .groupBy("Physician_Specialty")\
    .count()\
    .sort(desc("count"))
pmt_cnt_by_spec.limit(5).toPandas()


pmt_cnt_by_priType = data_filtered.select("Physician_Primary_Type")\
    .groupBy("Physician_Primary_Type")\
    .count()\
    .sort(desc("count"))
pmt_cnt_by_priType.limit(5).toPandas()

sum_pay_state = physicians_sub.select("Total_Amount_of_Payment_USDollars",\
                                     "Recipient_State")\
                    .groupby("Recipient_State")\
                    .sum()\
                    .sort(desc("sum(Total_Amount_of_Payment_USDollars)"))\
                    .limit(10)\
                    .toPandas()

y_pos = np.arange(10)
 
plt.bar(y_pos, sum_pay_state['sum(Total_Amount_of_Payment_USDollars)'],
        align='center', alpha=0.5)
plt.xticks(y_pos, sum_pay_state['Recipient_State'])
plt.ylabel('Payment Amount')
plt.title('Payments by State')
 
plt.show()


data_filtered.select("Total_Amount_of_Payment_USDollars", "Recipient_State",\
                     "Nature_of_Payment_or_Transfer_of_Value").groupby("Recipient_State",\
                     "Nature_of_Payment_or_Transfer_of_Value").mean().sort(desc("avg(Total_Amount_of_Payment_USDollars)")).limit(5).toPandas()



avg_type_pay_state = data_filtered.select("Total_Amount_of_Payment_USDollars", "Recipient_State")\
    .groupby("Recipient_State").mean()\
    .sort(desc("avg(Total_Amount_of_Payment_USDollars)")).limit(5).toPandas()

y_pos = np.arange(5)
 
plt.bar(y_pos, avg_type_pay_state['avg(Total_Amount_of_Payment_USDollars)'], align='center', alpha=0.5)
plt.xticks(y_pos, avg_type_pay_state['Recipient_State'])
plt.ylabel('Payment Amount')
plt.title('Average Payments by State')
 
plt.show()


avg_pay_nat_pay = data_filtered.select("Total_Amount_of_Payment_USDollars", "Nature_of_Payment_or_Transfer_of_Value")\
    .groupby("Nature_of_Payment_or_Transfer_of_Value").mean()\
    .sort(desc("avg(Total_Amount_of_Payment_USDollars)")).limit(5).toPandas()

y_pos = np.arange(5)
 
plt.bar(y_pos, avg_pay_nat_pay['avg(Total_Amount_of_Payment_USDollars)'], align='center', alpha=0.5)
plt.xticks(y_pos, avg_pay_nat_pay['Nature_of_Payment_or_Transfer_of_Value'])
plt.ylabel('Payment Amount')
plt.title('Average Payments by Type')
 
plt.show()

top_travel_dest = data_filtered\
    .withColumn("City_of_Travel", initcap(lower(col("City_of_Travel"))))\
    .filter(data_filtered.Nature_of_Payment_or_Transfer_of_Value=="Travel and Lodging")\
    .filter(data_filtered.Country_of_Travel != "United States")\
    .select("City_of_Travel", "Country_of_Travel")\
    .groupBy("City_of_Travel", "Country_of_Travel")\
    .count()\
    .sort(desc("count"))\
    .limit(10)\
    .toPandas()

y_pos = np.arange(10)
 
plt.bar(y_pos, top_travel_dest['count'], align='center', alpha=0.5)
plt.xticks(y_pos, top_travel_dest['City_of_Travel'])
plt.ylabel('Number of Travels')
plt.title('Travels by City')

plt.show()

top_docs_MN = data_filtered\
    .filter(data_filtered.Physician_License_State_code1=="MN")\
    .filter(data_filtered.Covered_Recipient_Type=="Covered Recipient Physician")\
    .select("Total_Amount_of_Payment_USDollars", "Physician_Profile_ID")\
    .groupBy("Physician_Profile_ID")\
    .sum("Total_Amount_of_Payment_USDollars")\
    .sort(desc("sum(Total_Amount_of_Payment_USDollars)"))
top_5_docs_MN = top_docs_MN.limit(5).toPandas()
top_5_docs_MN


y_pos = np.arange(5)
 
plt.bar(y_pos, top_5_docs_MN['sum(Total_Amount_of_Payment_USDollars)'], align='center', alpha=0.5)
plt.xticks(y_pos, top_5_docs_MN['Physician_Profile_ID'])
plt.ylabel('Total Payment Amount')
plt.xlabel('Physician ID')
plt.title('Payment by Physician ID')

plt.show()

top_docs_all = data_filtered\
    .filter(data_filtered.Covered_Recipient_Type=="Covered Recipient Physician")\
    .select("Total_Amount_of_Payment_USDollars", "Physician_Profile_ID")\
    .groupBy("Physician_Profile_ID")\
    .sum("Total_Amount_of_Payment_USDollars")\
    .sort(desc("sum(Total_Amount_of_Payment_USDollars)"))
top_5_docs_all = top_docs_all.limit(5).toPandas()
top_5_docs_all

y_pos = np.arange(5)
 
plt.bar(y_pos, top_5_docs_all['sum(Total_Amount_of_Payment_USDollars)'], align='center', alpha=0.5)
plt.xticks(y_pos, top_5_docs_all['Physician_Profile_ID'])
plt.ylabel('Total Payment Amount')
plt.xlabel('Physician ID')
plt.title('Payment by Physician ID')

plt.show()

data_filtered.withColumn("City_of_Travel", initcap(lower(col("City_of_Travel"))))\
    .filter(data_filtered.Nature_of_Payment_or_Transfer_of_Value == "Travel and Lodging")\
    .filter(data_filtered.Country_of_Travel != "United States")\
    .select("City_of_Travel", "Country_of_Travel")\
    .groupby("City_of_Travel", "Country_of_Travel").count()\
    .sort(desc("count")).limit(5).toPandas()

data_filtered.toPandas().shape

data_LR = data_filtered.select("Recipient_State", 
                             "Nature_of_Payment_or_Transfer_of_Value", 
                             "Physician_License_State_code1", 
                             "Physician_Primary_Type", 
                             "Total_Amount_of_Payment_USDollars")


from pyspark.ml.feature import (VectorAssembler,
                                VectorIndexer,
                                OneHotEncoder,
                                StringIndexer)

Rstate_indexer = StringIndexer(inputCol = "Recipient_State", 
                               outputCol = "ind_Rstate")
Rstate_encoder = OneHotEncoder(inputCol = "ind_Rstate", 
                               outputCol = "OHERstate")

Pstate_indexer = StringIndexer(inputCol = "Physician_License_State_code1", 
                               outputCol = "ind_Pstate")
Pstate_encoder = OneHotEncoder(inputCol = "ind_Pstate", 
                               outputCol = "OHEPstate")

Pprime_indexer = StringIndexer(inputCol = "Physician_Primary_Type", 
                               outputCol = "ind_Pprime")
Pprime_encoder = OneHotEncoder(inputCol = "ind_Pprime", 
                               outputCol = "OHEPprime")

Type_indexer = StringIndexer(inputCol = "Nature_of_Payment_or_Transfer_of_Value", 
                             outputCol = "ind_Type")
Type_encoder = OneHotEncoder(inputCol = "ind_Type", 
                             outputCol = "OHE_Type")

assembler = VectorAssembler(inputCols=["OHERstate", "OHEPstate", 
                                       "OHEPprime", "OHE_Type"], 
                            outputCol="features")


from pyspark.ml.regression import LinearRegression

lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, 
                      labelCol = "Total_Amount_of_Payment_USDollars")

from pyspark.ml import Pipeline


pipeline = Pipeline(stages=[Rstate_indexer, Rstate_encoder, 
                            Pstate_indexer, Pstate_encoder, 
                            Pprime_indexer, Pprime_encoder, 
                            Type_indexer, Type_encoder])

data_LR_model = pipeline.fit(data_LR).transform(data_LR)

train_data, test_data = data_LR_model.randomSplit([0.7,0.3]) 

data_LR_model = lr.fit(train_data)

results = data_LR_model.transform(test_data)

lrModel_model.summary()
print("Coefficients: %s" % str(lrModel_model.coefficients))
print("Intercept: %s" % str(lrModel_model.intercept))

results = lrModel_model.transform(test_data)
results2 = results.withColumn('prediction', when(results.prediction >= 0, results.prediction).otherwise(0.0))

# Summarize the model over the testing set and print out some metrics
testingSummary = lrModel_model.
print("numIterations: %d" % testingSummary.totalIterations)
print("objectiveHistory: %s" % str(testingSummary.objectiveHistory))
testingSummary.residuals.show()
print("RMSE: %f" % testingSummary.rootMeanSquaredError)
print("r2: %f" % testingSummary.r2)

							