
**1. Data Cleaning and transformation using dplyr**

Using the dplyr functions to select the important features and filter out as required. Arguably one of the biggest advantages of sparklyr is the ability to use dplyr operations on your data. Basically dplyr functions will be internally converted into SQL statements by sparklyr and then will be fed into spark.

Using dplyr functions to select the important features and filter out as required:

```
library(dplyr)

physicians_sub <- physicians_tbl %>% select(Covered_Recipient_Type, Teaching_Hospital_ID, Physician_Profile_ID, Recipient_City,Recipient_State, Recipient_Zip_Code, Recipient_Country, Physician_Primary_Type, Physician_Specialty, Physician_License_State_code1, Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name, Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State, Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country,Total_Amount_of_Payment_USDollars, Date_of_Payment, Nature_of_Payment_or_Transfer_of_Value, Physician_Ownership_Indicator, Contextual_Information, Record_ID, Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1, Product_Category_or_Therapeutic_Area_1, Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1,  )
```

```
physicians_sub <- physicians_sub %>% filter(Recipient_Country=="United States")
physicians_sub <- physicians_sub %>% filter(Covered_Recipient_Type=="Covered Recipient
Physician")
```

Data cleaning using dplyr functions to eliminate the null values in the data:

```
\#data cleaning: Removing the
blank values and replacing them with "Other"
physicians_sub$Contextual_Information <- ifelse(physicians_sub$Contextual_Information=="","Other",physicians_sub$Contextual_Information)

physicians_sub$Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1 <- ifelse(physicians_sub$Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1=="","Other",physicians_sub$Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1)

physicians_sub$Product_Category_or_Therapeutic_Area_1 <- ifelse(physicians_sub$Product_Category_or_Therapeutic_Area_1=="","Other",physicians_sub$Product_Category_or_Therapeutic_Area_1)

physicians_sub$Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 <- ifelse(physicians_sub$Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1=="","Other",physicians_sub$Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1)
```

**2. Visualization**

Using ggplot2, we can visualize some of the business questions which are of interest to us. Given that the dataset shows the payments made by medical devices and manufacturer companies, the first question that would be of interest would be just how much the top few doctors are being paid 

```
## top 5 paid doctors in the US

top_doctors_US <- physicians_sub %>% filter(!is.na(Physician_Profile_ID)) %>%
  group_by(Physician_Profile_ID) %>%
  summarise(payments = sum(Total_Amount_of_Payment_USDollars/1000000)) %>% 
  arrange(desc(payments)) %>%
  top_n(5) %>%
  left_join(medical_data,by="Physician_Profile_ID") %>%
  distinct(Physician_Profile_ID,Physician_First_Name,Physician_Last_Name,payments) %>%
  mutate(Physician_Name = paste(Physician_First_Name,Physician_Last_Name,sep=" "))
  
ggplot(top_doctors_US,aes(x=reorder(Physician_Name,-payments),y=payments)) + geom_bar(stat="identity",fill="skyblue") + labs(x="Doctors",y="Amount
paid (in Millions)") 
```




##### ![img](/viz/clip_image001.png)

We notice that there are doctors being paid as much as $20M!! We now narrow it down to doctors in the state of Minneapolis, which would be of a little more interest to us.

```
\## top 5 paid doctors in MN

top_doctors_MN <- physicians_sub %>% filter(!is.na(Physician_Profile_ID),Recipient_State=="MN") %>%
  group_by(Physician_Profile_ID) %>%
  summarise(payments = sum(Total_Amount_of_Payment_USDollars/1000000)) %>% 
  arrange(desc(payments)) %>%
  top_n(5) %>%
  left_join(medical_data,by="Physician_Profile_ID") %>%
  distinct(Physician_Profile_ID,Physician_First_Name,Physician_Last_Name,payments) %>%
  mutate(Physician_Name = paste(tolower(Physician_First_Name),tolower(Physician_Last_Name),sep=" "))
  
ggplot(top_doctors_MN,aes(x=Physician_Name,y=payments)) + geom_bar(stat="identity",fill="skyblue") + labs(x="Doctors",y="Amount
paid (in Millions)") + theme(axis.text.x = element_text(angle = 90, hjust = 0,vjust=0))
```




![img](/viz/clip_image002.png)

While not as high as some of the other doctors, the fact that the some doctors in Minneapolis are being paid as much as $3M is still staggering. 

We notice that there are doctors who travel abroad and wonder if the money paid to such doctors is higher or lower

```
## doctors travelling outside the US
payments_abroad <- physician_tbl %>% filter(!Country_of_Travel %in% c("United
States","")) %>%
  group_by(Physician_Profile_ID) %>%
  summarise(payments = sum(Total_Amount_of_Payment_USDollars/1000)) %>% 
  arrange(desc(payments)) %>%
  top_n(5) %>%
  left_join(medical_data,by="Physician_Profile_ID") %>%
  distinct(Physician_Profile_ID,Physician_First_Name,Physician_Last_Name,payments) %>%
  mutate(Physician_Name = paste(tolower(Physician_First_Name),tolower(Physician_Last_Name),sep=" "))
  
ggplot(payments_abroad,aes(x=Physician_Name,y=payments)) + geom_bar(stat="Identity",fill="skyblue") + labs(x="Doctors",y="Amount
paid (in thousands)")
```

![img](/viz/clip_image003.png)

Interestingly, these doctors have been paid much lesser than doctors who never leave the US for such trips. It does seem a little suspicious that the payments seem lopsided

We take a look at countries that these doctors frequent the most

```
## countries visited outside the US

top_countries_visited <- physician_tbl %>% filter(!Country_of_Travel %in% c("United
States","")) %>%
  group_by(Country_of_Travel) %>%
  summarise(visitors = n()) %>% 
  arrange(desc(visitors)) %>%
  top_n(5)
ggplot(top_countries_visited,aes(x=Country_of_Travel,y=visitors)) + geom_bar(stat="Identity",fill="skyblue")

```

![img](/viz/clip_image004.png)

Canada and China are the top destinations doctors travel to followed by 3 European countries - Germany, UK and Switzerland. Let us look at a breakdown of the types of doctors visiting outside the US 



```
## kind of doctors visiting outside the US
doctor_types_abroad <- physician_tbl %>% filter(!Country_of_Travel %in% c("United
States","")) %>%
  group_by(Physician_Primary_Type) %>%
  summarise(doctors = n()) %>% 
  arrange(desc(doctors)) %>%
  top_n(3)
\## Selecting by doctors
ggplot(doctor_types_abroad,aes(x=reorder(Physician_Primary_Type,-doctors),y=doctors)) + geom_bar(stat="Identity",fill="skyblue") + labs(x="Physician
type",y="Number
of doctors")
```

![img](/viz/clip_image005.png)

Not surprisingly, medical doctors are the overwhelming favorites for being invited to travel abroad. Dentists and ostheopathy practitioners come a distant second and third

Finally,we are interested in companies which have approved the highest payouts

```
## companies which pay out the most

highest_company_payouts <- physicians_sub %>% group_by(Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name) %>%
  summarise(payments = sum(Total_Amount_of_Payment_USDollars/1000000)) %>% 
  arrange(desc(payments)) %>%
  top_n(5)
ggplot(highest_company_payouts,aes(x=reorder(Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,-payments),y=payments)) + geom_bar(stat="Identity",fill="skyblue") + labs(x="Company",y="Payout
(in millions)") + theme(axis.text.x = element_text(angle = 90, hjust = 0,vjust=0))

```

![img](/viz/clip_image006.png)

We see that Genentech Inc pays out more than $350M, more than the next 3 companies combined.

Playing around with the data has enabled us to get a better understanding of the data well as make a couple of interesting observations

**3.Modelling using Regression :**

sparklyr supports the native mllib package in spark and supports over 10,000 packages. sparklyr is integrated with other R packages and supports scalable, high performance models. sparklyr also supports wide variety of algorithms and can create custom extensions in case the package is not available in sparklyr.

We used linear regression modelling technique to predict the amount paid to the physicians based on features "Recipient_State", "Physician_Primary_Type", "Physician_License_State_code1", "Nature_of_Payment_or_Transfer_of_Value". Unlike Spark, the attributes need not be converted into double. R will take of datatypes transformation and it would be much more easier to run modelling techniques if there are multiple attributes.

```
physicians_sub1 <- physicians_sub %>% select("Recipient_State","Physician_Primary_Type","Physician_License_State_code1","Total_Amount_of_Payment_USDollars","Nature_of_Payment_or_Transfer_of_Value")

physicians_model <- physicians_sub1 %>%
  sdf_partition(train = 0.8, test = 0.2, seed = 1234)

\# Create table references
physicians_train_tbl <- sdf_register(physicians_model$train, "physicians_train")
physicians_test_tbl <- sdf_register(physicians_model$train, "physicians_test")

\# Cache model data
tbl_cache(sc, "physicians_train")

\# Model data
model_formula <- formula(Total_Amount_of_Payment_USDollars ~ 
                Nature_of_Payment_or_Transfer_of_Value 
                + Recipient_State  
                + Physician_Primary_Type 
                + Physician_License_State_code1)
m1 <- ml_linear_regression(physicians_model$train, model_formula)

## * No rows dropped by 'na.omit' call
```

 

```
summary(m1)

\## Call: ml_linear_regression(physicians_model$train,model_formula)

\## Deviance Residuals: (approximate):

\##       Min        1Q   Median        3Q       Max 

\## -56775.85 -1470.04   -978.25     86.35 98248.04  

\## (Intercept)                                                                                                                                                                               34862.3758                                                                                      

\## R-Squared: 0.3033

\## Root Mean Squared Error: 6235
```

 

References:

<https://spark.rstudio.com/examples/yarn-cluster-emr/>

<https://medium.com/ibm-data-science-experience/read-and-write-data-to-and-from-amazon-s3-buckets-in-rstudio-1a0f29c44fa7>

 
