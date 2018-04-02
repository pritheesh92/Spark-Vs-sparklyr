#         sparklyr
sparklyr - The best of Apache Spark and R


# 															 Data analysis using Sparklyr



##### Sparklyr – takes the best of spark and R

**Introduction : **A brief introduction, set up and demonstration of sparklyr, a package in R, for data analysis using open payments dataset.

**Connecting to the local spark cluster :**

code to connect to local cluster, but our analysis is done on EMR cluster.

```
library(sparklyr)
# install the recent version of java on the computer before this
spark_install(version = "2.1.0")
sc <- spark_connect(master = "local")
```

**Setting up R on Amazon EMR cluster :**
First create an EMR cluster on amazon AWS and define a custom port 8787 for rstudio.

1. Connect to the EMR cluster :

```
ssh -i ~/.ssh/emr_ohio.pemhadoop@ec2-18-221-229-68.us-east-2.compute.amazonaws.com
```

2. Install required elements :

```
sudo yum update
sudo yum install libcurl-devel openssl-devel # used for devtools
```

3. Install r studio :

```
wget -P /tmp
https://s3.amazonaws.com/rstudio-dailybuilds/rstudio-server-rhel-0.99.1266-x86_64.rpm
sudo yum install --nogpgcheck /tmp/rstudio-server-rhel-0.99.1266-x86_64.rpm
```

4. Create a separate user for R :

```
sudo useradd -m rstudio-user
sudo passwd rstudio-user
```

5. Create new directory in HDFS :

```
hadoop fs -mkdir /user/rstudio-user
hadoop fs -chmod 777 /user/rstudio-user
```

6. Install other dependencies for sparklyr :

```
sudo yum install curl curl-devel
```

7. Connect to R through a browser with IP:8787 (this IP will be a public IP for the EMR cluster - located in hardware tab of EMR cluster page )

```
http://18.217.7.238:8787/
```

**1. Connecting to spark EMR cluster**

Data can be fetched from any file storage systems such as HDFS, amazon S3 and local file system etc. We stored the data on amazon S3 and loaded the file directly into R on amazon EMR cluster.
Connect to the EMR spark cluster from the R studio on the cluster and read the in out data file from Amazon S3 :

```
install.packages("sparklyr")
install.packages("dplyr")
install.packages("ggplot2")
library(sparklyr)
Sys.setenv(SPARK_HOME="/usr/lib/spark")
config <- spark_config()
sc <- spark_connect(master = "yarn-client", config = config, version = '2.2.0')

physicians_tbl <- spark_read_csv(sc,name = "physicians_tbl",path ="s3://bigdataprojectcvsdata/OP_DTL_GNRL_PGYR2016_P06302017.csv")
```

We further compared data analyses using sparklyr vs spark, by demonstrating advantages of sparklyr.

For further demonstration on data analyses using sparklyr, refer to file sparklyr.md

For the same analyses on spark, refer file spark.py
