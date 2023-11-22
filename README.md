# pyspark-application-earthquake

#The application was developed and tested on YARN managed spark cluster provided by ITVersity

#run the below command to initiate the job on spark cluster. Make sure the csv file should be named database.csv and present in the home directory

spark-submit \
--master yarn \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 2G \
--conf spark.dynamicAllocation.enabled=false \
database.csv
