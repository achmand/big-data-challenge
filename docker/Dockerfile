# get OpenJDK image (Java)
FROM openjdk:8-alpine

# installing utilities; 
# 1. wget: download spark 
# 2. tar: extract spark
# 3. bash: to run scripts  
RUN apk --update add wget tar bash 

# downloads spark from source 
RUN wget https://www-eu.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz 

# extracts spark and moves it to '/spark' dir, once finished removes '.tgz'
RUN tar -xzf spark-2.4.4-bin-hadoop2.7.tgz && \
    mv spark-2.4.4-bin-hadoop2.7 /spark && \
    rm spark-2.4.4-bin-hadoop2.7.tgz

# get/copy the start master shell script to image. 
# to make script executable (chmod +x start-master.sh)
COPY start-master.sh /start-master.sh 

# get/copy the start worker shell script to image. 
# to make script executable (chmod +x start-worker.sh)
COPY start-worker.sh /start-worker.sh 

# to make script executable (chmod +x start-master.sh)
COPY start-computation.sh /start-computation.sh 

# copy jar file 
COPY source.jar /source.jar 

# copy data file 
COPY data/ /data/ 

