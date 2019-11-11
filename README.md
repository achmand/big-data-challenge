# Big Data Engineering - Code Challenge 
***

*Disclaimer: This is my first go at Scala and Spark, for another Big Data project visit the following [SciPi repository](https://github.com/achmand/SciPi) where I worked on a problem using Flink and Flink’s Gelly API.* 

## Task

The development of a single batch application is required in order to solve the 2 problems listed below.

- For each customer, the calculation of its money balance for its wallet is required since its registration.
- The calculation of the net profit per customer’s country is required (it can be negative, positive or zero).

## Assumptions

- #1: Every *'deposit'* is a valid tx
- #2: Every '*bet*' which has a suffix of *'_B'* is bonus bet (no balance out of wallet)
- #3: bonus bets do not consider a tax fee
- #4: Every *'bet'* which do not have a suffix of *'_B'* is not a bonus bet (balance out of wallet)
- #5 Some bets may not be placed due insufficient funds (no balance)
- #6 Every *'win'* is a valid tx
- #7 Every *'withdraw'* is valid if sufficient funds are available

## Environment

| Name     | Version           |
| -------------- | ----------------- |
| Ubuntu (Linux)  | 18.04.1 LTS      |
| IntelliJ IDEA Ultimate |  2019.1    |
| Scala  |  2.12.10             |
| Spark  |  2.4.4             |

## Deliverables
- [Code](https://github.com/achmand/big-data-challenge/blob/master/source/src/main/scala/BatchProcessing.scala)
- [Balance Result (.csv)](https://github.com/achmand/big-data-challenge/blob/master/results/customers_balance.csv)
- [Revenue Result (.csv)](https://github.com/achmand/big-data-challenge/blob/master/results/net_country_profit.csv)

## Run on Docker 

Clone repository, build docker image and bring a cluster up. 
*You can modify the number of workers too.*
```
git clone https://github.com/achmand/big-data-challenge.git
cd big-data-challenge/docker/
chmod +x start-computation.sh 
chmod +x start-master.sh 
chmod +x start-worker.sh 
docker build -t challenge/spark:latest .
docker network create spark_network
docker-compose up --scale spark-worker=2
```

Access master node container and execute program. 
**Note:** Replace container id with the container id which is running the master node (```docker container ls```).
```
docker exec -it <container_id> bash # get into master node shell 
./start-computation.sh  # start program 
```