# still works locally not in a cluster 
/spark/bin/spark-submit --master spark://spark-master:7077 --class BatchProcessing source.jar '/data/' '/output/' 'local'
