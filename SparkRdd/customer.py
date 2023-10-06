from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Customer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customer_id = int(fields[0])
    amount = float(fields[2])
    return (customer_id, amount)

lines = sc.textFile("file:///Spark/customer-orders.csv")
rdd = lines.map(parseLine)
reduced=rdd.reduceByKey(lambda x, y: (x+y))
results  = reduced.sortByKey(True, 3, keyfunc=lambda (x, y): (y,x)).collect()
for result in results:
    print(result)
