from pyspark import SparkConf, SparkContext
# from IPython import embed

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("/Users/tjterris/Developer/spark-1.5.2-bin-hadoop2.6/customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
# embed()
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

flipped = totalByCustomer.map(lambda (x,y):(y,x))
totalByCustomerSorted = flipped.sortByKey()

results = totalByCustomer.collect();
for result in results:
    print result
