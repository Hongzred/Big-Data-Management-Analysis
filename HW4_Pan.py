from pyspark import SparkContext

def extractComplaints(partId, complains):
    if partId == 0:
        next(complains)
    import csv 
    reader = csv.reader(complains)
    for record in reader:
        if len(record)>7 and record[0][:4].isdigit():
            yield ((record[0][:4], record[1].lower(),record[7].lower()),1)

def main(sc):
	rdd = sc.textFile('complaints_small.csv')
	runner = sc.parallelize([1,2,3,4,5])
	complaints = sc.textFile('complaints_small.csv',use_unicode=False).cache()
	result = complaints.mapPartitionsWithIndex(extractComplaints) \
        .groupByKey() \
        .mapValues(lambda values: sum(values)) \
        .map(lambda x: ((x[0][1],x[0][0]), x[1])) \
        .groupByKey() \
        .mapValues(lambda x: (sum(x),len(x), round(max(x)/sum(x)*100))) \
        .sortByKey() \
        .map(lambda x: [item for items in x for item in items]) \
        .take(25)

	result

if __name__ == "__main__":
	sc = SparkContext()
	main(sc)
