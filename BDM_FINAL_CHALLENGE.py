from pyspark import SparkContext
import sys


def to_house_num(houseNumRaw):
  if '-' in houseNumRaw:
    arr = houseNumRaw.split('-')
    if arr[0].isdigit() and arr[1].isdigit():
      return (int(arr[0]),int(arr[1]))
  elif houseNumRaw:
    return int(houseNumRaw)
  else:
    return 0

def isValid(range, houseNum):
  if isinstance(range[0],tuple) and isinstance(range[1],tuple) and isinstance(range[2],tuple) and isinstance(range[3],tuple) and isinstance(houseNum,tuple) :
    if houseNum[0] >= range[0][0] and houseNum[0] >= range[0][1] and houseNum[1] <= range[1][0] and houseNum[1] <= range[1][1]:
      return True
    if houseNum[0] >= range[2][0] and houseNum[0] >= range[2][1] and houseNum[1] <= range[3][0] and houseNum[1] <= range[3][1]:
      return True
  if isinstance(range[0],tuple) or isinstance(range[1],tuple) or isinstance(range[2],tuple) or isinstance(range[3],tuple) or isinstance(houseNum,tuple):
    return False   
  if houseNum >= range[0] and houseNum <= range[1]:
    return True
  if houseNum >= range[2] and houseNum <= range[3]:
    return True
  return False


def extractViolation(partId, records):
  if partId == 0:
    next(records)
  import csv
  reader = csv.reader(records)
  boroughCodes_map = {
    "MAN":1, "MH":1, "MN":1, "NEWY":1, "NEW":1,"Y":1,"NY":1,
    "BRONX":2, "BX":2,
    "BK":3, "K":3,"KING":3,"KINGS":3,
    "Q":4, "QN":4,"QNS":4, "QU":4, "QUEEN":4,
    "R":5, "RICHMOND":5
}
  for row in reader:
    if len(row) >= 43:
      date = row[4][-4:]
      county = row[21]
      summon_num = row[0]
      house_num = to_house_num(row[23])
      street_name = row[24]
      borough_code = 0
      if county in boroughCodes_map:
        borough_code = boroughCodes_map[county]
      if borough_code and street_name and house_num and date and summon_num:
        yield (borough_code, street_name), (house_num, int(date), int(summon_num))


def extractCenterLine(partId, records):
  if partId == 0:
    next(records)
  import csv
  reader = csv.reader(records)
  boroughCodes_map = {
    "MAN":1, "MH":1, "MN":1, "NEWY":1, "NEW":1,"Y":1,"NY":1,
    "BRONX":2, "BX":2,
    "BK":3, "K":3,"KING":3,"KINGS":3,
    "Q":4, "QN":4,"QNS":4, "QU":4, "QUEEN":4,
    "R":5, "RICHMOND":5
}
  for row in reader:
    if len(row) >= 32:
      physical_id = row[1]
      left_lo = to_house_num(row[0])
      left_hi = to_house_num(row[2])
      right_lo = to_house_num(row[4])
      right_hi = to_house_num(row[5])
      borough_code = int(row[13])
      street_label = row[10]
      full_label = row[28]
      if borough_code and full_label and physical_id:
        yield (borough_code, full_label), (physical_id, (left_lo, left_hi, right_lo, right_hi))
      if borough_code and street_label and physical_id:
        yield (borough_code, street_label), (physical_id, (left_lo, left_hi, right_lo, right_hi))


# violation_location \
#                   .filter(lambda x: isValid(x[1][0][1],x[1][1][0])) \
#                   .mapValues(lambda x: ((x[0][0],x[1][1]), x[1][2])) \
#                   .map(lambda x: (x[1][0], x[1][1])) \
#                   .groupByKey() \
#                   .mapValues(lambda x: len(set(x))) \
#                   .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
#                   .groupByKey() \
#                   .mapValues(lambda x: [item for item in x]) \
#                   .map(lambda x: (x[0], sorted(x[1], key=lambda tup: tup[0]))) \
#                   .take(15)

def main(sc):

	CET_FN = '/data/share/bdm/nyc_cscl.csv'
	VIO_FN = '/data/share/bdm/nyc_parking_violation/2015.csv'



	cet = sc.textFile(CET_FN, use_unicode=False).cache()
	vio = sc.textFile(VIO_FN, use_unicode=False).cache()
	violation_data = vio.mapPartitionsWithIndex(extractViolation)
	centerLine_data = cet.mapPartitionsWithIndex(extractCenterLine)
	violation_location = centerLine_data.join(violation_data)
	result = violation_location \
                  .filter(lambda x: isValid(x[1][0][1],x[1][1][0])) \
                  .saveAsTextFile("test_output")


if __name__ == "__main__":
	sc = SparkContext()
	main(sc)















