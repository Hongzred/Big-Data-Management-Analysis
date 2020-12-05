from pyspark import SparkContext
import sys

def to_house_num(houseNumRaw):
  if '-' in houseNumRaw:
    arr = houseNumRaw.split('-')
    if arr[0].isdigit() and arr[1].isdigit():
      return (int(arr[0]),int(arr[1]))
  elif houseNumRaw.isdigit():
    return int(houseNumRaw)
  else:
    return 0

def to_decimal(houseNum):
  if type(houseNum) == tuple:
    res = float('.'.join(str(ele) for ele in houseNum))
    return res
  else:
    return houseNum

def extractViolation(partId, records):
  if partId == 0:
    next(records)
  import csv
  reader = csv.reader(records)
  for row in reader:
    if len(row) >= 25:
      date = row[4][-4:]
      county = row[21]
      summon_num = row[0]
      house_num = to_house_num(row[23])
      street_name = row[24]
      borough_code = 0
      if county.strip().upper() in boroughCodes_map:
        borough_code = boroughCodes_map[county]
      if borough_code!=0 and street_name and house_num and date and int(date) in (2015,2016,2017,2018,2019) and summon_num:
        # yield (borough_code, street_name.strip().lower().replace(" ",""),to_decimal(house_num)), (house_num, int(date), int(summon_num),1)
        yield (borough_code, street_name.strip().lower().replace(" ",""),to_decimal(house_num)), (1,int(date),summon_num)

    def extractCenterLine(partId, records):
  if partId == 0:
    next(records)
  import csv
  reader = csv.reader(records)
  for row in reader:
    if len(row) >= 30:
      physical_id = row[0]
      left_lo = to_house_num(row[2])
      left_hi = to_house_num(row[3])
      right_lo = to_house_num(row[4])
      right_hi = to_house_num(row[5])
      borough_code = int(row[13])
      street_label = row[29]
      full_label = row[28]
      isLeft = True
      
      if physical_id and physical_id.isdigit() and full_label and street_label:
        low = min(to_decimal(left_lo),to_decimal(right_lo))
        yield (borough_code, full_label.strip().lower().replace(" ",""),to_decimal(left_lo)), (2,int(physical_id))
        yield (borough_code, full_label.strip().lower().replace(" ",""),to_decimal(left_hi)), (2,int(physical_id))
        yield (borough_code, full_label.strip().lower().replace(" ",""),to_decimal(right_lo)), (2,int(physical_id))
        yield (borough_code, full_label.strip().lower().replace(" ",""),to_decimal(right_hi)), (2,int(physical_id))
        if street_label != full_label:
          yield (borough_code, street_label.strip().lower().replace(" ",""),to_decimal(left_lo)), (2,int(physical_id))
          yield (borough_code, street_label.strip().lower().replace(" ",""),to_decimal(left_hi)), (2,int(physical_id))
          yield (borough_code, street_label.strip().lower().replace(" ",""),to_decimal(right_lo)), (2,int(physical_id))
          yield (borough_code, street_label.strip().lower().replace(" ",""),to_decimal(right_hi)), (2,int(physical_id))

      
def getCounts(_, records):
  low = -1
  high = -1
  count = [0,0,0,0,0]
  pre_key = None
  for key, value in records:
    mode = value[0]
    if mode == 1 and low == -1:
      continue
    if mode == 2 and low == -1:
      low = key[2]
      continue
    if mode == 1 and low != -1:
      date = value[1]
      count[date-2015]+=1
      continue
    if mode == 2 and low != -1:
      yield  sum(count)
      low = -1
      count=[0,0,0,0,0]


def main(sc):

	CET_FN = '/data/share/bdm/nyc_cscl.csv'
	VIO_FN = '/data/share/bdm/nyc_parking_violation/*.csv'

  cet = sc.textFile(CET_FN, use_unicode=True).cache()
  vio = sc.textFile(VIO_FN, use_unicode=True).cache()
  boroughCodes_map = {
      "MAN":1, "MH":1, "MN":1, "NEWY":1, "NEW":1,"Y":1,"NY":1,
      "BRONX":2, "BX":2,
      "BK":3, "K":3,"KING":3,"KINGS":3,
      "Q":4, "QN":4,"QNS":4, "QU":4, "QUEEN":4,
      "R":5, "RICHMOND":5
  }

  violation_data = vio.mapPartitionsWithIndex(extractViolation)
  centerLine_data = cet.mapPartitionsWithIndex(extractCenterLine)

  union = (violation_data + centerLine_data).sortByKey() \
        .mapPartitionsWithIndex(getCounts) \
        .reduce(lambda x,y: x+y)
        .saveAsTextFile(countSum)

if __name__ == "__main__":
	sc = SparkContext()
	main(sc)















