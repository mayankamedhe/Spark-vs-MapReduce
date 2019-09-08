from pyspark import SparkConf, SparkContext
from functools import reduce
import sys
import os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def splitLines(line):
    line = str(line).split(" ")
    return line

def filterBadEntries(line):
    if list(line)[1] != "en":
        return False

    if list(line)[2] == "Main_Page":
        return False

    if str(list(line)[2]).startswith("special"):
        return False
    
    return True

def mapToPair(line):
    return (line[0], (line[2], int(line[3])))

def reduceToHighest(page1, page2):
    if page1[1] > page2[1]:
        return page1
    else:
        return page2

def printLines(rdd, numLines):
    testLines = rdd.take(numLines)
    
    for line in testLines:
        print(line)

def main():
    dataFolder = "pagecounts-with-time-2/"

    allFiles = os.listdir(dataFolder)
    print("All files length: ", len(allFiles))
    
    reducedFileNames = reduce((lambda path1, path2: path1 + "," + path2), map((lambda path: dataFolder + path), allFiles))
    print(reducedFileNames)
    rdd = sc.textFile(reducedFileNames)
    print("RDD count: ", rdd.count())

    job(rdd)

def job(rdd):
    dataFile = rdd

    print("Initial load:")
    printLines(dataFile, 5)

    split = dataFile.map(splitLines)

    print("After split:")
    printLines(split, 5)

    filtered = split.filter(filterBadEntries)

    print("After filter:")
    printLines(filtered, 5)

    paired = filtered.map(mapToPair)

    print("After map to pair:")
    printLines(paired, 5)

    reduced = paired.reduceByKey(reduceToHighest)

    print("After reduction:")
    printLines(reduced, 5)

    sortedByKey = reduced.sortByKey(True)

    print("After sorting")
    printLines(sortedByKey, 5)

    outputFormat = sortedByKey.map(lambda tuple: str(tuple[0]) + "\t" + "(" + str(tuple[1][1]) + ", " + str(tuple[1][0]) + ")")
    
    print("After converting for output:")
    printLines(outputFormat, 5)

    outputFormat.saveAsTextFile("./testOutput")

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')

    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    main()

