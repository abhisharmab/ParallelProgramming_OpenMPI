import random 
import numpy as py
import copy

# Reference for KMean Algorithm 
# http://datasciencelab.wordpress.com/2013/12/12/clustering-with-k-means-in-python/

#This is a logical abstraction of Cluster.
class Cluster: 
    # Centroid of that Cluster
    # HashTable containing a mapping between points and the distances 
    def __init__(self, dataPoint, pointsandDistance = {}):
        self.centroid = dataPoint
        self.pointsandDistance = pointsandDistance
        
    def getCurrentCentroid(self):
        print "Current centroid for this cluster is - ", self.centroid  
        
        
        
#This is a logical abstraction of a Point
class DataPoint:
    #Method to initialize the Tuple (basically the Data Point) 
    def __init__(self, tuple): 
        self.tuple = tuple
    
    #Method to be able to print the Tuple 
    #Reference: http://stackoverflow.com/questions/1984162/purpose-of-pythons-repr
    def __repr__(self):
        return '%s' % self.tuple
    
    #def __eq__(self,other):
        #return self.tuple == other.tuple
        
        
    
#Generate Random Data 
def generatorNumbers(lowerBound, upperBound, maxPoints, numClusters):
    dataCollection = []
    for i in range(1, maxPoints):
        dataCollection.append(DataPoint([(py.random.uniform(lowerBound,upperBound)) for j in range(2)])) #2D points
        
        
    #Creating Dictionaries for each of the Clusters. Since they need to operate on that    
    ListofHashtableofDataPoints = []
    
    tempHashTable = {}
    for i in range(len(dataCollection)):
        tempHashTable.update({dataCollection[i]:0})
    
    for i in range(numClusters):
        ListofHashtableofDataPoints.append(copy.deepcopy(tempHashTable));
        
    print ListofHashtableofDataPoints
        
    '''
    # Experimental Code to Learn Python
    x = {}
    x.update({dataCollection[0] : 10})
    c = Cluster(dataCollection[0], x)
    if(10 == x.get(dataCollection[0])):
        print "We Are good"

    for k in sorted(x.keys()):
        if(dataCollection[0] == k):
            print "even better"
    c.getCurrentCentroid()    
    print dataCollection[0]
    '''
        
    '''clusterList = []
    initialCentroids = py.random.sample(dataCollection, numClusters)
    
    for randomPoint in initialCentroids:
        clusterList.append(object)
    
    for i in range(numClusters):
         clusterList.append(py.random.sample(dataCollection))
         
    print clusterList'''
    
def main():
    #numDimensions = int (raw_input("Enter number of Dimensions N:\n"))
    lowerBound = 1 #int (raw_input("Enter the lowerBound for DataGenerationN: \n"))
    upperBound = 2 #int (raw_input("Enter the upperBound for DataGenerationN: \n"))
    maxPoints = 10 #int (raw_input("Enter the maxPoints for DataGenerationN:\n"))
    numClusters = 3 #int (raw_input("Enter the number of Clusters :\n")) 
    generatorNumbers(lowerBound, upperBound, maxPoints, numClusters)
    
if __name__ == "__main__":
    main()
    
