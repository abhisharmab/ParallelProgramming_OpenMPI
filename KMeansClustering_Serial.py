import random 
import numpy as py
import copy
from numpy import matrix
from numpy import mean

#KMean Serialized Algorithm 
tempHashTable = {}
#This is a logical abstraction of Cluster.
class Cluster: 
    # Centriod of that Cluster
    # HashTable containing a mapping between points and the distances 
    def __init__(self, dataPoint, pointsandDistance=None):
        self.centroid = dataPoint
        self.pointsandDistance = pointsandDistance

        
    def getCurrentCentroid(self):
        print "Current centroid for this cluster is - ", self.centroid  
    
    def __hash__(self):
        return hash(self)
        
    def __repr__(self):
        return '%s %s' % (self.centroid, self.pointsandDistance)
'''        
#This is a logical abstraction of a Point
class DataPoint:
    #Method to initialize the Tuple (basically the Data Point) 
    def __init__(self, tuple): 
        self.tuple = tuple
    
    #Method to be able to print the Tuple 
    Reference: http://stackoverflow.com/questions/1984162/purpose-of-pythons-repr
    def __repr__(self):
        return '%s' % self.tuple
    #def __eq__(self,other):
        #return self.tuple == other.tuple
 '''
#Calculate Eculedian Distance 
def calculateEculedianDistance(centroid, datapoint):
    #return py.sqrt(sum((py.array(centroid.tuple) - py.array(datapoint.tuple)) ** 2))
    return py.sqrt(sum((py.array(centroid) - py.array(datapoint)) ** 2))

def kMeansAlgo(clusterList, threshold, flag, Max_Iterations):
    newCentroids = []
    for cluster in clusterList:
        newCentroids.append(cluster.centroid)

    
    #newCentroids = ([(clusterList[i].centroid)] for i in range(len(clusterList)))
    oldCentroids = [] #py.array([])
    iterations = 0;
    
    while not shouldNotStop(oldCentroids, newCentroids, iterations, Max_Iterations):
        #print "OldCentroids" , oldCentroids
        #print  "NewCentroids" , newCentroids
        oldCentroids = newCentroids
        iterations += 1
        i=0
    
        for cluster in clusterList:
            cluster.pointsandDistance = copy.deepcopy(tempHashTable)
            
        for currentCluster in clusterList:  # Iteration over each of the Cluster
            i = i + 1
            pointstoDeletefromCurrentCluster = []
            for element in currentCluster.pointsandDistance:
                #if(element != currentCluster.centroid):           
                    distCurrentCluster = calculateEculedianDistance(currentCluster.centroid, element)
                    currentCluster.pointsandDistance[element] = distCurrentCluster #Update Distance in HashTable
                    if(i > 1):
                        #print i
                        for prevCluster in clusterList[::-1]: #Iterate over Previous Lists
                            if(prevCluster.centroid == currentCluster.centroid):
                                continue
                            else:
                                print "prev" , prevCluster.centroid, prevCluster.pointsandDistance
                                print "curr" , currentCluster.centroid, currentCluster.pointsandDistance
                                print "keyToSearchinPrev" , element
                                if(prevCluster.pointsandDistance.has_key(element)):
                                    if(distCurrentCluster > prevCluster.pointsandDistance.get(element) and distCurrentCluster >= threshold):
                                        print "We are bigger"
                                        pointstoDeletefromCurrentCluster.append(element)
                                        break  
                                    elif(distCurrentCluster == prevCluster.pointsandDistance.get(element)):
                                        print "We are equal"
                                        pointstoDeletefromCurrentCluster.append(element)
                                        break
                                    elif((distCurrentCluster > prevCluster.pointsandDistance.get(element) and distCurrentCluster <= threshold)
                                         or (distCurrentCluster < prevCluster.pointsandDistance.get(element))):
                                        print "element is smaller or we are good within threshold"
                                        print distCurrentCluster
                                        print prevCluster.pointsandDistance[element]
                                        del prevCluster.pointsandDistance[element]
                                        

          
            for item in pointstoDeletefromCurrentCluster:
                del currentCluster.pointsandDistance[item]
                
            #print currentCluster.pointsandDistance
            #print currentCluster.pointsandDistance.keys()
            
                            
        for cluster in clusterList:
            print "Final Cluster Points-" , cluster.pointsandDistance
            cluster.centroid = tuple(map(mean, zip(*cluster.pointsandDistance.keys())))
            print "Final New Centroid" , cluster.centroid
    
                  
def shouldNotStop(oldCentroids, newCentroids, iterations, Max_Iterations):
    if (iterations > Max_Iterations): 
        return True
    return oldCentroids.__eq__(newCentroids)
    
              
#Generate Random Data 
def fireUp(lowerBound, upperBound, maxPoints, numClusters, threshold, maxIterations):
    dataCollection = []
    for i in range(maxPoints):
        dataCollection.append(tuple([(py.random.uniform(lowerBound,upperBound)) for j in range(2)])) #2D points
    
    #print dataCollection
    #Creating Dictionaries for each of the Clusters. Since they need to operate on that    
    ListofHashtableofDataPoints = []
    
    '''Created a HashTable with the Points and Distance set to ZERO'''

    for i in range(len(dataCollection)):
        tempHashTable.update({dataCollection[i]:0}) 
    
    
    '''Deep Copy to Make as many copies as the Number of Clusters we need'''
    for i in range(numClusters):
        ListofHashtableofDataPoints.append(copy.deepcopy(tempHashTable));
         
    #print ListofHashtableofDataPoints
    
    #Get three random Centriods
    initialCentroids = random.sample(dataCollection, numClusters)
    #print "Centroids:" , initialCentroids
    
    #numClusters will automatically ensure that the size of ListofHashtableofDataPoints and initialCentroids is same.
    '''clusterList = []
    for i in range(len(ListofHashtableofDataPoints)):
        clusterList.append(Cluster(initialCentroids[i], ListofHashtableofDataPoints[i]));'''
    
    clusterList = []
    for i in range(len(ListofHashtableofDataPoints)):
        clusterList.append(Cluster(initialCentroids[i], ListofHashtableofDataPoints[i]));
        
    '''for c in clusterList:
        print c
    print "\n" '''
        
    flag = True;
    #Calling the KMeans Algorithm
    kMeansAlgo(clusterList, threshold, flag, maxIterations)
    
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

    
def main():
    lowerBound = 1 #int (raw_input("Enter the lowerBound for DataGenerationN: \n"))
    upperBound = 2 #int (raw_input("Enter the upperBound for DataGenerationN: \n"))
    maxPoints = 2 #int (raw_input("Enter the maxPoints for DataGenerationN:\n"))
    numClusters = 2 #int (raw_input("Enter the number of Clusters :\n")) 
    threshold = 0.01
    maxIterations = 2
    
    fireUp(lowerBound, upperBound, maxPoints, numClusters, threshold, maxIterations)
    
if __name__ == "__main__":
    main()
    
