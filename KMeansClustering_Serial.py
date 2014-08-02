#This is Serial KMeans Program for 2D Points 
import random 
import numpy as py
import copy
from numpy import matrix
from numpy import mean
import csv
import sys
import time 

#KMean Serialized Algorithm 
tempHashTable = {} 
#This dictionary holds all the Randomly Generated Points


#This is a logical abstraction of Cluster.
class Cluster: 
    # Centriod of that Cluster
    # HashTable containing a mapping between points and the distances 
    def __init__(self, dataPoint, pointsandDistance):
        self.centroid = dataPoint #Centroid 
        self.pointsandDistance = pointsandDistance #Dictionary containing chunk of the file

    def getCurrentCentroid(self):
        print "Current centroid for this cluster is - ", self.centroid  
        #Print the Centroid

    def __hash__(self):
        return hash(self)
        
    def __repr__(self):
        return '%s %s' % (self.centroid, self.pointsandDistance)


#Calculate Eculedian Distance 
#This functions calculates the Eculedian Distance
def calculateEculedianDistance(centroid, datapoint):
    return py.sqrt(sum((py.array(centroid) - py.array(datapoint)) ** 2))


#Check if we should stop the iterations 
#1. Check if we have exceeded the iterations 
#2. Or the Old and New Centroids are same implying - Centroids have not changed
def shouldNotStop(oldCentroids, newCentroids, iterations, Max_Iterations):
    if (iterations > Max_Iterations): 
        return True
    return oldCentroids.__eq__(newCentroids)


#This is the actual KMEANS Parallel Algorithm
def kMeansAlgo(clusterList, threshold, Max_Iterations):

    start_time = time.time()
        
    #Initialize things we need for Algos
    newCentroids = []
    for cluster in clusterList:
        newCentroids.append(copy.deepcopy(cluster.centroid))
        
    
    oldCentroids = [] 
    iterations = 0;
    
    while not shouldNotStop(oldCentroids, newCentroids, iterations, Max_Iterations):

        oldCentroids = copy.deepcopy(newCentroids)
        iterations += 1
        i=0

        #Run the code for each Cluster
        for cluster in clusterList:
            cluster.pointsandDistance = copy.deepcopy(tempHashTable)
            
        for currentCluster in clusterList:  # Iteration over each of the Cluster
            i = i + 1
            pointstoDeletefromCurrentCluster = []
            for element in currentCluster.pointsandDistance:
                    distCurrentCluster = calculateEculedianDistance(currentCluster.centroid, element)
                    currentCluster.pointsandDistance[element] = distCurrentCluster #Update Distance in HashTable
                    if(i > 1):
                        for prevCluster in clusterList[::-1]: #Iterate over Previous Lists
                            if(prevCluster.centroid == currentCluster.centroid):
                                continue
                            else:
                                if(prevCluster.pointsandDistance.has_key(element)):
                                    if(distCurrentCluster > prevCluster.pointsandDistance.get(element) and distCurrentCluster >= threshold):
                                        #print "We are futher"
                                        pointstoDeletefromCurrentCluster.append(element)
                                        break  
                                    elif(distCurrentCluster == prevCluster.pointsandDistance.get(element)):
                                        #print "We are equidistant" #Just pop from any place
                                        pointstoDeletefromCurrentCluster.append(element)
                                        break
                                    elif((distCurrentCluster > prevCluster.pointsandDistance.get(element) and distCurrentCluster <= threshold)
                                         or (distCurrentCluster < prevCluster.pointsandDistance.get(element))):
                                        #print "We are nearer or well within our threshold_limit"
                                        del prevCluster.pointsandDistance[element]
                                        
            for item in pointstoDeletefromCurrentCluster:
                del currentCluster.pointsandDistance[item]
                            
        del newCentroids[:]                    
        for cluster in clusterList:
            #Calcualte the Mean to get the new Centroid
            answer = tuple(map(mean, zip(*cluster.pointsandDistance.keys())))
            if not answer: 
                cluster.centroid = 0,0
            else:
                cluster.centroid = tuple(map(mean, zip(*cluster.pointsandDistance.keys())))
            newCentroids.append(copy.deepcopy(cluster.centroid))

    
    print("Program ran for -- %d seconds -- Done!!!" % (time.time() - start_time))        
    index = 0
    writer = csv.writer(open("KMean_Serial_Result.csv", "w"))
    for cluster in clusterList:
        index = index + 1
        for point in cluster.pointsandDistance.keys():
            #print 'Cluster_%s' % index , point
            writer.writerow(['Cluster_%s' %index , point])      
            
    
    print ""
    print "All information pushed to: KMean_Serial_Result.csv file in the same directory\n"         
                    
                    
                    
                    
#Initialze Setup for Running the Code
def fireUp(lowerBound, upperBound, maxPoints, numClusters, threshold, maxIterations):
    dataCollection = []
    for i in range(maxPoints):
        dataCollection.append(tuple([(py.random.uniform(lowerBound,upperBound)) for j in range(2)])) #2D points
    
    #Creating Dictionaries for each of the Clusters. Since they need to operate on that    
    ListofHashtableofDataPoints = []
    
    '''Created a HashTable with the Points and Distance set to ZERO'''
    for i in range(len(dataCollection)):
        tempHashTable.update({dataCollection[i]:0}) 
    
    '''Deep Copy to Make as many copies as the Number of Clusters we need'''
    for i in range(numClusters):
        ListofHashtableofDataPoints.append(copy.deepcopy(tempHashTable)); 
    
    #Get three random Centriods
    initialCentroids = random.sample(dataCollection, numClusters)

    
    #numClusters will automatically ensure that the size of ListofHashtableofDataPoints and initialCentroids is same.
    clusterList = []
    for i in range(len(ListofHashtableofDataPoints)):
        clusterList.append(Cluster(initialCentroids[i], ListofHashtableofDataPoints[i]));

    #Calling the KMeans Algorithm
    kMeansAlgo(clusterList, threshold, maxIterations)
    
    
def main():
    while True:
        try:
            print "\n"
            lowerBound = int (raw_input("Enter the lowerBound for DataGeneration (Integer only.Example = 1):\n"))
            upperBound = int (raw_input("Enter the upperBound for DataGeneration (Integer only. Example = 2):\n"))
            maxPoints = int (raw_input("Enter the maxPoints for DataGeneration (Integer only. Example = 10000):\n"))
            numClusters = int (raw_input("Enter the number of Clusters (Integer only. Example = 5):\n")) 
            threshold = float (raw_input("Enter the distance you are ready to accept as threshold from centroid (Integer or Float) . Example = 0.2):\n"))
            maxIterations = int (raw_input("Enter the maximum iterations you want to allow (Integer only). Example = 300):\n"))
            print "Processing......\n"
            break
        
        except ValueError:
            print "Oops! Invalid Entry. Please follow instructions carefully. Try again..\n"

    #Fire-Up our Working Code
    fireUp(lowerBound, upperBound, maxPoints, numClusters, threshold, maxIterations)
    
if __name__ == "__main__":
    main()
    
