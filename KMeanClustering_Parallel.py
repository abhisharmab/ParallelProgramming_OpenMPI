from mpi4py import MPI 
import random 
import numpy as py
import copy
from numpy import matrix
from numpy import mean
import csv
import sys
import collections
import itertools


comm = MPI.COMM_WORLD
machineNumber = comm.Get_rank()
sizeofCluster = comm.Get_size()
nameofMachine = MPI.Get_processor_name()
MPI_GOD = 0

#Stuff Everyone Needs to Know about is Here
tempHashTable = collections.OrderedDict() #HashTable that has all points and distances initialized to zero.
maxPoints = 0
numClusters = 0
maxIterations = 0
threshold = 0
initialCentroids=[]
localclusterList=[]
#localDataHashtable={}
globalclusterList=[] #Maybe we might need this


#This is a logical abstraction of Cluster.
class Cluster: 
    # Centriod of that Cluster
    # HashTable containing a mapping between points and the distances 
    def __init__(self, dataPoint, pointsandDistance, maxIterations):
        self.centroid = dataPoint
        self.pointsandDistance = pointsandDistance
        self.maxIterations = maxIterations

    def getCurrentCentroid(self):
        print "Current centroid for this cluster is - ", self.centroid  
    
    def __hash__(self):
        return hash(self)
        
    def __repr__(self):
        return '%s %s' % (self.centroid, self.pointsandDistance)


#Calculate Eculedian Distance 
def calculateEculedianDistance(centroid, datapoint):
    return py.sqrt(sum((py.array(centroid) - py.array(datapoint)) ** 2))


def shouldNotStop(oldCentroids, newCentroids, iterations, Max_Iterations):
    if (iterations > Max_Iterations): 
        return True
    #print oldCentroids 
    #print newCentroids
    return oldCentroids.__eq__(newCentroids)


'''System Initialization'''
def initializeDistributedSystem(initialCentroids, maxIterations):
    if(comm.Get_rank() == 0):
        #print "System Initializing. Please wait....."
        #Send the Data to Minions and Also Take Up Some Work
        slices = len(tempHashTable) / sizeofCluster
        print "Total Slices" , slices
        print "MaxIterations" , maxIterations
        localDataHashtable = dict(itertools.islice(tempHashTable.iteritems(), 0, slices)) #Slice the Dictionary
        #print localDataHashtable
        
        #Make local copy of those centroids
        centroids = initialCentroids
        
        for i in range(len(initialCentroids)):
            localclusterList.append(Cluster(centroids[i], copy.deepcopy(localDataHashtable), maxIterations))
        print "Boss Cluster" , localclusterList
      
        for i in range(1,sizeofCluster):
            minionData = collections.OrderedDict()
            
            if(i < sizeofCluster - 1):
                minionData = dict(itertools.islice(tempHashTable.iteritems(), slices * i , slices * (i + 1)))
                #print minionData
            else:
                minionData = dict(itertools.islice(tempHashTable.iteritems(), slices * i , len(tempHashTable)))
                #print minionData
                
            try:
                comm.send(minionData, dest=i, tag=11)
                comm.send(centroids, dest=i, tag=11)
                comm.send(maxIterations, dest=i, tag=11)
            except Exception:
                print "We got exception due to MPI. Following is the error: \n"
                print str(Exception.message)
                sys.exit(0)
                
    else:
        #Receive the Data From Master
        localDataHashtable = comm.recv(source=0, tag=11)
        centroids = comm.recv(source=0, tag=11)
        maxIterations = comm.recv(source=0, tag=11)
        
        for i in range(len(centroids)):
            localclusterList.append(Cluster(centroids[i], copy.deepcopy(localDataHashtable), maxIterations))
            
        print "Minion Cluster- %d" %machineNumber , localclusterList
        
        

'''K-MEANS Parallel Algorithm'''
def kMeansParallelAlgo(initialCentroids, maxIterations):
    
    initialCentroids = initialCentroids
    initializeDistributedSystem(initialCentroids, maxIterations)
    comm.barrier() #Waiting for everyone to sync-up
    
    localDataHashtable = copy.deepcopy(localclusterList[0].pointsandDistance)
    #print localDataHashtable
    
    #Initialize things we need for Algos
    newCentroids = []
    for cluster in localclusterList:
        newCentroids.append(copy.deepcopy(cluster.centroid))
        
    #print "NewCentroids at beginning" , newCentroids
    oldCentroids = [] 
    iterations = 0;
    
    while not shouldNotStop(oldCentroids, newCentroids, iterations, localclusterList[0].maxIterations):
        
        oldCentroids = copy.deepcopy(newCentroids)
        iterations += 1
        i=0
        #if(i==0):
            #print "OldCentroids at beginning" , oldCentroids
    
        for cluster in localclusterList:
            cluster.pointsandDistance = copy.deepcopy(localDataHashtable)
            
        for currentCluster in localclusterList:  # Iteration over each of the Cluster
            i = i + 1
            pointstoDeletefromCurrentCluster = []
            for element in currentCluster.pointsandDistance:
                #if(element != currentCluster.centroid):           
                    distCurrentCluster = calculateEculedianDistance(currentCluster.centroid, element)
                    currentCluster.pointsandDistance[element] = distCurrentCluster #Update Distance in HashTable
                    if(i > 1):
                        #print i
                        for prevCluster in localclusterList[::-1]: #Iterate over Previous Lists
                            if(prevCluster.centroid == currentCluster.centroid):
                                continue
                            else:
                                #print "prev" , prevCluster.centroid, prevCluster.pointsandDistance
                                #print "curr" , currentCluster.centroid, currentCluster.pointsandDistance
                                #print "keyToSearchinPrev" , element
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
                                        #print distCurrentCluster
                                        #print prevCluster.pointsandDistance[element]
                                        del prevCluster.pointsandDistance[element]
                                        
            for item in pointstoDeletefromCurrentCluster:
                del currentCluster.pointsandDistance[item]
                            
        del newCentroids[:]     
                       
        xcordList =[]
        ycordList =[]
        numPoints =[]
        finalnumPoints = 0
        for cluster in localclusterList:
            #xcordList.append(sum([key[0] for key in cluster.pointsandDistance.keys()]))
            #print "Machine_%d" %machineNumber, xcordList
            #ycordList.append(sum([key[1] for key in cluster.pointsandDistance.keys()]))
            #print "Machine_%d" %machineNumber, ycordList
            numPoints.append(len(cluster.pointsandDistance))
            
        print "Machine_%d" %machineNumber, numPoints[0]  
        try:
            #comm.allreduce([xcordList,MPI.DOUBLE], [len(localclusterList),MPI.DOUBLE], op=MPI.SUM);
            #comm.allreduce([ycordList,MPI.DOUBLE], [len(localclusterList),MPI.DOUBLE], op=MPI.SUM);
            comm.allreduce([numPoints[0], MPI.INT], None, op=MPI.SUM);

            
            #print xcordList[0]
            #print ycordList[0]
            print "Machine_%d" %machineNumber, numPoints[0]
        except Exception:
            print "MPI Communication Exception"
            
    
       
        '''x = 0    
        if(x==0):   
            print xcordList
            print ycordList
            print numPoints
            x = x + 1'''
        
        sys.exit(0)
        #print oldCentroids
        #print newCentroids'''



'''Fire_Up Function'''
#Generate Random Data First
def fireUp(lowerBound, upperBound, maxPoints, numClusters, threshold, maxIterations):
    dataCollection = []
    for i in range(maxPoints):
        dataCollection.append(tuple([(py.random.uniform(lowerBound,upperBound)) for j in range(2)])) #2D points
    
    #Created a HashTable with the Points and Distance set to ZERO
    for i in range(len(dataCollection)):
        tempHashTable.update({dataCollection[i]:0}) 
    #print tempHashTable 
    
    #Get three random Centriods
    initialCentroids = random.sample(dataCollection, numClusters)
    return initialCentroids

'''Main Function for User_Input'''
def main():
    initialCentroids=[]
    maxIterations=0
    if(comm.Get_rank() == 0):
        while True:
            try:
                #print "I am boss %d of %d on %s" % (machineNumber, sizeofCluster, nameofMachine)
                lowerBound = 1 #int (raw_input("Enter the lowerBound for DataGeneration (Integer only): \n"))
                upperBound = 2 #int (raw_input("Enter the upperBound for DataGeneration (Integer only): \n"))
                maxPoints = 6 #int (raw_input("Enter the maxPoints for DataGeneration (Integer only):\n"))
                numClusters = 2 #int (raw_input("Enter the number of Clusters (Integer only):\n")) 
                threshold = 0.2 #float (raw_input("Enter the distance you are ready to accept as threshold from centroid (Integer or Float):\n"))
                maxIterations = 20 #int (raw_input("Enter the maximum iterations you want to allow (Integer only):\n"))
                break
        
            except ValueError:
                print "Oops! Invalid Entry. Please follow instructions carefully. Try again..\n"
    
        #Fire-Up our Working Code
        initialCentroids = fireUp(lowerBound, upperBound, maxPoints, numClusters, threshold, maxIterations)
    
    #Calling the KMeans Algorithm
    kMeansParallelAlgo(initialCentroids,maxIterations)    
    
if __name__ == "__main__":
        main()