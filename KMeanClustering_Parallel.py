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
from itertools import chain
import time 

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
        #print "Total Slices" , slices
        #print "MaxIterations" , maxIterations
        localDataHashtable = dict(itertools.islice(tempHashTable.iteritems(), 0, slices)) #Slice the Dictionary
        #print localDataHashtable
        
        #Make local copy of those centroids
        centroids = initialCentroids
        
        for i in range(len(initialCentroids)):
            localclusterList.append(Cluster(centroids[i], copy.deepcopy(localDataHashtable), maxIterations))
        #print "Boss Cluster" , localclusterList
      
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
        #print "Minion Cluster- %d" %machineNumber , localclusterList
        
        

'''K-MEANS Parallel Algorithm'''
def kMeansParallelAlgo(initialCentroids, maxIterations):
    if(comm.Get_rank() == 0):
        start_time = time.time()
    
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
                       
        finalnumPointsAddded = py.array([0])
        finalxcordAdded = py.array([0.0])
        finalycordAdded = py.array([0.0])
      
        for cluster in localclusterList:
            try:
                xcordList = py.array([(sum([key[0] for key in cluster.pointsandDistance.keys()]))])
                #print "Machine_%d" %machineNumber, xcordList
            
                ycordList = py.array([(sum([key[1] for key in cluster.pointsandDistance.keys()]))])
                #print "Machine_%d" %machineNumber, ycordList
            
                numPoints =  py.array([(len(cluster.pointsandDistance))])
                #print "Machine_%d" %machineNumber, numPoints 
                
                comm.Allreduce([numPoints, MPI.INT], [finalnumPointsAddded, MPI.INT], op=MPI.SUM);
                #print "Machine_%d" %machineNumber, finalnumPointsAddded
                
                comm.Allreduce([xcordList, MPI.DOUBLE], [finalxcordAdded, MPI.DOUBLE], op=MPI.SUM);
                #print "Machine_%d" %machineNumber, finalxcordAdded
                
                comm.Allreduce([ycordList, MPI.DOUBLE], [finalycordAdded, MPI.DOUBLE], op=MPI.SUM);
                #print "Machine_%d" %machineNumber, finalycordAdded
                
                if(finalnumPointsAddded != 0):
                    x = (finalxcordAdded/finalnumPointsAddded)
                    y = (finalycordAdded/finalnumPointsAddded)
                    cluster.centroid = x[0], y[0]
                else:
                    cluster.centroid = 0,0
                
                newCentroids.append(cluster.centroid)
                #print cluster.centroid
                
            except:
                print "MPI Communication Exception"

    for cluster in localclusterList:
        if(comm.Get_rank() == 0): # You are ROOT you must aggregate all results
            for i in range(1,sizeofCluster):
                tempclusterHashTable = comm.recv(source=i, tag=12)
                cluster.pointsandDistance = dict(chain.from_iterable(d.iteritems() for d in (cluster.pointsandDistance,tempclusterHashTable)))
                
        else: #You are slave and must send all information to Boss
            comm.send(cluster.pointsandDistance, dest = 0, tag = 12) 
            
    if(comm.Get_rank() == 0):
        index = 0    
        writer = csv.writer(open("KMean_Parallel_Result.csv", "w"))
        for cluster in localclusterList:
            index = index + 1
            for point in cluster.pointsandDistance.keys():
                print 'Cluster_%s' % index , point
                writer.writerow(['Cluster_%s' %index , point]) 
            print "\n"    
            
        print("Program ran for -- %d seconds --" % (time.time() - start_time))
        print "Done!!! All information pushed to a KMean_Parallel_Result.csv file in the same directory"          
    
    sys.exit(0)


'''Fire_Up Function'''
#Generate Random Data First
def fireUp(lowerBound, upperBound, maxPoints, numClusters, threshold, maxIterations):
    dataCollection = []
    for i in range(maxPoints):
        dataCollection.append(tuple([(py.random.uniform(lowerBound,upperBound)) for j in range(2)])) #2D points
    #print dataCollection
    
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
                lowerBound = int (raw_input("Enter the lowerBound for DataGeneration (Integer only): \n"))
                upperBound = int (raw_input("Enter the upperBound for DataGeneration (Integer only): \n"))
                maxPoints = int (raw_input("Enter the maxPoints for DataGeneration (Integer only):\n"))
                numClusters = int (raw_input("Enter the number of Clusters (Integer only):\n")) 
                threshold = float (raw_input("Enter the distance you are ready to accept as threshold from centroid (Integer or Float):\n"))
                maxIterations = int (raw_input("Enter the maximum iterations you want to allow (Integer only):\n"))
                break
        
            except ValueError:
                print "Oops! Invalid Entry. Please follow instructions carefully. Try again..\n"
    
        #Fire-Up our Working Code
        initialCentroids = fireUp(lowerBound, upperBound, maxPoints, numClusters, threshold, maxIterations)
    
    #Calling the KMeans Algorithm
    kMeansParallelAlgo(initialCentroids,maxIterations)    
    
if __name__ == "__main__":
        main()