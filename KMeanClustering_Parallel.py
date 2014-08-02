#This is Parallel KMeans Program for 2D Points 
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

#Initialize the the MPI Related Variables 
comm = MPI.COMM_WORLD
machineNumber = comm.Get_rank()
sizeofCluster = comm.Get_size()
nameofMachine = MPI.Get_processor_name()
MPI_GOD = 0

#Stuff that methods  Needs to Know about is Here
#HashTable that has all points and distances initialized to zero.
tempHashTable = collections.OrderedDict() #This is actually a HashTable of all the points
maxPoints = 0
numClusters = 0
maxIterations = 0
threshold = 0
initialCentroids=[]
localclusterList=[]



#This is a logical abstraction of Cluster.
#This contains the Centroid, A Dictionary of all the points given to this Machine, And Maximum Iterations Allowed
class Cluster: 
    # Centriod of that Cluster
    # HashTable containing a mapping between points and the distances 
    def __init__(self, dataPoint, pointsandDistance, maxIterations):
        self.centroid = dataPoint #Centroid 
        self.pointsandDistance = pointsandDistance #Dictionary containing chunk of the file
        self.maxIterations = maxIterations #Iterations

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


#This fucntions initiales the Distributed System 
#Breaks the Data into chunks and sends it across to all the Machines 
'''System Initialization'''
def initializeDistributedSystem(initialCentroids, maxIterations):
    if(comm.Get_rank() == 0): #Rank =0 implies MASTER NODE
        #Send the Data to Minions and Also Take Up Some Work
        slices = len(tempHashTable) / sizeofCluster
        
        localDataHashtable = dict(itertools.islice(tempHashTable.iteritems(), 0, slices)) #Slice the Dictionary
        
        #Make local copy of those centroids
        centroids = initialCentroids
        
        for i in range(len(initialCentroids)):
            localclusterList.append(Cluster(centroids[i], copy.deepcopy(localDataHashtable), maxIterations))
        
      
        for i in range(1,sizeofCluster): #Make as many chunks as the number of Clusters
            minionData = collections.OrderedDict()
            
            if(i < sizeofCluster - 1):
                minionData = dict(itertools.islice(tempHashTable.iteritems(), slices * i , slices * (i + 1)))
                
            else:
                minionData = dict(itertools.islice(tempHashTable.iteritems(), slices * i , len(tempHashTable)))
                
                
            try:
                comm.send(minionData, dest=i, tag=11) #Send the Data Chunks
                comm.send(centroids, dest=i, tag=11)  #Send the Centroids 
                comm.send(maxIterations, dest=i, tag=11) #Send the MaxIterations we should run

            except Exception:
                print "We got exception due to MPI. Following is the error: \n"
                print str(Exception.message)
                sys.exit(0)
                
    else: #These are Minions doing the work.
        #Receive the Data From Master
        localDataHashtable = comm.recv(source=0, tag=11)
        centroids = comm.recv(source=0, tag=11)
        maxIterations = comm.recv(source=0, tag=11)
        
        for i in range(len(centroids)):
            localclusterList.append(Cluster(centroids[i], copy.deepcopy(localDataHashtable), maxIterations))
        print "Minion Node- %d on %s has Received Data and Processing...." % (machineNumber, nameofMachine)
        
        

#This is the actual KMEANS Parallel Algorithms 
'''K-MEANS Parallel Algorithm'''
def kMeansParallelAlgo(initialCentroids, maxIterations):
    if(comm.Get_rank() == 0):
        start_time = time.time()
    
    initialCentroids = initialCentroids
    initializeDistributedSystem(initialCentroids, maxIterations)
    
    comm.barrier() #Waiting for everyone to sync-up until this point 
    
    localDataHashtable = copy.deepcopy(localclusterList[0].pointsandDistance)
    
    
    #Initialize things we need for the Algorithm
    newCentroids = []
    for cluster in localclusterList:
        newCentroids.append(copy.deepcopy(cluster.centroid))
        
    
    oldCentroids = [] 
    iterations = 0;
    
    while not shouldNotStop(oldCentroids, newCentroids, iterations, localclusterList[0].maxIterations):
        
        oldCentroids = copy.deepcopy(newCentroids)
        iterations += 1
        i=0
    
        #Run the code for each Cluster
        for cluster in localclusterList: 
            cluster.pointsandDistance = copy.deepcopy(localDataHashtable)
            
        for currentCluster in localclusterList:  # Iteration over each of the Cluster
            i = i + 1
            pointstoDeletefromCurrentCluster = []
            for element in currentCluster.pointsandDistance:
                    distCurrentCluster = calculateEculedianDistance(currentCluster.centroid, element)
                    currentCluster.pointsandDistance[element] = distCurrentCluster #Update Distance in HashTable
                    if(i > 1):
                        #Check the previous Clusters. Maybe the point is closer to other Cluster 
                        for prevCluster in localclusterList[::-1]: #Iterate over Previous Lists
                            if(prevCluster.centroid == currentCluster.centroid):
                                continue
                            else:

                                if(prevCluster.pointsandDistance.has_key(element)):
                                    if(distCurrentCluster > prevCluster.pointsandDistance.get(element) and distCurrentCluster >= threshold):
                                        #print "We are futher away"
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
                       
        finalnumPointsAddded = py.array([0])
        finalxcordAdded = py.array([0.0])
        finalycordAdded = py.array([0.0])
      
        for cluster in localclusterList:
            try:
                xcordList = py.array([(sum([key[0] for key in cluster.pointsandDistance.keys()]))])
                #Add up all the x-coordinates
            
                ycordList = py.array([(sum([key[1] for key in cluster.pointsandDistance.keys()]))])
                #Add up all the y-cordinates
            
                numPoints =  py.array([(len(cluster.pointsandDistance))])
                #The total points in that local cluster
                
                comm.Allreduce([numPoints, MPI.INT], [finalnumPointsAddded, MPI.INT], op=MPI.SUM);
                #Add all the X-coordinates across all nodes
                
                comm.Allreduce([xcordList, MPI.DOUBLE], [finalxcordAdded, MPI.DOUBLE], op=MPI.SUM);
                #Add all the Y-coordinates across all nodes
                
                comm.Allreduce([ycordList, MPI.DOUBLE], [finalycordAdded, MPI.DOUBLE], op=MPI.SUM);
                #Get the total points
                
                if(finalnumPointsAddded != 0):
                    x = (finalxcordAdded/finalnumPointsAddded)
                    y = (finalycordAdded/finalnumPointsAddded)
                    cluster.centroid = x[0], y[0]
                else:
                    cluster.centroid = 0,0 #If no points remain in cluster then set to zero
                
                #Updates New Centroid 
                newCentroids.append(cluster.centroid)
                
                
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
        print "\n" 
        print("Program ran for -- %d seconds -- Done!!!" % (time.time() - start_time))
        index = 0    
        writer = csv.writer(open("KMean_Parallel_Result.csv", "w"))
        for cluster in localclusterList:
            index = index + 1
            for point in cluster.pointsandDistance.keys():
                writer.writerow(['Cluster_%s' %index , point]) 
                #Write Data to Excel File
              
        
        print ""
        print "All information pushed to: KMean_Parallel_Result.csv file in the same directory\n"          
    
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
                print "\nMaster %d of %d nodes running on %s \n" % (machineNumber, sizeofCluster, nameofMachine)
                lowerBound = int (raw_input("Enter the lowerBound for DataGeneration (Integer only.Example = 1):\n"))
                upperBound = int (raw_input("Enter the upperBound for DataGeneration (Integer only. Example = 2):\n"))
                maxPoints = int (raw_input("Enter the maxPoints for DataGeneration (Integer only. Example = 10000):\n"))
                numClusters = int (raw_input("Enter the number of Clusters (Integer only. Example = 5):\n")) 
                threshold = float (raw_input("Enter the distance you are ready to accept as threshold from centroid (Integer or Float) . Example = 0.2):\n"))
                maxIterations = int (raw_input("Enter the maximum iterations you want to allow (Integer only). Example = 300):\n"))
                print "\n"
                break
        
            except ValueError:
                print "Oops! Invalid Entry. Please follow instructions carefully. Try again..\n"
    
        #Fire-Up our Working Code
        initialCentroids = fireUp(lowerBound, upperBound, maxPoints, numClusters, threshold, maxIterations)
    
    #Calling the KMeans Algorithm
    kMeansParallelAlgo(initialCentroids,maxIterations)    
    
if __name__ == "__main__":
        main()