from mpi4py import MPI 
import random 
import numpy as py
import copy
from numpy import matrix
from numpy import mean
import csv
import sys


comm = MPI.COMM_WORLD
machineNumber = comm.Get_rank()
sizeofCluster = comm.Get_size()
nameofMachine = MPI.Get_processor_name()

#Stuff Everyone Needs to Know about is Here
tempHashTable = {} #HashTable that has all points and distances initialized to zero.
maxPoints = 0
numClusters = 0
maxIterations = 0
threshold = 0
localclusterList=[]

globalclusterList=[] #Maybe we might need this


#This is a logical abstraction of Cluster.
class Cluster: 
    # Centriod of that Cluster
    # HashTable containing a mapping between points and the distances 
    def __init__(self, dataPoint, pointsandDistance, localdataList):
        self.centroid = dataPoint
        self.pointsandDistance = pointsandDistance
        self.localdataList = localdataList

    def getCurrentCentroid(self):
        print "Current centroid for this cluster is - ", self.centroid  
    
    def __hash__(self):
        return hash(self)
        
    def __repr__(self):
        return '%s %s' % (self.centroid, self.pointsandDistance)

def kMeansParallelAlgo():
    print "I am minion %d of %d on %s" % (machineNumber, sizeofCluster, nameofMachine)

#Generate Random Data First
def fireUp(lowerBound, upperBound, maxPoints, numClusters, threshold, maxIterations):
    dataCollection = []
    for i in range(maxPoints):
        dataCollection.append(tuple([(py.random.uniform(lowerBound,upperBound)) for j in range(2)])) #2D points
    
    '''Created a HashTable with the Points and Distance set to ZERO'''
    for i in range(len(dataCollection)):
        tempHashTable.update({dataCollection[i]:0}) 
        
    
def main():
    if(comm.Get_rank() ==0):
        while True:
            try:
                print "I am boss %d of %d on %s" % (machineNumber, sizeofCluster, nameofMachine)
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
        #fireUp(lowerBound, upperBound, maxPoints, numClusters, threshold, maxIterations)
    
    #Calling the KMeans Algorithm
    kMeansParallelAlgo()    
    
if __name__ == "__main__":
        main()