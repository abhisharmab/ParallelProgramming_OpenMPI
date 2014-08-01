from mpi4py import MPI
import numpy as np



def psum(a):
    r = MPI.COMM_WORLD.Get_rank()
    size = MPI.COMM_WORLD.Get_size()
    m = len(a) / size
    locsum = np.array([10,20])
    #np.sum(a[r*m:(r+1)*m])
    print locsum
    rcvBuf = np.array([0,0])
    MPI.COMM_WORLD.Allreduce([locsum, MPI.INT], [rcvBuf, MPI.INT], op=MPI.SUM)
    x = list(rcvBuf)
    print x[0]
    print x[1]
    return rcvBuf


N = 128
a = np.random.rand(N)
np.save("random-vector.npy", a)
a = np.load("random-vector.npy")
s = psum(a)

if MPI.COMM_WORLD.Get_rank() == 0:
    print "sum =", s, ", numpy sum =", a.sum()