from pyraphtory import TemporalGraph
from time import time
import dill as pickle
graph = TemporalGraph()
start_outer = time()
start_inner = time()
count = 0
with open("/Users/bensteer/Documents/alphabay_sorted.csv","r") as file:
     for line in file:
        count+=1
        if(count%1000000==0):
            end = time()
            timetaken = end - start_inner
            print("Lines processed:"+str(count)+" in "+str(timetaken)+" seconds")
            start_inner = time()
        cols = line.split(",")
        src = int(cols[3])
        dst = int(cols[4])
        timestamp = int(cols[5])
        graph.add_vertex(src,timestamp)
        graph.add_vertex(src,timestamp)
        graph.add_edge(src,dst,timestamp)

timetaken =time() - start_outer
print("Time taken:"+str(timetaken))
start = time()
print("Out degree of 107055: "+str(graph.outbound_degree(107055)))
timetaken =time() - start
print("Time taken for degree check:"+str(timetaken))

#file = open('pickle.p', 'wb')
#pickle.dump(graph, file)
#file.close()