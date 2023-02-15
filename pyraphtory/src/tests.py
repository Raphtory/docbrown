import pyraphtory
from pyraphtory import Direction, TEdge

g = pyraphtory.GraphDB.load_from_file("resources/test/graphdb.bincode")

gandalf = 13840129630991083248
indegree = g.degree(gandalf, pyraphtory.Direction.IN)

print(indegree)

for v in g.vertices():
    print(v)

for e in g.neighbours(13840129630991083248, Direction.IN):
    print(e.src, e.dst, e.t, e.is_remote)
