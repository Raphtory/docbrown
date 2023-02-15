import pyraphtory

g = pyraphtory.GraphDB.load_from_file("resources/test/graphdb.bincode")

gandalf = 13840129630991083248
indegree = g.degree(gandalf, pyraphtory.Direction.IN)

print(indegree)

for v in g.vertices():
    print(v)
