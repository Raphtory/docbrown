import sys
import pyraphtory
from pyraphtory import Direction

gandalf = 13840129630991083248

g = pyraphtory.GraphDB.load_from_file("resources/test/graphdb.bincode")

print(f"Graph length = {g.len()}")
print(f"Graph edge length = {g.edges_len()}")
print(f"Gandalf exists = {g.contains(gandalf)}")

indegree = g.degree(gandalf, pyraphtory.Direction.IN)
outdegree = g.degree(gandalf, pyraphtory.Direction.OUT)
degree = g.degree(gandalf, pyraphtory.Direction.BOTH)
print(
    f"Gandalf has {indegree} in-degree, {outdegree} out-degree and {degree} total degree")

indegree_w = g.degree_window(gandalf, 0, sys.maxsize, pyraphtory.Direction.IN)
outdegree_w = g.degree_window(gandalf, 0, sys.maxsize, pyraphtory.Direction.OUT)
degree_w = g.degree_window(gandalf, 0, sys.maxsize, pyraphtory.Direction.BOTH)
print(f"Gandalf has {indegree_w} windowed in-degree, {outdegree_w} windowed out-degree and {degree_w} total degree")

print("\nGandalf's windowed outbound neighbours")
for e in g.neighbours_window(gandalf, 0, sys.maxsize, Direction.OUT):
    print(
        f"TEdge {{ src: {e.src}, dst: {e.dst}, t: Some({e.t}), is_remote: {e.is_remote} }}")

print("\nGandalf's outbound neighbours")
for e in g.neighbours(gandalf, Direction.OUT):
    print(
        f"TEdge {{ src: {e.src}, dst: {e.dst}, t: Some({e.t}), is_remote: {e.is_remote} }}")

print("\nGandalf's windowed outbound neighbours with timestamp")
for e in g.neighbours_window_t(gandalf, 0, sys.maxsize, Direction.OUT):
    print(
        f"TEdge {{ src: {e.src}, dst: {e.dst}, t: Some({e.t}), is_remote: {e.is_remote} }}")

print("\n All Vertices")
for v in g.vertices():
    print(v)
