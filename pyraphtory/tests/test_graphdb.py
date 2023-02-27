import sys
from pyraphtory import Graph
from pyraphtory import Direction


def create_graph(num_shards):
    g = Graph(num_shards)

    edges = [
        (1, 1, 2),
        (2, 1, 3),
        (-1, 2, 1),
        (0, 1, 1),
        (7, 3, 2),
        (1, 1, 1)
    ]

    g.add_vertex(0, 1, {"type": "wallet", "cost": 99.5})
    g.add_vertex(-1, 2, {"type": "wallet", "cost": 10.0})
    g.add_vertex(6, 3, {"type": "wallet", "cost": 76})

    for e in edges:
        g.add_edge(e[0], e[1], e[2], {"prop1": 1, "prop2": 9.8, "prop3": "test"})

    return g


def test_graph_len_edge_len():
    g = create_graph(2)

    assert g.len() == 3
    assert g.edges_len() == 5


def test_graph_contains():
    g = create_graph(2)

    assert g.contains(3)


def test_windowed_graph_contains():
    g = create_graph(2)
    
    assert g.window(-1, 1).contains(1)


def test_windowed_graph_degree():
    g = create_graph(3)

    view = g.window(0, sys.maxsize)

    indegree = view.degree(1, Direction.IN)
    outdegree = view.degree(2, Direction.OUT)
    degree = view.degree(3, Direction.BOTH)

    assert indegree == 1
    assert outdegree == 0
    assert degree == 2


def test_windowed_graph_neighbours():
    g = create_graph(1)

    view = g.window(0, sys.maxsize)

    in_neighbours = []
    for e in view.neighbours(1, Direction.IN):
        in_neighbours.append([e.src, e.dst, e.t, e.is_remote])
    assert in_neighbours == [
        [1, 1, None, False]
    ]

    out_neighbours = []
    for e in view.neighbours(2, Direction.OUT):
        out_neighbours.append([e.src, e.dst, e.t, e.is_remote])
    assert out_neighbours == []

    neighbours = []
    for e in view.neighbours(3, Direction.BOTH):
        neighbours.append([e.src, e.dst, e.t, e.is_remote])
    assert neighbours == [
        [1, 3, None, False],
        [3, 2, None, False]
    ]


def test_windowed_graph_vertex_ids():
    g = create_graph(3)

    vs = [v for v in g.window(-1, 1).vertex_ids()]
    vs.sort()

    assert vs == [1, 2]


def test_windowed_graph_vertices():
    g = create_graph(1)

    view = g.window(-1, 0)

    vertices = []
    for v in view.vertices():
        vertices.append([v.g_id, v.props])
    assert vertices == [
            [1, {}], 
            [2, {"type": [(-1, "wallet")], "cost": [(-1, 10.0)]}], 
        ]
