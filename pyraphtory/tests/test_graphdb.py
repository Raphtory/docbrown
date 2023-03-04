import sys
from pyraphtory import Graph

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


def test_graph_has_vertex():
    g = create_graph(2)

    assert g.has_vertex(3)


def test_windowed_graph_has_vertex():
    g = create_graph(2)
    
    assert g.window(-1, 1).has_vertex(1)


def test_windowed_graph_get_vertex():
    g = create_graph(2)
    
    view = g.window(0, sys.maxsize)

    assert view.vertex(1).g_id == 1
    assert view.vertex(10) == None
    assert view.vertex(1).degree() == 3


def test_windowed_graph_degree():
    g = create_graph(3)

    view = g.window(0, sys.maxsize)

    degrees = [v.degree() for v in view.vertices()]
    degrees.sort()

    assert degrees == [2, 2, 3]

    in_degrees = [v.in_degree() for v in view.vertices()]
    in_degrees.sort()

    assert in_degrees == [1, 1, 2]

    out_degrees = [v.out_degree() for v in view.vertices()]
    out_degrees.sort()

    assert out_degrees == [0, 1, 3]


def test_windowed_graph_get_edge():
    g = create_graph(2)
    
    max_size = sys.maxsize
    min_size = -sys.maxsize - 1

    view = g.window(min_size, max_size)

    assert (view.edge(1, 3).src, view.edge(1, 3).dst) == (1, 3)
    assert (view.edge(2, 3).src, view.edge(2, 3).dst) == (3, 2)
    assert view.edge(6, 5) == None

    assert (view.vertex(1).g_id, view.vertex(3).g_id) == (1, 3)

    view = g.window(2, 3)
    assert (view.edge(1, 3).src, view.edge(1, 3).dst) == (1, 3)

    view = g.window(3, 7)
    assert view.edge(1, 3) == None


def test_windowed_graph_edges():
    g = create_graph(1)

    view = g.window(0, sys.maxsize)

    tedges = [v.edges() for v in view.vertices()]
    edges = []
    for e_iter in tedges:
        for e in e_iter:
            edges.append([e.src, e.dst, e.t, e.is_remote])

    assert edges == [
            [1, 1, None, False], 
            [1, 1, None, False], 
            [1, 2, None, False], 
            [1, 3, None, False], 
            [1, 2, None, False], 
            [3, 2, None, False], 
            [1, 3, None, False], 
            [3, 2, None, False]
        ]

    tedges = [v.in_edges() for v in view.vertices()]
    in_edges = []
    for e_iter in tedges:
        for e in e_iter:
            in_edges.append([e.src, e.dst, e.t, e.is_remote])

    assert in_edges == [
            [1, 1, None, False], 
            [1, 2, None, False], 
            [3, 2, None, False], 
            [1, 3, None, False]
        ]
    
    tedges = [v.out_edges() for v in view.vertices()]
    out_edges = []
    for e_iter in tedges:
        for e in e_iter:
            out_edges.append([e.src, e.dst, e.t, e.is_remote])

    assert out_edges == [
            [1, 1, None, False], 
            [1, 2, None, False], 
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
        vertices.append(v.g_id)

    assert vertices == [1, 2]


def test_windowed_graph_neighbours():
    g = create_graph(1)

    max_size = sys.maxsize
    min_size = -sys.maxsize - 1

    view = g.window(min_size, max_size)

    vertices_w = [v.neighbours() for v in view.vertices()]
    neighbours = []
    for v_iter in vertices_w:
        neighbours.append([v.g_id for v in v_iter])

    assert neighbours == [[1, 2, 1, 2, 3], [1, 3, 1], [1, 2]]

    vertices_w = [v.in_neighbours() for v in view.vertices()]
    in_neighbours = []
    for v_iter in vertices_w:
        in_neighbours.append([v.g_id for v in v_iter])

    assert in_neighbours == [[1, 2], [1, 3], [1]]
    
    vertices_w = [v.out_neighbours() for v in view.vertices()]
    out_neighbours = []
    for v_iter in vertices_w:
        out_neighbours.append([v.g_id for v in v_iter])

    assert out_neighbours == [[1, 2, 3], [1], [2]]


def test_windowed_graph_neighbours_ids():
    g = create_graph(1)

    max_size = sys.maxsize
    min_size = -sys.maxsize - 1

    view = g.window(min_size, max_size)

    vertices_w = [v.neighbours_ids() for v in view.vertices()]
    neighbours_ids = []
    for v_iter in vertices_w:
        neighbours_ids.append([v for v in v_iter])

    assert neighbours_ids == [[1, 2, 1, 2, 3], [1, 3, 1], [1, 2]]

    vertices_w = [v.in_neighbours_ids() for v in view.vertices()]
    in_neighbours_ids = []
    for v_iter in vertices_w:
        in_neighbours_ids.append([v for v in v_iter])

    assert in_neighbours_ids == [[1, 2], [1, 3], [1]]
    
    vertices_w = [v.out_neighbours_ids() for v in view.vertices()]
    out_neighbours_ids = []
    for v_iter in vertices_w:
        out_neighbours_ids.append([v for v in v_iter])

    assert out_neighbours_ids == [[1, 2, 3], [1], [2]]


def test_windowed_graph_vertex_prop():
    g = create_graph(1)

    max_size = sys.maxsize
    min_size = -sys.maxsize - 1

    view = g.window(min_size, max_size)

    assert view.vertex(1).prop("type") == [(0, 'wallet')]
    assert view.vertex(1).prop("undefined") == []


def test_windowed_graph_vertex_props():
    g = create_graph(1)

    max_size = sys.maxsize
    min_size = -sys.maxsize - 1

    view = g.window(min_size, max_size)

    assert view.vertex(1).props() == {'cost': [(0, 99.5)], 'type': [(0, 'wallet')]}


def test_windowed_graph_edge_prop():
    g = create_graph(1)

    max_size = sys.maxsize
    min_size = -sys.maxsize - 1

    view = g.window(min_size, max_size)

    edge = next(view.vertex(1).edges())

    assert edge.prop("prop1") == [(0, 1), (1, 1)]
    assert edge.prop("prop3") == [(0, 'test'), (1, 'test')]
    assert edge.prop("undefined") == []
