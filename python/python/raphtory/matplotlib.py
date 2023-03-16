import numpy as np

def draw_nodes(
            graph, 
            node_position,
            node_size=100,
            node_color="#000000",
            node_shape="o",
            cmap=None,
            vmin=None,
            vmax=None,
            alpha=None,
            linewidths=None,
            edgecolors=None, 
            label=None
            ):
      try:
        import matplotlib as mpl
        import matplotlib.collections  # call as mpl.collections
        import matplotlib.pyplot as plt
      except ImportError as e:
        raise ImportError(
            "Matplotlib is required. Please use 'pip install matplotlib' to install."
        ) from e
      
      node_list = graph.vertices()

      if len(node_list) == 0:
        return mpl.collections.PathCollection(None)
      
      ax = plt.gca()
     
      xy = np.asarray([node_position[v] for v in node_list])
     
      node_collection = ax.scatter(
            xy[:, 0],
            xy[:, 1],
            s=node_size,
            c=node_color,
            marker=node_shape,
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            alpha=alpha,
            linewidths=linewidths,
            edgecolors=edgecolors,
            label=label,
      )
      
      ax.tick_params(
            axis="both",
            which="both",
            bottom=False,
            left=False,
            labelbottom=False,
            labelleft=False,
      )

      return node_collection

def draw_edges(
        graph,
        edge_position,
        arrow_color,
        width=1.0
):
      try:
        import matplotlib as mpl
        import matplotlib.colors  # call as mpl.colors
        import matplotlib.patches  # call as mpl.patches
        import matplotlib.path  # call as mpl.path
        import matplotlib.pyplot as plt
      except ImportError as e:
        raise ImportError(
            "Matplotlib needs installing, use 'pip install matplotlib' to install."
        ) from e

      ax = plt.gca()
      edge_list = graph.edge_list()

      if len(edge_list) == 0:
          return []
      arrow_collection = []

      node_list = list(graph.node_indices())

      edge_pos = np.asarray([(edge_position[e[0]], edge_position[e[1]]) for e in edge_list])

      for i, (src, dst) in enumerate(edge_pos):
          x, y = src
          dx, dy = dst

      arrow = mpl.patches.Arrow(
          x,
          y,
          dx,
          dy,
          width,
          color=arrow_color
      )

      arrow_collection.append(arrow)
        
      ax.add_patch(arrow)

      ax.tick_params(
        axis="both",
        which="both",
        bottom=False,
        left=False,
        labelbottom=False,
        labelleft=False,
      )

      return arrow_collection

def draw_graph(
        graph,
        position=None,
        arrows = True,
        **kwds
):
      try:
            import matplotlib.pyplot as plt
      except ImportError as e:
            raise ImportError(
                  "Please install matplotlib - You can install it with pip install matplotlib."
            ) from e

      valid_node_kwds = {
        "node_size",
        "node_color",
        "node_shape",
        "cmap",
        "vmin",
        "vmax",
        "linewidths",
        "edgecolors",
        "label",
      }

      valid_edge_kwds = {
        "arrow_color",
        "width",
      }

      valid_kwds = valid_node_kwds | valid_edge_kwds 

      if any([k not in valid_kwds for k in kwds]):
        invalid_args = ", ".join([k for k in kwds if k not in valid_kwds])
        raise ValueError(f"Received invalid argument(s): {invalid_args}")

      node_kwds = {k: v for k, v in kwds.items() if k in valid_node_kwds}
      edge_kwds = {k: v for k, v in kwds.items() if k in valid_edge_kwds}

      draw_nodes(graph, position, **node_kwds)
      draw_edges(graph, position, arrows=arrows, **edge_kwds)

      plt.draw_if_interactive()

def matplot_draw(graph, 
                 position=None, 
                 arrows=True, 
                 **kwds
                 ):

      try:
            import matplotlib.pyplot as plt
      except ImportError as e:
            raise ImportError(
            "Matplotlib needs to be installed with 'pip install matplotlib'."
            ) from e
    

      cf = plt.gcf()
      cf.set_facecolor("w")

      draw_graph(graph, position=position, arrows=arrows, **kwds)
      plt.draw_if_interactive()

      if not plt.isinteractive():
        return cf
