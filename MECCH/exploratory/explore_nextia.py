import dgl
from pathlib import Path
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
import torch as th

# %%
def printStatsG(graphName, graph_param):
    print(f"-------------{graphName.upper()}-------------")
    print(f"Number of nodes: {graph_param.number_of_nodes()}")
    print(f"Number of edges: {graph_param.number_of_edges()}")
    print(f"Node types: {graph_param.ntypes} ")

def graph_stats(graphName, g):
    print(f"-------------{graphName.upper()}-------------")
    # stats
    print(f"Number of nodes: {g.number_of_nodes()}")
    for ntype in g.ntypes:
        print("Number of nodes in type '{}': {}".format(ntype, g.number_of_nodes(ntype)))

    # Print the number of edges per type
    for etype in g.etypes:
        print("Number of edges in type '{}': {}".format(etype, g.number_of_edges(etype)))

def print_edges_with_node_ids(graph, graphName):
    print(f"-------------{graphName.upper()} EDGES-------------")
    for etype in graph.canonical_etypes:
        src_nodes, dst_nodes = graph.edges(etype=etype)
        src_ids = src_nodes.tolist()
        dst_ids = dst_nodes.tolist()
        # Retrieve node features
        src_features = graph.nodes[etype[0]].data['x'][src_ids]
        dst_features = graph.nodes[etype[2]].data['x'][dst_ids]
        distinct_values = set()
        for src, dst, src_feat, dst_feat in zip(src_ids, dst_ids, src_features, dst_features):
            if etype[0] == "ENTITY" and etype[2] == "ENTITY" :
                # print(f"Edge: {etype[0]}{src} -{etype[1]}-> {etype[2]}{dst}")
                # print(f"Source Node Features: {src_feat}")
                # print(f"Destination Node Features: {dst_feat}")
                distinct_values.add(src_feat.item())
                distinct_values.add(dst_feat.item())

        print(f"Distinct entity ids for edge {etype[1]} and len {len(distinct_values)} : {distinct_values}")

# Convert the DGL heterogeneous graph to a NetworkX graph:
def to_homogeneous_nx(g):
    nx_graph = nx.DiGraph()
    for etype in g.canonical_etypes:
        src_type, rel_type, dst_type = etype
        for src, dst in zip(*g.edges(etype=etype)):
            src = f"{src_type}_{src.item()}"
            dst = f"{dst_type}_{dst.item()}"
            nx_graph.add_edge(src, dst, label=rel_type)
    return nx_graph

# def visualize_nx_graph(nx_g):
#     pos = nx.kamada_kawai_layout(nx_g)
#     nx.draw(nx_g, pos, with_labels=False, node_color='skyblue', edge_color='black', node_size=30, font_size=10, alpha=0.5)
#     plt.show()

def visualize_nx_graph(nx_g, node_size=20, edge_width=0.2, font_size=8, alpha=0.5):
    # Use the spring layout for better visualization of large graphs
    pos = nx.spring_layout(nx_g, seed=42)
    # Draw nodes
    nx.draw_networkx_nodes(nx_g, pos, node_size=node_size, node_color='skyblue', alpha=alpha)
    # Draw edges
    nx.draw_networkx_edges(nx_g, pos, width=edge_width, edge_color='black', alpha=alpha)
    # Draw node labels (optional, can be removed for very large graphs)
    # nx.draw_networkx_labels(nx_g, pos, font_size=font_size, font_color='black')
    # Display the graph
    plt.show()

# %%
load_path = Path('/Users/javierflores/Documents/upc/projects/MECCH/data/nextia')
# %%
load_path = Path('/Users/javierflores/Documents/upc/projects/MECCH/data/pubmed')
# %%

graph_data = dgl.load_graphs(str(Path(load_path, "graph.bin")))
# To access the graph object itself, we use the indexing operator [0] to access the first element of the tuple,
# and then [0] again to access the first element of the list, which is the graph object.
g_train = graph_data[0][0]
g_val = graph_data[0][1]
g_test  = graph_data[0][2]
# %%
printStatsG("g_train", g_train)
printStatsG("g_val", g_val)
printStatsG("g_test", g_test)

# %%
graph_stats("g_train", g_train)
graph_stats("g_val", g_val)
graph_stats("g_test", g_test)

# %%
print_edges_with_node_ids(g_train,"g_train")

# print_edges_with_node_ids(g_val,"g_val")
# print_edges_with_node_ids(g_test,"g_test")

# %%

train_val_test_idx = np.load(str(load_path / 'train_val_test_idx.npz'))
train_eid_dict = {'ENTITY-alignment-ENTITY': th.tensor(train_val_test_idx['train_idx'])}
val_eid_dict = {'ENTITY-alignment-ENTITY': th.tensor(train_val_test_idx['val_idx'])}
test_eid_dict = {'ENTITY-alignment-ENTITY': th.tensor(train_val_test_idx['test_idx'])}

val_neg_uv = th.tensor(np.load(str(load_path / 'val_neg_edges.npy')))
test_neg_uv = th.tensor(np.load(str(load_path / 'test_neg_edges.npy')))
        # in_dim_dict = {ntype: g_test.nodes[ntype].data['x'].shape[1] for ntype in g_test.ntypes}

print(val_neg_uv)
print(test_neg_uv)

print(len(train_val_test_idx['train_idx']))
print(len(train_val_test_idx['val_idx']))
print(len(train_val_test_idx['test_idx']))
# val_neg_uv = th.tensor(np.load(str(load_path / 'val_neg_edges.npy')))
# test_neg_uv = th.tensor(np.load(str(load_path / 'test_neg_edges.npy')))
# %%

desired_edge_type = ('ENTITY', 'ENTITY-alignment-ENTITY', 'ENTITY')
src, dst = g_train.edges(etype=desired_edge_type)
# src, dst = g_train.edges(etype=('ENTITY', 'ENTITY-alignment-ENTITY', 'ENTITY'))
print("Source nodes:", src)
print("Destination nodes:", dst)
print("source len", len(src))
print("dest len", len(dst))
print(len(train_val_test_idx['train_idx']))
print(train_val_test_idx['train_idx'])



# %%
# Convert the DGL graph to a NetworkX graph for visualization
nx_g = to_homogeneous_nx(g_train)
visualize_nx_graph(nx_g)
# Plot the graph
# pos = nx.kamada_kawai_layout(nx_g)
# nx.draw(nx_g, pos, with_labels=True, node_color='skyblue', edge_color='black', node_size=800)
# plt.show()


# %%



#
#
# train_val_test_idx = np.load(str(load_path / 'train_val_test_idx.npz'))
# val_neg_uv = th.tensor(np.load(str(load_path / 'val_neg_edges.npy')))
# test_neg_uv = th.tensor(np.load(str(load_path / 'test_neg_edges.npy')))