import pandas as pd
import dgl
from pathlib import Path
import torch as th
import numpy as np


def graph_stats( g, graphName):
    print(f"-------------{graphName.upper()}-------------")
    # stats
    print(f"Number of nodes: {g.number_of_nodes()}")
    for ntype in g.ntypes:
        print("Number of nodes in type '{}': {}".format(ntype, g.number_of_nodes(ntype)))

    # Print the number of edges per type
    for etype in g.etypes:
        print("Number of edges in type '{}': {}".format(etype, g.number_of_edges(etype)))


def process_helpers(df):

    # train_row = df[df['key'] == 'train_idx']
    # train_value = train_row['value'].values[0]
    # train_idx = np.array([int(x) for x in train_value.split(',')])
    #
    # val_row = df[df['key'] == 'val_idx']
    # val_value = val_row['value'].values[0]
    # val_idx = np.array([int(x) for x in val_value.split(',')])

    test_row = df[df['key'] == 'test_idx']
    test_value = test_row['value'].values[0]
    test_idx = np.array([int(x) for x in test_value.split(',')])

    neg_row = df[df['key'] == 'test_neg_edges']
    neg_value = neg_row['value'].values[0]
    test_neg_edges =np.array([np.fromstring(row, dtype=int, sep=',') for row in neg_value.split(';')])

    # neg_val_row = df[df['key'] == 'val_neg_edges']
    # neg_val_value = neg_val_row['value'].values[0]
    # val_neg_edges = np.array([np.fromstring(row, dtype=int, sep=',') for row in neg_val_value.split(';')])

    return  test_idx, test_neg_edges



if __name__ == "__main__":
    # load_path = Path('/Users/javierflores/Documents/upc/projects/MECCH/data/TEST3/str_num_inv/data')
    # save_path = Path('/Users/javierflores/Documents/upc/projects/MECCH/data/TEST3/str_num_inv/graph')


    # load_path = Path('/Users/javierflores/Documents/upc/projects/MECCH/data/Test_last_bool2/data')
    # save_path = Path('/Users/javierflores/Documents/upc/projects/MECCH/data/Test_last_bool2/graph')

    load_path = Path('/Users/javierflores/Documents/upc/projects/MECCH/data/nextia_entity_context_experiment_test/raw')
    save_path = Path('/Users/javierflores/Documents/upc/projects/MECCH/data/nextia_entity_context_experiment_test/')

    # Read data from CSV files
    entity_nodes = pd.read_csv(load_path /'entityNodes.csv')
    str_att_nodes = pd.read_csv(load_path /'strNodes.csv')
    num_att_nodes = pd.read_csv(load_path / 'numNodes.csv')
    g_att_links =  pd.read_csv(load_path /'g_attr_links.csv')
    g_num_links = pd.read_csv(load_path / 'g_num_links.csv')
    g_rel_links = pd.read_csv(load_path / 'g_rel_links.csv')
    g_test_align_links = pd.read_csv(load_path / 'g_test_alignments_links.csv')
    # g_train_align_links = pd.read_csv(load_path / 'g_train_alignments_links.csv')
    # g_val_align_links = pd.read_csv(load_path / 'g_val_alignments_links.csv')
    helpers = pd.read_csv(load_path / 'helpers.csv')

    num_nodes_dict = { 'ENTITY': len(entity_nodes), 'STR_ATTR': len(str_att_nodes), "NUM_ATTR": len(num_att_nodes) }
    # Create the dictionary for DGL Heterograph

    data_dict_test = {
        ('ENTITY', 'ENTITY-alignment-ENTITY', 'ENTITY'): (g_test_align_links['src_id'].values, g_test_align_links['dst_id'].values),
        ('ENTITY', '^ENTITY-alignment-ENTITY', 'ENTITY'): (g_test_align_links['dst_id'].values, g_test_align_links['src_id'].values),

        ('ENTITY', 'ENTITY-relationship-ENTITY', 'ENTITY'): (g_rel_links['src_id'].values, g_rel_links['dst_id'].values),
        ('ENTITY', '^ENTITY-relationship-ENTITY', 'ENTITY'): (g_rel_links['dst_id'].values, g_rel_links['src_id'].values),

        ('ENTITY', 'ENTITY-hasstrattr-STR_ATTR', 'STR_ATTR'): (g_att_links['src_id'].values, g_att_links['dst_id'].values),
        ('STR_ATTR', '^ENTITY-hasstrattr-STR_ATTR', 'ENTITY'): (g_att_links['dst_id'].values, g_att_links['src_id'].values),

        ('ENTITY', 'ENTITY-hasnumattr-STR_ATTR', 'NUM_ATTR'): (g_num_links['src_id'].values, g_num_links['dst_id'].values),
        ('NUM_ATTR', '^ENTITY-hasnumattr-STR_ATTR', 'ENTITY'): (g_num_links['dst_id'].values, g_num_links['src_id'].values),

    }

    entity_features =  th.from_numpy(np.genfromtxt(entity_nodes["node_attributes"].tolist(), delimiter=',') ).float()
    str_att_features = th.from_numpy(np.genfromtxt(str_att_nodes["node_attributes"].tolist(), delimiter=',')).float()
    num_att_features = th.from_numpy(np.genfromtxt(num_att_nodes["node_attributes"].tolist(), delimiter=',')).float()


    g_test = dgl.heterograph(data_dict_test, num_nodes_dict, idtype=th.int64)
    g_test.nodes['ENTITY'].data['x'] = entity_features
    g_test.nodes['STR_ATTR'].data['x'] = str_att_features
    g_test.nodes['NUM_ATTR'].data['x'] = num_att_features


    graph_stats(g_test, "g_test")

    test_idx, test_neg_edges = process_helpers(helpers)



    dgl.save_graphs(str(save_path / 'graph.bin'), [g_test, g_test, g_test])
    np.savez(save_path / 'train_val_test_idx.npz',
             # train_idx=train_idx,
             # val_idx=val_idx,
             test_idx=test_idx)

    # np.save(save_path / 'val_neg_edges.npy', val_neg_edges)
    np.save(save_path / 'test_neg_edges.npy', test_neg_edges)
