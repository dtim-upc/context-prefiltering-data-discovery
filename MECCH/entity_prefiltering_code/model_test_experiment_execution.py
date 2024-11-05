import argparse

import dgl
import torch as th
import torch.nn as nn
from pathlib import Path
import dgl.function as fn
import tqdm
import numpy as np
from dgl import load_graphs
from sklearn.metrics import f1_score
from experiment import utils
from model.MECCH import MECCH
from model.modules import LinkPrediction_minibatch
from utils import metapath2str, get_metapath_g, load_data_lp, load_data_lp_test_nextia, load_data_lp_only_test_nextia
from sklearn.metrics import confusion_matrix
import pandas as pd

def find_optimal_threshold(pos_scores, neg_scores, true_labels):
    scores = np.concatenate((pos_scores, neg_scores))
    thresholds = np.unique(scores)

    max_f1 = 0
    optimal_threshold = thresholds[0]

    for threshold in thresholds:
        predicted_labels = np.where(scores >= threshold, 1, 0)
        f1 = f1_score(true_labels, predicted_labels)

        if f1 > max_f1:
            max_f1 = f1
            optimal_threshold = threshold

    return optimal_threshold


def stats_pred(bin_class, type):
    print("----- " + type + " ----- ")
    unique, counts = np.unique(bin_class, return_counts=True)
    for (u, v) in zip(unique, counts):
        print("number of values "+ str(u) +" is: "+ str(v))


def confusion_M(true, pred, type):
    print("----- " + type + " ----- ")
    # print("Unique values in y_true:", np.unique(y_true))
    # print("Unique values in pos_pred:", np.unique(pos_pred))
    unique_classes = np.unique(np.concatenate([true, pred]))  # Get all unique classes

    cm = confusion_matrix(true, pred)
    if len(unique_classes) == 1:  # If only one class
        cm = np.vstack([cm, [0]])  # Add a row of zeros at the end
        cm = np.hstack([cm, [[0], [0]]])  # Add a column of zeros at the end
        unique_classes = np.append(unique_classes, 1) if unique_classes[0] == 0 else np.append(unique_classes,
                                                                                               0)  # Add the missing class
        cm_df = pd.DataFrame(cm, index=['Actual ' + str(c) for c in unique_classes],
                             columns=['Predicted ' + str(c) for c in unique_classes])
    else :
        cm_df = pd.DataFrame(cm, index=['Actual 0', 'Actual 1'], columns=['Predicted 0', 'Predicted 1'])
    print("Confusion Matrix:")
    print(cm_df)


def create_y_score(n, m):
    ones = np.ones(n)
    zeros = np.zeros(m)
    result = np.concatenate((ones, zeros))
    return result


if __name__ == '__main__':

    directoryModel = "/Users/javierflores/Documents/upc/projects/MECCH/entity_prefiltering_code/entity_model"

    graphTestPath = "/Users/javierflores/Documents/upc/projects/MECCH/data/nextia_entity_context_experiment_test/"
    outputPath = "/Users/javierflores/Documents/upc/projects/MECCH/data/nextia_entity_context_experiment_test/output/"

    # set up parameters
    dict = {"hidden_dim": 64, "n_layers": 3, "dropout": 0.5, "context_encoder": "mean", "use_v": False,
            "metapath_fusion": "conv", "residual": False, "layer_norm": True, "dataset": "nextia", "n_heads": 8,
            "n_neighbor_samples": 0, "batch_size": 102400, "device": th.device('cpu'),
            "max_mp_length": 1
            }
    args = type("argsObject", (object,), dict)
    print("loading data...")

    g_test, in_dim_dict, test_eid_dict, test_neg_uv = load_data_lp_only_test_nextia(graphTestPath)

    test_eid_dict = {metapath2str([g_test.to_canonical_etype(k)]): v for k, v in test_eid_dict.items()}
    target_etype = list(test_eid_dict.keys())[0]

    g_test, selected_metapaths = get_metapath_g(g_test, args)

    n_heads_list = [args.n_heads] * args.n_layers

    # Recreate the MECCH model with g_test.
    model_mecch = MECCH(g_test, selected_metapaths, in_dim_dict, args.hidden_dim, args.hidden_dim, args.n_layers,
                        n_heads_list, dropout=args.dropout, context_encoder=args.context_encoder, use_v=args.use_v,
                        metapath_fusion=args.metapath_fusion, residual=args.residual, layer_norm=args.layer_norm)

    # Recreate the LinkPrediction_minibatch model
    model_lp = LinkPrediction_minibatch(model_mecch, args.hidden_dim, target_etype)

    # Load the saved state_dict
    model_lp.load_state_dict(th.load(str(directoryModel + "/model/checkpoint.pt")))

    num_workers = 4
    if args.n_neighbor_samples <= 0:
        block_sampler = dgl.dataloading.MultiLayerFullNeighborSampler(args.n_layers)
    else:
        block_sampler = dgl.dataloading.MultiLayerNeighborSampler([{
            etype: args.n_neighbor_samples for etype in g_test.canonical_etypes}] * args.n_layers)


    test_eid2neg_uv = {eid: (u, v) for eid, (u, v) in
                       zip(test_eid_dict[target_etype].cpu().tolist(), test_neg_uv.cpu().tolist())}


    g_new = g_test
    new_eid_dict = test_eid_dict
    new_eid2neg_uv = test_eid2neg_uv
    # Create a DataLoader for the new data
    # load graph data in the form of mini-batches for edge-based tasks like link prediction and edge classification
    new_dataloader = dgl.dataloading.EdgeDataLoader(
        g_new,  # The input graph on which the edge-based tasks will be performed.
        new_eid_dict,  # A dictionary that maps edge types to the edge IDs that will be used for the task.
        block_sampler,  # The block sampler is responsible for sampling the subgraphs (blocks) needed for computation
        g_sampling=g_new,# An optional graph for neighbor sampling. If not specified, it defaults to the input graph g.
        negative_sampler= utils.FixedNegSampler(new_eid2neg_uv), #responsible for generating negative samples.
        batch_size=args.batch_size,  # The number of edges to include in each mini-batch.
        shuffle=False,  # If set to True, the edges will be shuffled before creating mini-batches.
        drop_last=False,
        # If set to True, the last mini-batch will be dropped if its size is smaller than the specified batch size.
        num_workers=num_workers  # The number of worker processes to use for parallel data loading.
    )

    # Iterate through the DataLoader and make predictions
    print("eval...")
    model_lp.eval()
    with th.no_grad():
        pos_score_list = []
        original_src_list = []  # list for saving original source nodes
        original_dst_list = []  # list for saving original destination nodes
        # not using the negative_graph, you still need to unpack it from the dataloader. Then you can ignore it for the rest of the loop.
        for input_nodes, positive_graph, negative_graph, blocks in new_dataloader:
            blocks = [b.to(args.device) for b in blocks]
            positive_graph = positive_graph.to(args.device)

            input_features = blocks[0].srcdata["x"]
            pos_score, _ = model_lp(positive_graph, negative_graph, blocks, input_features)

            # Get original node IDs
            original_entity_nodes = blocks[0].srcdata[dgl.NID]['ENTITY']

            # Map the relabeled IDs in positive_graph to original IDs in g_new
            src, dst = positive_graph.edges(form='uv', order='eid', etype="mp:ENTITY=>ENTITY-alignment-ENTITY=>ENTITY")
            original_src = original_entity_nodes[src].tolist()
            original_dst = original_entity_nodes[dst].tolist()

            # Append to the lists
            pos_score_list.append(pos_score.cpu().numpy())
            original_src_list.append(original_src)
            original_dst_list.append(original_dst)

        pos_scores = np.concatenate(pos_score_list, axis=0)
        original_srcs = np.concatenate(original_src_list)
        original_dsts = np.concatenate(original_dst_list)



        #........... SAVING PREDICTIONS.......
        df = pd.DataFrame({
            'src': original_srcs,
            'dst': original_dsts,
            'score': pos_scores
        })
        df.to_csv(outputPath + '/predictions.csv', index=False)
        
