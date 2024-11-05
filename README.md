# Contextual Pre-Filtering for Data Discovery

# MECCH Model for Graph-Based Schema

This repository contains a modified implementation of MECCH, tailored to support our custom graph-based schema for training a heterogeneous graph neural network.

## Getting Started

To generate the model using MECCH, you can run the following command:

```bash
python main.py -m MECCH -t link_prediction -d nextia -g 0
```


Parameters

    -m MECCH: Specifies the model, here set to MECCH.
    -t link_prediction: Defines the task, which in this case is link prediction.
    -d nextia: Sets the dataset to nextia, representing our custom graph schema.
    -g 0: Indicates the GPU to be used (if available); use 0 for the first GPU.


Data

The dataset used for training, testing, and validation is located in:

data/nextia/data

This directory includes the graph representations required for training and evaluating the MECCH model on link prediction tasks.
