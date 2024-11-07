# Contextual Pre-Filtering for Data Discovery

This repository contains the code and resources for implementing a contextual pre-filtering technique in data discovery tasks, using a modified MECCH model and data profiling strategies.

## Repository Structure

- **MECCH**: Contains the modified MECCH code adapted from the [original MECCH repository](https://github.com/cynricfu/MECCH/tree/master). Modifications were made to support our custom graph with entities and attributes.
- **EntityDiscovery**: The **EntityDiscovery** folder includes code for generating the ground truth, building graph-based schemas, and profiling datasets. This code is essential for understanding how the data was processed and prepared for training the MECCH model.

---

## Data information

The training and testing datasets, along with their graph-based schemas and metadata, are provided for replicating our experiments and training the MECCH model.

### Training Data

The training ground truth files can be downloaded from the following links:

1. **Datasets Zip**: Contains `datasetInfo.csv`, `entityAffinityLinks.csv` and the datasets.
   - **`datasetInfo.csv`**: Lists each dataset with its corresponding ID and name. The ID is crucial, as the URI resources for the graph-based schemas are constructed using the format:
     ```
     http://www.essi.upc.edu/DTIM/NextiaDI/DataSource/Schema/<datasetID>/<resource name>
     ```
   - **`entityAffinityLinks.csv`**: Contains `src_node_iri` and `dst_node_iri` columns, indicating affinity links between entities.
   - **Download Link**: [Training Datasets Zip](https://mydisk.cs.upc.edu/s/AfCn9dskZL9dpja)

2. **Graph-Based Schemas**: Schemas generated from the datasets in TTL format.
   - **Download Link**: [Graph-Based Schemas](https://mydisk.cs.upc.edu/s/74kTEcE4tB25f9G)

3. **Graph-Based Schemas with Extra Metadata**: Includes additional metadata, such as empty attributes.
   - **Download Link**: [Graph-Based Schemas with Metadata](https://mydisk.cs.upc.edu/s/ZNEzES7FYScFE52)


### Test Data

   The data used for validating our experiments is organized as follows:

   1. **Testing Datasets**: Includes `datasetInfo.csv`, `entityAffinityLinks.csv`, and the dataset files.
      - **Download Link**: [Testing Datasets](https://mydisk.cs.upc.edu/s/dsgBiTyxkmaFKJj)

   2. **Entity Tables**: Datasets provided as individual entity tables.
      - **Download Link**: [Entity Tables](https://mydisk.cs.upc.edu/s/ye3t3qgDPtzLTBk)

   3. **Graph-Based Schemas for Testing**: Graph-based schemas in TTL format for testing datasets.
      - **Download Link**: [Graph-Based Schemas for Testing](https://mydisk.cs.upc.edu/s/k44GXK2BBEfiEQ6)

   4. **Graph-Based Schemas with Extra Metadata**: Contains metadata highlighting empty attributes.
      - **Download Link**: [Testing Graph-Based Schemas with Metadata](https://mydisk.cs.upc.edu/s/k44GXK2BBEfiEQ6)

   For details on using this data to train and test the MECCH model, refer to the README inside the **MECCH** folder in this repository.   


---



## MECCH Folder Overview

The **MECCH** folder is a copy of the original MECCH codebase with modifications to handle our type of graph structure. This includes:
- Extending the model to recognize entities and attributes
- Modifying internal code to align with our graph schema requirements

### Data Preparation

To prepare the training data for MECCH, use the script at: MECCH/entity_prefiltering_code/entity_with_nums_strs_inv.py


This script processes raw data and generates the necessary input format for MECCH. Place the generated data files in `MECCH/data/nextia_entity_context`.

#### Raw Data Files

Raw data required for generating the MECCH input structure is located in `MECCH/data/nextia_entity_context/raw`, containing the following files:
- **entityNodes.csv**: Includes node ID, name, alias, IRI, dataset ID, dataset name, and node attributes.
- **strNodes.csv**: String nodes with attributes.
- **numNodes.csv**: Numerical nodes with attributes.
- **g_attr_links.csv**, **g_num_links.csv**, **g_rel_links.csv**: Link files for attributes, numbers, and relationships.
- **g_test_alignments_links.csv**, **g_train_alignments_links.csv**, **g_val_alignments_links.csv**: Alignment files for training, validation, and testing.
- **helpers.csv**: Refer to the MECCH documentation for further details.

Alternatively, you can download the pre-processed data from [this link](https://mydisk.cs.upc.edu/s/Mna387A4aZDmLbG) and unzip it into `MECCH/data/nextia_entity_context`.

### Training the MECCH Model

To train the model, follow the MECCH guidelines and use the command below with the specified parameters:

```bash
python main.py -m MECCH -t link_prediction -d nextia -g 0
```
* -m MECCH: Specifies the MECCH model.
* -t link_prediction: Defines the task as link prediction.
* -d nextia: Uses the nextia dataset, representing our custom graph schema.
* -g 0: Specifies GPU usage (if available); 0 is the first GPU.

The generated model can be found in MECCH/entity_prefiltering_code/entity_model or downloaded from this [link](https://mydisk.cs.upc.edu/s/AXsQCyYncw37zqe).

Making Predictions

To make predictions with the trained MECCH model, follow these steps:

1. Data Transformation: Transform your experiment testing data into the MECCH-compatible format using:

```
MECCH/entity_prefiltering_code/entity_with_nums_strs_inv_experiment_Test.py
```

This will output the necessary structures in MECCH/data/nextia_entity_context_experiment_test, or you can download them from this [link](https://mydisk.cs.upc.edu/s/omTYmZzGac4XiwM).

2. Prediction Execution: Use the script:

```
MECCH/entity_prefiltering_code/model_test_experiment_execution.py

```
This will execute predictions based on the trained model and the transformed test data.


For more detailed information on parameter configurations and MECCH requirements, refer to the [MECCH documentation](https://github.com/cynricfu/MECCH/tree/master).
