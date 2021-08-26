#!/usr/bin/env python3

import json
import networkx as nx
import numpy as np
import pandas as pd


def ds_generation(raw_path_1, raw_path_2, ds_path):
    # Load the raw dataset in DataFrame format
    ds1 = pd.read_csv(raw_path_1, skipinitialspace=True)
    ds1 = ds1.rename(columns={'instance_id': 'id'})
    ds2 = pd.read_csv(raw_path_2, skipinitialspace=True)
    ds2 = ds2.rename(columns={'instance_id': 'id'})
    ds = pd.concat([ds1, ds2])

    # Create a numeric version of 'size' attribute: remove ' GB' at the end of size values and treat them as float
    ds['size_num'] = ds['size']
    ds['size_num'] = ds['size_num'].astype(str)
    ds['size_num'] = ds['size_num'].map(lambda x: x.rstrip(' GB'))
    ds['size_num'] = ds['size_num'].map(lambda x: x.rstrip(' T'))
    ds['size_num'] = ds['size_num'].astype(float)

    # Now, put all the non-numeric attributes in lowercase
    for attribute in ds.columns:
        if ds[attribute].dtype == 'object':
            ds[attribute] = ds[attribute].str.lower()

    # Save the generated dataset in a CSV file
    ds.to_csv(ds_path, index=False)


def gold_generation(raw_path_1, raw_path_2, gold_path, specifications):
    # Load the raw dataset in DataFrame format
    ds1 = pd.read_csv(raw_path_1)
    ds2 = pd.read_csv(raw_path_2)
    ds = pd.concat([ds1, ds2])

    # Keep only the matches
    ds = ds.loc[ds['label'] == 1]

    # Rename id columns and drop labels
    ds = ds.rename(columns={'left_instance_id': 'left_spec_id', 'right_instance_id': 'right_spec_id'})
    ds = ds.drop(columns='label')

    # Put the elements of the matching couples in the right order
    ds['left_spec_id'], ds['right_spec_id'] = np.where(ds['left_spec_id'] < ds['right_spec_id'],
                                                       (ds['left_spec_id'], ds['right_spec_id']),
                                                       (ds['right_spec_id'], ds['left_spec_id']))

    # Save the matching couples in a CSV file
    ds.to_csv(gold_path, index=False)


def blocking(ds_dict):
    # Generate blocks
    ds_name = 'altosight_sigmod'
    blocks_path = "data/" + ds_name + "_blocks.txt"
    block_costs_path = "data/" + ds_name + "_block_costs.txt"
    record_blocks_path = "data/" + ds_name + "_record_blocks.txt"

    # Inverted index on 'brand' attribute: generate blocks
    blocks = dict()
    for d in ds_dict:
        if d['brand'] == 'NaN' or d['size'] == 'NaN':
            pass
        else:
            blocker = d['brand'] + d['size']
            blocker = ''.join(blocker.split())
            if blocker in blocks.keys():
                blocks[blocker].append(d['id'])
            else:
                blocks.update({blocker: [d['id']]})

    # Associate to each block its cost (the number of required comparisons)
    block_cost = dict()
    for b in blocks.keys():
        block_cost[b] = int((len(blocks[b]) * (len(blocks[b]) - 1)) / 2)

    # Associate to each record the blocks in which it appears
    record_blocks = dict((record['id'], []) for record in ds_dict)
    for b in blocks.keys():
        for record in blocks[b]:
            record_blocks[record].append(b)

    # Transitive closure: merge blocks with elements in common (connected components of a graph)
    nodes = list()
    for d in ds_dict:
        nodes.append(d['id'])
    edges = set()
    for b in blocks.keys():
        if len(blocks[b]) > 1:
            for i in blocks[b]:
                for j in blocks[b]:
                    if i < j:
                        edges.add((i, j))
                    if i > j:
                        edges.add((j, i))
    bg = nx.Graph()
    bg.add_nodes_from(nodes)
    bg.add_edges_from(edges)
    blocks = list()
    for cc in nx.connected_components(bg):
        blocks.append(list(cc))

    # Save the generated lists/dictionaries about blocking as JSON
    with open(blocks_path, 'w') as output_file:
        json.dump(blocks, output_file)
    with open(block_costs_path, 'w') as output_file:
        json.dump(block_cost, output_file)
    with open(record_blocks_path, 'w') as output_file:
        json.dump(record_blocks, output_file)


def main():
    # Define the path of the dataset to be generated and the one of the raw dataset to start from
    raw_path_1 = "data_raw/altosight_sigmod_X.csv"
    raw_path_2 = "data_raw/altosight_sigmod_Z.csv"
    ds_path = "data/altosight_sigmod_dataset.csv"

    # Generate the dataset
    ds_generation(raw_path_1, raw_path_2, ds_path)

    # Define the path of the ground truth to be generated
    raw_path_1 = "data_raw/altosight_sigmod_Y.csv"
    raw_path_2 = "data_raw/altosight_sigmod_E.csv"
    gold_path = "data/altosight_sigmod_gold.csv"

    # Generate the ground truth
    specifications = pd.read_csv('data/altosight_sigmod_dataset.csv')
    gold_generation(raw_path_1, raw_path_2, gold_path, specifications)

    ds = pd.read_csv('data/altosight_sigmod_dataset.csv')
    ds['price'] = pd.to_numeric(ds['price'], errors='coerce')
    ds['size_num'] = pd.to_numeric(ds['size_num'], errors='coerce')
    for column in ds.columns:
        if ds[column].dtype == 'object':
            ds[column] = ds[column].fillna('NaN')
    ds_dict = ds.to_dict('records')
    blocking(ds_dict)


if __name__ == "__main__":
    main()
