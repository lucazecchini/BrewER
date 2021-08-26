#!/usr/bin/env python3

import json
import networkx as nx
import pandas as pd


def ds_generation(raw_path, ds_path):
    # Load the raw dataset in DataFrame format
    ds = pd.read_csv(raw_path, skipinitialspace=True)
    ds = ds.fillna(value='null')

    # Insert a progressive identifier
    ds = ds.reset_index()
    ds = ds.rename(columns={'index': 'id'})
    ds['id'] = ds['id'].apply(lambda x: 'funding_' + str(x))

    # Put all column names in lowercase
    ds.columns = ds.columns.str.lower()

    # Remove the eventual whitespace at the end of each column name (if present) and replace the others with underscore
    for attribute in ds.columns:
        if attribute.endswith(' '):
            ds = ds.rename(columns={attribute: attribute[:-1]})
    ds.columns = ds.columns.str.replace(' ', '_')

    # Remove '$' at the beginning of amount values and treat them as float
    ds['amount'] = ds['amount'].astype(str)
    ds['amount'] = ds['amount'].map(lambda x: x.lstrip('$'))
    ds['amount'] = ds['amount'].astype(float)

    # Now, put all the non-numeric attributes in lowercase
    for attribute in ds.columns:
        if ds[attribute].dtype == 'object':
            ds[attribute] = ds[attribute].str.lower()

    # Save the generated dataset in a CSV file
    ds.to_csv(ds_path, index=False)


def gold_generation(ds_path, gold_path):
    # Load the dataset in DataFrame format, creating also an alternative version as a list of dictionaries
    ds = pd.read_csv(ds_path)
    ds_dict = ds.to_dict('records')

    # Inverted index on the attribute 'cluster_id' to find clusters and matching couples
    clusters = dict()
    for s in ds_dict:
        if s['cluster_id'] in clusters.keys():
            clusters[s['cluster_id']].append(s['id'])
        else:
            clusters.update({s['cluster_id']: [s['id']]})
    couples = set()
    for c in clusters.keys():
        if len(clusters[c]) > 1:
            for i in clusters[c]:
                for j in clusters[c]:
                    if i < j:
                        couples.add((i, j))
                    if i > j:
                        couples.add((j, i))
    couples = list(couples)
    couples = pd.DataFrame(couples, columns=['left_spec_id', 'right_spec_id'])

    # Save the matching couples in a CSV file
    couples.to_csv(gold_path, index=False)


def blocking(ds_dict):
    # Generate blocks
    ds_name = 'funding'
    blocks_path = "data/" + ds_name + "_blocks.txt"
    block_costs_path = "data/" + ds_name + "_block_costs.txt"
    record_blocks_path = "data/" + ds_name + "_record_blocks.txt"

    # Inverted index on 'legal_name' and 'address': generate blocks
    blocks = dict()
    for d in ds_dict:
        if d['legal_name'] != 'NaN':
            tmp = str(d['legal_name']).split(',')[0]
            tmp = tmp.strip('"')
            tmp = tmp.strip(',')
            tmp = ''.join(tmp.split(' ')[0:2])
            d['NAME'] = ''.join(c for c in tmp if c.isalnum())
            if d['NAME'] in blocks.keys():
                blocks[d['NAME']].append(d['id'])
            else:
                blocks.update({d['NAME']: [d['id']]})
        if d['address'] != 'NaN':
            tmp = str(d['address']).strip('"')
            tmp = ''.join(tmp.split(' ')[0:2])
            d['ADDRESS'] = ''.join(c for c in tmp if c.isalnum())
            if d['ADDRESS'] in blocks.keys():
                blocks[d['ADDRESS']].append(d['id'])
            else:
                blocks.update({d['ADDRESS']: [d['id']]})

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
    raw_path = "data_raw/address.csv"
    ds_path = "data/funding_dataset.csv"

    # Generate the dataset
    ds_generation(raw_path, ds_path)

    # Define the path of the ground truth to be generated
    gold_path = "data/funding_gold.csv"

    # Generate the ground truth
    gold_generation(ds_path, gold_path)

    ds = pd.read_csv('data/funding_dataset.csv')
    ds['amount'] = pd.to_numeric(ds['amount'], errors='coerce')
    for column in ds.columns:
        if ds[column].dtype == 'object':
            ds[column] = ds[column].fillna('NaN')
    ds_dict = ds.to_dict('records')
    blocking(ds_dict)


if __name__ == "__main__":
    main()
