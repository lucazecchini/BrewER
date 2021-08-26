#!/usr/bin/env python3

import json
import networkx as nx
import os
import pandas as pd
import re


def ds_generation():
    # Punctuation list for string normalization (substituted by space)
    punctuation = [",", ";", ":", "!", "?", "(", ")", "[", "]", "{", "}", '"', "/", "|", "*"]

    # List of common brands (including aliases and misspelled versions)
    brands = ["aiptek", "apple", "argus", "bell+howell", "benq", "cannon", "canon", "casio", "coleman", "contour",
              "dahua", "epson", "fugi", "fugifilm", "fuji", "fujifilm", "fujufilm", "garmin", "ge", "general", "gopro",
              "gopros", "hasselblad", "hikvision", "howell", "hp", "intova", "jvc", "kodak", "leica", "lg", "lowepro",
              "lytro", "minolta", "minotla", "minox", "motorola", "mustek", "nikon", "olympus", "olympuss", "panasonic",
              "panosonic", "pentax", "philips", "polaroid", "ricoh", "sakar", "samsung", "sanyo", "sekonic", "sigma",
              "sony", "ssamsung", "tamron", "toshiba", "vivitar", "vtech", "wespro", "yourdeal"]

    # List of (not only) measure suffixes to be ignored for the model extraction
    measures = ["cm", "mm", "nm", "inch", "ft", "gb", "mb", "mp", "megapixel", "megapixels", "mega", "hz", "mah", "mps",
                "cmos", "color", "colors", "pcs"]

    # Read JSON specifications
    path = 'data_raw/2013_camera_specs'

    specifications = []

    for folder_name in os.listdir(path):
        for file_name in os.listdir(path + '/' + str(folder_name)):
            with open(path + '/' + str(folder_name) + '/' + str(file_name), 'r') as data:

                # Initialize the current specification record as an empty dictionary
                camera = {}

                # Read the JSON content as a dictionary and lower the names of the attributes
                spec = json.load(data)
                spec = dict((k.lower(), v) for k, v in spec.items())

                # Add file path as 'id' attribute
                camera['id'] = str(folder_name) + '//' + str(file_name[:-5])

                # Add '<page_title>' attribute as 'description' and create a normalized copy for information retrieval
                camera['description'] = spec['<page title>'].lower()
                description = camera['description']
                for p in punctuation:
                    description = description.replace(p, ' ')

                # Split 'description' attribute for information retrieval
                splitted = description.split()

                # Extract the brand among 'description' split words
                camera['brand'] = 'null'
                for s in splitted:
                    if s in brands:
                        camera['brand'] = s
                        break

                # Define for each brand some specific elements which can be useful for model retrieval:
                # common prefixes that can appear separately from the rest of the model name
                prefixes = []
                # common suffixes that can appear separately from the rest of the model name
                suffixes = []
                # totally alphabetic or totally numeric model names (not retrievable through regular expressions)
                models = []
                # alphanumeric words which do not represent models
                exceptions = ['1080p', '3d', '720p', 'h.246', 'h.264', 'h.624', 'h264', 'ip65', 'ip66', 'ip67', 'mpeg4',
                              'no.1', 'p2p']

                if camera['brand'] == 'aiptek':
                    prefixes = ['dv']
                elif camera['brand'] == 'apple':
                    prefixes = ['iphone', 'quicktake']
                elif camera['brand'] == 'argus':
                    prefixes = ['dc']
                elif camera['brand'] in ['bell+howell', 'howell']:
                    prefixes = ['take']
                    exceptions = exceptions + ['splash2']
                elif camera['brand'] in ['canon', 'cannon']:
                    prefixes = ['a', 'bg', 'elf', 'elph', 'hf', 'ixus', 'ixy', 'pro', 'sd', 'sx']
                    suffixes = ['c', 'd', 'ds', 'dx', 'x']
                    models = ['ixus i', 'ixus ii', 'xs', 'xsi', 'xt', 'xti']
                    exceptions = exceptions + ['100v', '2000s', '3x', '40m', '4x', '50x', '5260b009', '5x', '70db',
                                               '70dkis', '70dpk', '70dsk', '8160b001',
                                               '8231b005', '8595b005', '8x', '9126b003', '9156b001', 'd700', 'ew73b',
                                               'g151428', 'gf-b064', 'k47']
                elif camera['brand'] == 'casio':
                    prefixes = ['ex', 'qv']
                    models = ['tryx']
                    exceptions = exceptions + ['3x', 'l00ks', 'w0w']
                elif camera['brand'] == 'coleman':
                    exceptions = exceptions + ['xtreme2', 'xtreme3']
                elif camera['brand'] == 'contour':
                    prefixes = ['roam']
                elif camera['brand'] == 'dahua':
                    exceptions = exceptions + ['1.3m', '1.3mpl', '100m', '150m', '18x', '2048x1536', '20m', '20x',
                                               '30x', '3m', '3x', '50m', '700tvl', 'ik10',
                                               'ir100m', 'onvif2.0', 'rs485']
                elif camera['brand'] == 'epson':
                    prefixes = ['r', 'rd']
                elif camera['brand'] in ['fuji', 'fujifilm', 'fugi', 'fugifilm', 'fujufilm']:
                    prefixes = ['ax', 'ds', 'hs', 'instax', 'jx', 'mx', 'quicksnap', 'x', 'x pro', 'x-pro', 'z']
                    suffixes = ['exr', 'fd']
                    models = ['1300', '2300', '2400', '2600', '2650', '2800', '3800', '4700', '4900']
                    exceptions = exceptions + ['12x', '2.7in', '30x', '5-in-1', '5x', 'casioex-g1']
                elif camera['brand'] == 'garmin':
                    suffixes = ['elite']
                    models = ['virb', 'virb elite']
                elif camera['brand'] in ['ge', 'general']:
                    suffixes = ['w']
                elif camera['brand'] in ['gopro', 'gopros']:
                    prefixes = ['hero']
                    suffixes = ['+', 'plus']
                    exceptions = exceptions + ['30m', '45m', '5m', 'h3', 'st29']
                elif camera['brand'] == 'hasselblad':
                    prefixes = ['cfv']
                    suffixes = ['40', '50']
                    models = ['lunar', 'stellar']
                elif camera['brand'] == 'hikvision':
                    prefixes = ['ds']
                    exceptions = exceptions + ['100m', '10m', '12v', '20m', '20x', '30m', '32g', '50m', '960p',
                                               'hikvision1080p', 'ir100m', 'ir30m', 'm14',
                                               'rj45', 'rs-485']
                elif camera['brand'] == 'hp':
                    prefixes = ['r']
                    models = ['215', '315', '318', '320', '435', '618', '635', '720', '735', '812', '850', '935', '945']
                    exceptions = exceptions + ['8x']
                elif camera['brand'] == 'intova':
                    prefixes = ['cp', 'ic']
                elif camera['brand'] == 'kodak':
                    prefixes = ['cx', 'dc', 'dcs', 'dx', 'kv', 'm']
                    exceptions = exceptions + ['10x', '3x', '7c55', 'v2.21']
                elif camera['brand'] == 'leica':
                    prefixes = ['d-lux', 'digilux', 'lux', 'v-lux', 'vlux', 'x']
                    suffixes = ['p']
                    models = ['112', '114', '240', '701', '9', 'x vario']
                    exceptions = exceptions + ['0.68x']
                elif camera['brand'] == 'lg':
                    exceptions = exceptions + ['32in', 'pn4500']
                elif camera['brand'] in ['minolta', 'minotla']:
                    prefixes = ['x', 'xg']
                    suffixes = ['si']
                    models = ['5', '7', 'blowout']
                    exceptions = exceptions + ['3x', 'vc-7d']
                elif camera['brand'] == 'minox':
                    models = ['dcc', 'dsc', 'minoctar']
                elif camera['brand'] == 'motorola':
                    prefixes = ['phone']
                elif camera['brand'] == 'mustek':
                    prefixes = ['mdc']
                elif camera['brand'] == 'nikon':
                    prefixes = ['aw', 'd', 'l', 's', 'tc', 'v']
                    models = ['25462', '25480', '26286', '2000', '2100', '2200', '2500', '3100', '3200', '3500', '3700',
                              '4200', '4300', '4500', '4600', '4800',
                              '5000', '5100', '5400', '5600', '5700', '600', '700', '7600', '775', '7900', '800',
                              '8400', '8700', '8800', '885', '950', '990',
                              '995', 'a', 'df']
                    exceptions = exceptions + ['10x', '30x', '3x', '40m', '42x', '4g', '4x', '6x', '7x', 'k164318',
                                               'nikon1', 's2868', 's3090']
                elif camera['brand'] in ['olympus', 'olympuss']:
                    prefixes = ['c', 'd', 'e', 'em', 'ep', 'epm', 'f', 'fe', 'sh', 'sp', 'sz', 't', 'tg', 'vg', 'vh',
                                'vr', 'x', 'xz']
                    suffixes = ['sw', 'uz']
                    models = ['105', '300', '400', '410', '500', '600', '710', '730', '740', '750', '760', '780', '800',
                              '810', '820', '830', '850', '1000',
                              '1010', '1040', '1200', '3000', '5010', '6000', '6010', '6020', '7000', '7010', '7030',
                              '7040', '8000', '8010', '9000', '9010']
                    exceptions = exceptions + ['10.7x', '10x', '1134shot', '15x', '1m', '20x', '26-gvy1ozukj', '36x',
                                               '3x', '40m', '50x', '7x', 'dsc-rx100', 'f2',
                                               'j1', 'v103020bu000', 'x21']
                elif camera['brand'] in ['panasonic', 'panosonic']:
                    prefixes = ['fs', 'fx', 'hc', 'hx', 'tz']
                    models = ['141', '161', '91']
                    exceptions = exceptions + ['100v', '10x', '20x', '35x', '4x', '5x', '60x', '8x']
                elif camera['brand'] == 'philips':
                    exceptions = exceptions + ['3x']
                elif camera['brand'] == 'polaroid':
                    prefixes = ['is', 'pdc']
                    models = ['320', 'pogo']
                elif camera['brand'] in ['ricoh', 'pentax']:
                    prefixes = ['*ist', 'i', 'ist', 'k', 'mx', 'q', 'wg', 'x']
                    suffixes = ['ii', 'iis']
                    models = ['*ist dl', '*ist ds', '30', '60', 'efina', 'gr', 'gxr', 'ist d', 'ist dl', 'ist ds',
                              'k m', 'k r', 'k x', 'k-r', 'k-x', 'kr', 'kx',
                              'q +', 'q digital', 'q rare', 'q white', 'theta', 'wg-ii', 'wg iii']
                    exceptions = exceptions + ['4x', 'opti0']
                elif camera['brand'] == 'samsung':
                    prefixes = ['dv', 'mv', 'note', 'pl', 'sh', 'st', 'tl', 'wb']
                    exceptions = exceptions + ['18x', '20m', '21x', '21x23', '26x', '3g', '3x', '4k', '5m', 'camera2',
                                               'case2013']
                elif camera['brand'] == 'sanyo':
                    prefixes = ['s']
                    exceptions = exceptions + ['4x', '5x']
                elif camera['brand'] == 'sigma':
                    prefixes = ['f', 'sd']
                    suffixes = ['merrill', 'quattro']
                elif camera['brand'] == 'sony':
                    prefixes = ['dsc', 'hdr', 'ilca', 'ilce', 'kdf', 'kdl', 'nex', 'nsx']
                    suffixes = ['tvl']
                    exceptions = exceptions + ['0whli', '10x', '12x', '15m', '16x', '1g', '20m', '24ir', "24led's",
                                               '27x', '28x', '2x', "3'sony", '36x', '3g',
                                               '3x', '4.2v', '40m', '40mbs', '40meter', '42in', '42v', '4x', '500m',
                                               '50m', '5x', '63x', '6x', '70-75m', '75m',
                                               '7fps', '7x', '94mbs', '960h', '960p', 'bullet1', 'bw21', 'bw65',
                                               'cmos1000tvl', 'color1', 'dome1', 'ip66-rated',
                                               'ir40m', 'onvif2.2', 'price1', 'ps3', 'ry-5001c', 'ry-7075', 'ry-70d1',
                                               'sensor720p', 'sony+dslr+700+michigan',
                                               'sony1', 'sonydsc-wx5b', 'ss-7162', 'top10']
                elif camera['brand'] == 'tamron':
                    prefixes = ['f']
                elif camera['brand'] == 'toshiba':
                    prefixes = ['pdr']
                elif camera['brand'] == 'vivitar':
                    prefixes = ['v']
                    models = ['20', '5118']
                    exceptions = exceptions + ['10x25', '4x']

                # Manage suffixes to create alphanumeric model strings
                if len(suffixes) > 0:
                    for i in range(1, len(splitted)):
                        if (splitted[i] in suffixes) and (splitted[i - 1].endswith(splitted[i]) is False):
                            splitted[i - 1] = splitted[i - 1] + " " + splitted[i]

                # Find prefixes and insert them in model strings
                if len(prefixes) > 0:
                    for i in range(0, len(splitted) - 1):
                        if (splitted[i] in prefixes) and (splitted[i + 1].startswith(splitted[i]) is False):
                            splitted[i] = splitted[i] + " " + splitted[i + 1]

                # Extract the model among 'description' split words
                camera['model'] = 'null'
                for s in splitted:
                    if ((bool(re.match('^(?=.*[0-9])(?=.*[a-z])', s)) is True) and (s not in exceptions)) or (
                            s in models):
                        is_measure = False
                        for measure in measures:
                            if s.endswith(measure):
                                is_measure = True
                        if is_measure is False:
                            camera['model'] = s
                            break

                # Further actions to detect more details about model
                if camera['brand'] in ['canon', 'cannon'] and camera['model'] in ['1d', '1 d', '1ds', '1 ds', '5d',
                                                                                  '5 d', '6d', '7d', 'eos-1d', 'eos-7d',
                                                                                  'g1x',
                                                                                  'g1 x']:
                    for s in [' mark iv ', ' mk4 ']:
                        if s in description:
                            camera['model'] = camera['model'] + ' ' + (s.lstrip()).rstrip()
                    for s in [' mark iii ', ' mk iii ']:
                        if s in description:
                            camera['model'] = camera['model'] + ' ' + (s.lstrip()).rstrip()
                    if ' iii ' in description:
                        if camera['model'].endswith('iii') is False:
                            camera['model'] = camera['model'] + ' ' + 'iii'
                    if ' mark ii n' in description:
                        camera['model'] = camera['model'] + ' ' + 'mark ii n'
                    for s in [' mark ii ', ' markii ', ' mark 2 ', ' mk ii ', ' mkii ']:
                        if s in description:
                            if (camera['model'].endswith('ii') is False) and (
                                    camera['model'].endswith('ii n') is False):
                                camera['model'] = camera['model'] + ' ' + (s.lstrip()).rstrip()
                    if ' ii ' in description:
                        if (camera['model'].endswith('ii') is False) and (camera['model'].endswith('ii n') is False):
                            camera['model'] = camera['model'] + ' ' + 'ii'
                    if ' mark i ' in description:
                        camera['model'] = camera['model'] + ' ' + 'mark i'
                if camera['brand'] == 'leica' and camera['model'] != 'null':
                    if camera['model'].startswith('lux'):
                        if ' d ' in description:
                            camera['model'] = 'd' + ' ' + camera['model']
                        elif ' v ' in description:
                            camera['model'] = 'v' + ' ' + camera['model']
                        elif ' c ' in description:
                            camera['model'] = 'c' + ' ' + camera['model']
                    if camera['model'] == '240':
                        if ' m p ' in description:
                            camera['model'] = 'm p' + ' ' + camera['model']
                        elif ' m ' in description:
                            camera['model'] = 'm' + ' ' + camera['model']
                if camera['brand'] in ['pentax', 'ricoh'] and camera['model'] == 'gr':
                    if ' iv ' in description:
                        camera['model'] = camera['model'] + ' ' + 'iv'
                    elif ' digital iii ' in description:
                        camera['model'] = camera['model'] + ' ' + 'digital iii'
                    elif ' digital ii ' in description:
                        camera['model'] = camera['model'] + ' ' + 'digital ii'
                    elif ' digital ' in description:
                        camera['model'] = camera['model'] + ' ' + 'digital'
                if camera['brand'] == 'sony' and camera['model'] != 'null':
                    if camera['model'] == 'a77' or camera['model'] == 'slt-a77':
                        for s in [' ii ', ' 2 ']:
                            if s in description:
                                camera['model'] = camera['model'] + ' ' + s.strip()
                    if camera['model'] in ['dsc rx100', 'dsc-rx100', 'dscrx100']:
                        if ' ii ' in description:
                            camera['model'] = camera['model'] + ' ' + 'ii'
                        elif ' iii ' in description:
                            camera['model'] = camera['model'] + ' ' + 'iii'

                # Extract the resolution in megapixels of the camera
                units = ['mp', 'megapixel', 'megapixels', 'mega']
                camera['megapixels'] = 'null'
                for i in range(1, len(splitted)):
                    if camera['megapixels'] == 'null':
                        # case 1: the unit of measure is separated from the value
                        if splitted[i] in units:
                            # if the previous word is a dotted number, resolution found
                            if (bool(re.match('^(?=.*[0-9])', splitted[i - 1])) is True) and ('.' in splitted[i - 1]):
                                camera['megapixels'] = splitted[i - 1]
                            # if the previous word is 0, take its previous one if it is a number
                            elif splitted[i - 1] == '0' and splitted[i - 2].isdigit():
                                camera['megapixels'] = splitted[i - 2] + '.0'
                            # if the previous word is a number and the one before is not a number, resolution found
                            elif splitted[i - 1].isdigit() and splitted[i - 2].isdigit() is False:
                                camera['megapixels'] = splitted[i - 1] + '.0'
                            # if the previous word is a number and the one before is also a number (but not the model),
                            # resolution found
                            elif splitted[i - 1].isdigit() and splitted[i - 2].isdigit() and splitted[i - 2] not in \
                                    camera['model'].split():
                                camera['megapixels'] = splitted[i - 2] + '.' + splitted[i - 1]
                        # case 2: the unit of measure is used as a suffix for the value
                        else:
                            for u in units:
                                if splitted[i].endswith(u):
                                    word = splitted[i].replace(u, '')
                                    # if the word without suffix is a dotted number, resolution found
                                    if (bool(re.match('^(?=.*[0-9])', word)) is True) and ('.' in word):
                                        camera['megapixels'] = word
                                    # if the word without suffix is a 0, take the previous one if it is a number
                                    elif word == '0' and splitted[i - 1].isdigit():
                                        camera['megapixels'] = splitted[i - 1] + '.0'
                                    # if the word without suffix is a number and the previous one is not a number,
                                    # resolution found
                                    elif word.isdigit() and splitted[i - 1].isdigit() is False:
                                        camera['megapixels'] = word + '.0'
                                    # if the word without suffix is a number and the one before is also a number (but
                                    # not the model), resolution found
                                    elif word.isdigit() and splitted[i - 1].isdigit() and splitted[i - 1] not in \
                                            camera['model'].split():
                                        camera['megapixels'] = splitted[i - 1] + '.' + word
                    else:
                        break

                # Add the current specification to the complete specifications list
                specifications.append(camera)

    # Transform specifications list into a data frame, sort it by brand and model, and print it to a .CSV file
    specifications = pd.DataFrame(specifications)
    specifications.set_index('id')
    specifications = specifications.sort_values(by=['brand', 'model'])
    specifications['megapixels'] = pd.to_numeric(specifications['megapixels'], errors='coerce')
    specifications = specifications[specifications['megapixels'].notnull()]
    specifications.to_csv('data/alaska_camera_no_nan_dataset.csv', index=False)


def blocking(ds_dict):
    # Generate blocks
    ds_name = 'alaska_camera_no_nan'
    blocks_path = "data/" + ds_name + "_blocks.txt"
    block_costs_path = "data/" + ds_name + "_block_costs.txt"
    record_blocks_path = "data/" + ds_name + "_record_blocks.txt"

    # For each specification, generate two special attributes:
    # BMA is a set composed of the letters of the brand and the ones of the model
    # BMN is a set composed of the letters of the brand and the numbers of the model
    for d in ds_dict:
        d['BMA'] = set()
        d['BMN'] = set()
        if d['brand'] != 'NaN' and d['model'] != 'NaN':
            tmp = [b for b in str(d['brand'])]
            d['BMA'].update(tmp + [m for m in str(d['model']) if m.isalpha()])
            d['BMN'].update(tmp + [m for m in str(d['model']) if m.isdigit()])
        d['BMA'] = ''.join(sorted(list(d['BMA'])))
        d['BMN'] = ''.join(sorted(list(d['BMN'])))

    # Inverted index on BMA and BMN: generate blocks
    blocks = dict()
    for d in ds_dict:
        if not d['BMA']:
            pass
        else:
            if d['BMA'] in blocks.keys():
                blocks[d['BMA']].append(d['id'])
            else:
                blocks.update({d['BMA']: [d['id']]})
            if d['BMN'] in blocks.keys():
                blocks[d['BMN']].append(d['id'])
            else:
                blocks.update({d['BMN']: [d['id']]})

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
    ds_generation()
    ds = pd.read_csv('data/alaska_camera_no_nan_dataset.csv')
    ds['megapixels'] = pd.to_numeric(ds['megapixels'], errors='coerce')
    for column in ds.columns:
        if ds[column].dtype == 'object':
            ds[column] = ds[column].fillna('NaN')
    gold = pd.read_csv('data/alaska_camera_gold.csv')
    gold = gold[gold['left_spec_id'].isin(ds['id'].tolist()) & gold['right_spec_id'].isin(ds['id'].tolist())]
    gold.to_csv('data/alaska_camera_no_nan_gold.csv', index=False)
    ds_dict = ds.to_dict('records')
    blocking(ds_dict)


if __name__ == "__main__":
    main()
