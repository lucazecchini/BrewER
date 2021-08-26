import pandas as pd
import random


class AlaskaCameraTask(object):
    # Identify each instance (query) using an index
    def __init__(self, index):
        self.counter = index

        # Define the path of the files to be used to save the results of the query
        self.query_output = "results/" + str(self.counter) + "_query.txt"
        self.lazy_output = "results/" + str(self.counter) + "_lazy.csv"
        self.eager_output = "results/" + str(self.counter) + "_eager.csv"
        self.query_details = "results/queries.csv"

        # Define if batch version is required
        self.batch = True

        # SELECT

        # Top-K query: define the number of entities (K) to be returned (if all the entities must be returned, set <= 0)
        self.top_k = 0

        # Indicate if the entities with null value of the ordering key must be returned or ignored
        self.ignore_null = True

        # Define the aggregation function to be used for each attribute (min, max, avg, sum, vote, random, concat)
        # For the ordering key only some aggregation functions (min, max, avg, vote) are supported
        random_aggregation = random.choice(['max', 'min'])
        self.aggregations = {'id': 'min', 'brand': 'vote', 'model': 'vote', 'megapixels': random_aggregation}

        # Define the attributes to be shown for the resulting entities
        self.attributes = ['brand', 'model', 'megapixels']

        # FROM

        # Choose the dataset to be used (among: alaska_camera, funding)
        self.ds_name = 'alaska_camera'

        # Define the path of the dataset: it must be a CSV file
        self.ds_path = "data/" + self.ds_name + "_dataset.csv"

        # Define the path of the ground truth (a CSV file containing the matching pairs - couples ordered by id)
        self.gold_path = "data/" + self.ds_name + "_gold.csv"

        # With blocking or without blocking?
        self.blocking = True
        self.blocks_path = "data/" + self.ds_name + "_blocks.txt"
        self.block_costs = "data/" + self.ds_name + "_block_costs.txt"
        self.record_blocks = "data/" + self.ds_name + "_record_blocks.txt"

        # WHERE

        # HAVING

        # Define HAVING conditions as attribute-value pairs (for LIKE situation)
        brands = ['canon', 'dahua', 'fuji', 'hikvision', 'kodak', 'nikon', 'olympus', 'panasonic', 'samsung', 'sony']
        minor_brands = ['argus', 'benq', 'casio', 'coleman', 'ge', 'gopro', 'hasselblad', 'howell', 'hp', 'intova',
                        'leica', 'lg', 'minolta', 'pentax', 'polaroid', 'ricoh', 'sanyo', 'sigma', 'toshiba', 'vivitar']
        all_brands = brands + minor_brands
        models = {'canon': ['a', 'd', 'elph', 'g', 'ixus', 'mark', 's', 'sd', 'sx', 't', 'xs', 'xt'],
                  'dahua': ['dh', 'ipc', 'hd', 'hf', 'sd'], 'fuji': ['ax', 'f', 'hs', 'jx', 's'],
                  'hikvision': ['cd', 'de', 'ds', 'f', 'is'], 'kodak': ['dc', 'dx', 'm', 'v', 'z'],
                  'nikon': ['100', 'aw', 'd', 'j', 'l', 'p', 's', 'v'],
                  'olympus': ['c', 'd', 'e', 'fe', 'sp', 'sz', 'tg', 'vg', 'vr', 'xz'],
                  'panasonic': ['dmc', 'fz', 'gf', 'gh', 'gx', 'lx', 'lz', 's', 'tz', 'x', 'z', 'zs'],
                  'samsung': ['gc', 'nx', 'pl', 'st', 'wb'],
                  'sony': ['tvl', 'a', 'dsc', 'fd', 'pj', 'hx', 'nex', 'slt']}
        random_brand = random.choice(brands)
        random_model = random.choice(models[random_brand])
        self.having = [('brand', random_brand), ('model', random_model)]
        # self.having = [('brand', all_brands[self.counter - 1]), ('brand', all_brands[self.counter - 1])]

        # Define the logical operator to be used for HAVING conditions (and/or)
        self.operator = 'and'

        # ORDER BY

        # Define the numeric attribute to be used as ordering key (OK) and the ordering mode (asc or desc)
        self.ordering_key = 'megapixels'
        if random_aggregation == 'max':
            self.ordering_mode = 'asc'
        else:
            self.ordering_mode = 'desc'

        # Get the query in SQL
        select_clause = "SELECT "
        if self.top_k > 0:
            select_clause = select_clause + "TOP(" + str(self.top_k) + ") "
        for i in range(0, len(self.attributes)):
            select_clause = select_clause + self.aggregations[self.attributes[i]] + "(" + self.attributes[i] + ")"
            if i < len(self.attributes) - 1:
                select_clause = select_clause + ", "
            else:
                select_clause = select_clause + "\n"
        from_clause = "FROM " + self.ds_name + "\n"
        group_by_clause = "GROUP BY _\n"
        having_clause = "HAVING "
        for i in range(0, len(self.having)):
            having_clause = having_clause + self.aggregations[self.having[i][0]] + \
                            "(" + self.having[i][0] + ") LIKE '%" + self.having[i][1] + "%'"
            if i < len(self.having) - 1:
                having_clause = having_clause + " " + self.operator + " "
            else:
                having_clause = having_clause + "\n"
        order_by_clause = "ORDER BY " + self.aggregations[self.ordering_key] + "(" + self.ordering_key + \
                          ") " + self.ordering_mode + "\n"
        self.query = (select_clause + from_clause + group_by_clause + having_clause + order_by_clause).upper()

    # Define the query for batch ER algorithm
    # It is a post filtering (application of HAVING clauses on solved entity), so AND/OR are maintained
    def batch_query(self, entities):
        if self.ordering_mode == 'asc':
            ascending = True
        else:
            ascending = False
        if self.operator == 'and':
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)
        else:
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)

    # Define the preliminary filtering for BrewER
    def brewer_pre_filtering(self, records, solved):
        # To reduce the number of comparisons, if HAVING conditions are in AND...
        # ...we check that all conditions are separately satisfied by at least one record appearing the block
        if self.operator == 'and':
            # If we consider already solved records (no neighbours), simply filter them in AND
            if solved:
                return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                                   (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
            # Otherwise, check that all conditions are separately satisfied (if not, return an empty DataFrame)
            else:
                condition = records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False))]
                if len(condition) == 0:
                    return condition
                condition = records.loc[(records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
                if len(condition) == 0:
                    return condition
            # If the conditions are all satisfied, proceed as in OR case

        # Otherwise, in OR case, check that at least one of the conditions is satisfied by the records of the block
        return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                           (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]

    # Define the post filtering for BrewER (AND/OR are maintained)
    def brewer_post_filtering(self, entity):
        if self.ignore_null:
            if pd.isna(entity[self.ordering_key]):
                return False
        if self.operator == 'and':
            return self.having[0][1] in str(entity[self.having[0][0]]) and \
                   self.having[1][1] in str(entity[self.having[1][0]])
        else:
            return self.having[0][1] in str(entity[self.having[0][0]]) or \
                   self.having[1][1] in str(entity[self.having[1][0]])


class AlaskaCameraNoNanTask(object):
    # Identify each instance (query) using an index
    def __init__(self, index):
        self.counter = index

        # Define the path of the files to be used to save the results of the query
        self.query_output = "results/" + str(self.counter) + "_query.txt"
        self.lazy_output = "results/" + str(self.counter) + "_lazy.csv"
        self.eager_output = "results/" + str(self.counter) + "_eager.csv"
        self.query_details = "results/queries.csv"

        # Define if batch version is required
        self.batch = True

        # SELECT

        # Top-K query: define the number of entities (K) to be returned (if all the entities must be returned, set <= 0)
        self.top_k = 0

        # Indicate if the entities with null value of the ordering key must be returned or ignored
        self.ignore_null = True

        # Define the aggregation function to be used for each attribute (min, max, avg, sum, vote, random, concat)
        # For the ordering key only some aggregation functions (min, max, avg, vote) are supported
        random_aggregation = random.choice(['max', 'min'])
        self.aggregations = {'id': 'min', 'brand': 'vote', 'model': 'vote', 'megapixels': random_aggregation}

        # Define the attributes to be shown for the resulting entities
        self.attributes = ['brand', 'model', 'megapixels']

        # FROM

        # Choose the dataset to be used (among: alaska_camera, funding)
        self.ds_name = 'alaska_camera_no_nan'

        # Define the path of the dataset: it must be a CSV file
        self.ds_path = "data/" + self.ds_name + "_dataset.csv"

        # Define the path of the ground truth (a CSV file containing the matching pairs - couples ordered by id)
        self.gold_path = "data/" + self.ds_name + "_gold.csv"

        # With blocking or without blocking?
        self.blocking = True
        self.blocks_path = "data/" + self.ds_name + "_blocks.txt"
        self.block_costs = "data/" + self.ds_name + "_block_costs.txt"
        self.record_blocks = "data/" + self.ds_name + "_record_blocks.txt"

        # WHERE

        # HAVING

        # Define HAVING conditions as attribute-value pairs (for LIKE situation)
        brands = ['canon', 'dahua', 'fuji', 'hikvision', 'kodak', 'nikon', 'olympus', 'panasonic', 'samsung', 'sony']
        minor_brands = ['argus', 'benq', 'casio', 'coleman', 'ge', 'gopro', 'hasselblad', 'howell', 'hp', 'intova',
                        'leica', 'lg', 'minolta', 'pentax', 'polaroid', 'ricoh', 'sanyo', 'sigma', 'toshiba', 'vivitar']
        all_brands = brands + minor_brands
        models = {'canon': ['a', 'd', 'elph', 'g', 'ixus', 'mark', 's', 'sd', 'sx', 't', 'xs', 'xt'],
                  'dahua': ['dh', 'ipc', 'hd', 'hf', 'sd'], 'fuji': ['ax', 'f', 'hs', 'jx', 's'],
                  'hikvision': ['cd', 'de', 'ds', 'f', 'is'], 'kodak': ['dc', 'dx', 'm', 'v', 'z'],
                  'nikon': ['100', 'aw', 'd', 'j', 'l', 'p', 's', 'v'],
                  'olympus': ['c', 'd', 'e', 'fe', 'sp', 'sz', 'tg', 'vg', 'vr', 'xz'],
                  'panasonic': ['dmc', 'fz', 'gf', 'gh', 'gx', 'lx', 'lz', 's', 'tz', 'x', 'z', 'zs'],
                  'samsung': ['gc', 'nx', 'pl', 'st', 'wb'],
                  'sony': ['tvl', 'a', 'dsc', 'fd', 'pj', 'hx', 'nex', 'slt']}
        random_brand = random.choice(brands)
        random_model = random.choice(models[random_brand])
        self.having = [('brand', random_brand), ('model', random_model)]
        # self.having = [('brand', all_brands[self.counter - 1]), ('brand', all_brands[self.counter - 1])]

        # Define the logical operator to be used for HAVING conditions (and/or)
        self.operator = 'and'

        # ORDER BY

        # Define the numeric attribute to be used as ordering key (OK) and the ordering mode (asc or desc)
        self.ordering_key = 'megapixels'
        if random_aggregation == 'max':
            self.ordering_mode = 'asc'
        else:
            self.ordering_mode = 'desc'

        # Get the query in SQL
        select_clause = "SELECT "
        if self.top_k > 0:
            select_clause = select_clause + "TOP(" + str(self.top_k) + ") "
        for i in range(0, len(self.attributes)):
            select_clause = select_clause + self.aggregations[self.attributes[i]] + "(" + self.attributes[i] + ")"
            if i < len(self.attributes) - 1:
                select_clause = select_clause + ", "
            else:
                select_clause = select_clause + "\n"
        from_clause = "FROM " + self.ds_name + "\n"
        group_by_clause = "GROUP BY _\n"
        having_clause = "HAVING "
        for i in range(0, len(self.having)):
            having_clause = having_clause + self.aggregations[self.having[i][0]] + \
                            "(" + self.having[i][0] + ") LIKE '%" + self.having[i][1] + "%'"
            if i < len(self.having) - 1:
                having_clause = having_clause + " " + self.operator + " "
            else:
                having_clause = having_clause + "\n"
        order_by_clause = "ORDER BY " + self.aggregations[self.ordering_key] + "(" + self.ordering_key + \
                          ") " + self.ordering_mode + "\n"
        self.query = (select_clause + from_clause + group_by_clause + having_clause + order_by_clause).upper()

    # Define the query for batch ER algorithm
    # It is a post filtering (application of HAVING clauses on solved entity), so AND/OR are maintained
    def batch_query(self, entities):
        if self.ordering_mode == 'asc':
            ascending = True
        else:
            ascending = False
        if self.operator == 'and':
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)
        else:
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)

    # Define the preliminary filtering for BrewER
    def brewer_pre_filtering(self, records, solved):
        # To reduce the number of comparisons, if HAVING conditions are in AND...
        # ...we check that all conditions are separately satisfied by at least one record appearing the block
        if self.operator == 'and':
            # If we consider already solved records (no neighbours), simply filter them in AND
            if solved:
                return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                                   (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
            # Otherwise, check that all conditions are separately satisfied (if not, return an empty DataFrame)
            else:
                condition = records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False))]
                if len(condition) == 0:
                    return condition
                condition = records.loc[(records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
                if len(condition) == 0:
                    return condition
            # If the conditions are all satisfied, proceed as in OR case

        # Otherwise, in OR case, check that at least one of the conditions is satisfied by the records of the block
        return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                           (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]

    # Define the post filtering for BrewER (AND/OR are maintained)
    def brewer_post_filtering(self, entity):
        if self.ignore_null:
            if pd.isna(entity[self.ordering_key]):
                return False
        if self.operator == 'and':
            return self.having[0][1] in str(entity[self.having[0][0]]) and \
                   self.having[1][1] in str(entity[self.having[1][0]])
        else:
            return self.having[0][1] in str(entity[self.having[0][0]]) or \
                   self.having[1][1] in str(entity[self.having[1][0]])


class AltosightTask(object):
    # Identify each instance (query) using an index
    def __init__(self, index):
        self.counter = index

        # Define the path of the files to be used to save the results of the query
        self.query_output = "results/" + str(self.counter) + "_query.txt"
        self.lazy_output = "results/" + str(self.counter) + "_lazy.csv"
        self.eager_output = "results/" + str(self.counter) + "_eager.csv"
        self.query_details = "results/queries.csv"

        # Define if batch version is required
        self.batch = True

        # SELECT

        # Top-K query: define the number of entities (K) to be returned (if all the entities must be returned, set <= 0)
        self.top_k = 0

        # Indicate if the entities with null value of the ordering key must be returned or ignored
        self.ignore_null = True

        # Define the aggregation function to be used for each attribute (min, max, avg, sum, vote, random, concat)
        # For the ordering key only some aggregation functions (min, max, avg, vote) are supported
        random_aggregation = random.choice(['max', 'min'])
        self.aggregations = {'id': 'min', 'name': 'vote', 'brand': 'vote', 'size': 'vote', 'size_num': 'max',
                             'price': random_aggregation}

        # Define the attributes to be shown for the resulting entities
        self.attributes = ['name', 'brand', 'size', 'size_num', 'price']

        # FROM

        # Choose the dataset to be used (among: alaska_camera, funding)
        self.ds_name = 'altosight'

        # Define the path of the dataset: it must be a CSV file
        self.ds_path = "data/" + self.ds_name + "_dataset.csv"

        # Define the path of the ground truth (a CSV file containing the matching pairs - couples ordered by id)
        self.gold_path = "data/" + self.ds_name + "_gold.csv"

        # With blocking or without blocking?
        self.blocking = True
        self.blocks_path = "data/" + self.ds_name + "_blocks.txt"
        self.block_costs = "data/" + self.ds_name + "_block_costs.txt"
        self.record_blocks = "data/" + self.ds_name + "_record_blocks.txt"

        # WHERE

        # HAVING

        # Define HAVING conditions as attribute-value pairs (for LIKE situation)

        brands = ['intenso', 'kingston', 'lexar', 'pny', 'samsung', 'sandisk', 'sony', 'toshiba', 'transcend']
        sizes = ['4', '8', '16', '32', '64', '128', '256', '512']
        random_brand = random.choice(brands)
        random_size = random.choice(sizes)
        self.having = [('brand', random.choice(brands)), ('brand', random.choice(brands))]
        # self.having = [('brand', all_brands[self.counter - 1]), ('brand', all_brands[self.counter - 1])]

        # Define the logical operator to be used for HAVING conditions (and/or)
        self.operator = 'or'

        # ORDER BY

        # Define the numeric attribute to be used as ordering key (OK) and the ordering mode (asc or desc)
        self.ordering_key = 'price'
        if random_aggregation == 'max':
            self.ordering_mode = 'desc'
        else:
            self.ordering_mode = 'asc'

        # Get the query in SQL
        select_clause = "SELECT "
        if self.top_k > 0:
            select_clause = select_clause + "TOP(" + str(self.top_k) + ") "
        for i in range(0, len(self.attributes)):
            select_clause = select_clause + self.aggregations[self.attributes[i]] + "(" + self.attributes[i] + ")"
            if i < len(self.attributes) - 1:
                select_clause = select_clause + ", "
            else:
                select_clause = select_clause + "\n"
        from_clause = "FROM " + self.ds_name + "\n"
        group_by_clause = "GROUP BY _\n"
        having_clause = "HAVING "
        for i in range(0, len(self.having)):
            having_clause = having_clause + self.aggregations[self.having[i][0]] + \
                            "(" + self.having[i][0] + ") LIKE '%" + self.having[i][1] + "%'"
            if i < len(self.having) - 1:
                having_clause = having_clause + " " + self.operator + " "
            else:
                having_clause = having_clause + "\n"
        order_by_clause = "ORDER BY " + self.aggregations[self.ordering_key] + "(" + self.ordering_key + \
                          ") " + self.ordering_mode + "\n"
        self.query = (select_clause + from_clause + group_by_clause + having_clause + order_by_clause).upper()

    # Define the query for batch ER algorithm
    # It is a post filtering (application of HAVING clauses on solved entity), so AND/OR are maintained
    def batch_query(self, entities):
        if self.ordering_mode == 'asc':
            ascending = True
        else:
            ascending = False
        if self.operator == 'and':
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)
        else:
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)

    # Define the preliminary filtering for BrewER
    def brewer_pre_filtering(self, records, solved):
        # To reduce the number of comparisons, if HAVING conditions are in AND...
        # ...we check that all conditions are separately satisfied by at least one record appearing the block
        if self.operator == 'and':
            # If we consider already solved records (no neighbours), simply filter them in AND
            if solved:
                return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                                   (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
            # Otherwise, check that all conditions are separately satisfied (if not, return an empty DataFrame)
            else:
                condition = records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False))]
                if len(condition) == 0:
                    return condition
                condition = records.loc[(records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
                if len(condition) == 0:
                    return condition
            # If the conditions are all satisfied, proceed as in OR case

        # Otherwise, in OR case, check that at least one of the conditions is satisfied by the records of the block
        return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                           (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]

    # Define the post filtering for BrewER (AND/OR are maintained)
    def brewer_post_filtering(self, entity):
        if self.ignore_null:
            if pd.isna(entity[self.ordering_key]):
                return False
        if self.operator == 'and':
            return self.having[0][1] in str(entity[self.having[0][0]]) and \
                   self.having[1][1] in str(entity[self.having[1][0]])
        else:
            return self.having[0][1] in str(entity[self.having[0][0]]) or \
                   self.having[1][1] in str(entity[self.having[1][0]])


class AltosightNoNanTask(object):
    # Identify each instance (query) using an index
    def __init__(self, index):
        self.counter = index

        # Define the path of the files to be used to save the results of the query
        self.query_output = "results/" + str(self.counter) + "_query.txt"
        self.lazy_output = "results/" + str(self.counter) + "_lazy.csv"
        self.eager_output = "results/" + str(self.counter) + "_eager.csv"
        self.query_details = "results/queries.csv"

        # Define if batch version is required
        self.batch = True

        # SELECT

        # Top-K query: define the number of entities (K) to be returned (if all the entities must be returned, set <= 0)
        self.top_k = 0

        # Indicate if the entities with null value of the ordering key must be returned or ignored
        self.ignore_null = True

        # Define the aggregation function to be used for each attribute (min, max, avg, sum, vote, random, concat)
        # For the ordering key only some aggregation functions (min, max, avg, vote) are supported
        random_aggregation = random.choice(['max', 'min'])
        self.aggregations = {'id': 'min', 'name': 'vote', 'brand': 'vote', 'size': 'vote', 'size_num': 'max',
                             'price': random_aggregation}

        # Define the attributes to be shown for the resulting entities
        self.attributes = ['name', 'brand', 'size', 'size_num', 'price']

        # FROM

        # Choose the dataset to be used (among: alaska_camera, funding)
        self.ds_name = 'altosight_no_nan'

        # Define the path of the dataset: it must be a CSV file
        self.ds_path = "data/" + self.ds_name + "_dataset.csv"

        # Define the path of the ground truth (a CSV file containing the matching pairs - couples ordered by id)
        self.gold_path = "data/" + self.ds_name + "_gold.csv"

        # With blocking or without blocking?
        self.blocking = True
        self.blocks_path = "data/" + self.ds_name + "_blocks.txt"
        self.block_costs = "data/" + self.ds_name + "_block_costs.txt"
        self.record_blocks = "data/" + self.ds_name + "_record_blocks.txt"

        # WHERE

        # HAVING

        # Define HAVING conditions as attribute-value pairs (for LIKE situation)

        brands = ['intenso', 'kingston', 'lexar', 'pny', 'samsung', 'sandisk', 'sony', 'toshiba', 'transcend']
        sizes = ['4', '8', '16', '32', '64', '128', '256', '512']
        random_brand = random.choice(brands)
        random_size = random.choice(sizes)
        self.having = [('brand', random_brand), ('size', random_size)]
        # self.having = [('brand', all_brands[self.counter - 1]), ('brand', all_brands[self.counter - 1])]

        # Define the logical operator to be used for HAVING conditions (and/or)
        self.operator = 'and'

        # ORDER BY

        # Define the numeric attribute to be used as ordering key (OK) and the ordering mode (asc or desc)
        self.ordering_key = 'price'
        if random_aggregation == 'max':
            self.ordering_mode = 'asc'
        else:
            self.ordering_mode = 'desc'

        # Get the query in SQL
        select_clause = "SELECT "
        if self.top_k > 0:
            select_clause = select_clause + "TOP(" + str(self.top_k) + ") "
        for i in range(0, len(self.attributes)):
            select_clause = select_clause + self.aggregations[self.attributes[i]] + "(" + self.attributes[i] + ")"
            if i < len(self.attributes) - 1:
                select_clause = select_clause + ", "
            else:
                select_clause = select_clause + "\n"
        from_clause = "FROM " + self.ds_name + "\n"
        group_by_clause = "GROUP BY _\n"
        having_clause = "HAVING "
        for i in range(0, len(self.having)):
            having_clause = having_clause + self.aggregations[self.having[i][0]] + \
                            "(" + self.having[i][0] + ") LIKE '%" + self.having[i][1] + "%'"
            if i < len(self.having) - 1:
                having_clause = having_clause + " " + self.operator + " "
            else:
                having_clause = having_clause + "\n"
        order_by_clause = "ORDER BY " + self.aggregations[self.ordering_key] + "(" + self.ordering_key + \
                          ") " + self.ordering_mode + "\n"
        self.query = (select_clause + from_clause + group_by_clause + having_clause + order_by_clause).upper()

    # Define the query for batch ER algorithm
    # It is a post filtering (application of HAVING clauses on solved entity), so AND/OR are maintained
    def batch_query(self, entities):
        if self.ordering_mode == 'asc':
            ascending = True
        else:
            ascending = False
        if self.operator == 'and':
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)
        else:
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)

    # Define the preliminary filtering for BrewER
    def brewer_pre_filtering(self, records, solved):
        # To reduce the number of comparisons, if HAVING conditions are in AND...
        # ...we check that all conditions are separately satisfied by at least one record appearing the block
        if self.operator == 'and':
            # If we consider already solved records (no neighbours), simply filter them in AND
            if solved:
                return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                                   (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
            # Otherwise, check that all conditions are separately satisfied (if not, return an empty DataFrame)
            else:
                condition = records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False))]
                if len(condition) == 0:
                    return condition
                condition = records.loc[(records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
                if len(condition) == 0:
                    return condition
            # If the conditions are all satisfied, proceed as in OR case

        # Otherwise, in OR case, check that at least one of the conditions is satisfied by the records of the block
        return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                           (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]

    # Define the post filtering for BrewER (AND/OR are maintained)
    def brewer_post_filtering(self, entity):
        if self.ignore_null:
            if pd.isna(entity[self.ordering_key]):
                return False
        if self.operator == 'and':
            return self.having[0][1] in str(entity[self.having[0][0]]) and \
                   self.having[1][1] in str(entity[self.having[1][0]])
        else:
            return self.having[0][1] in str(entity[self.having[0][0]]) or \
                   self.having[1][1] in str(entity[self.having[1][0]])


class AltosightSigmodTask(object):
    # Identify each instance (query) using an index
    def __init__(self, index):
        self.counter = index

        # Define the path of the files to be used to save the results of the query
        self.query_output = "results/" + str(self.counter) + "_query.txt"
        self.lazy_output = "results/" + str(self.counter) + "_lazy.csv"
        self.eager_output = "results/" + str(self.counter) + "_eager.csv"
        self.query_details = "results/queries.csv"

        # Define if batch version is required
        self.batch = True

        # SELECT

        # Top-K query: define the number of entities (K) to be returned (if all the entities must be returned, set <= 0)
        self.top_k = 0

        # Indicate if the entities with null value of the ordering key must be returned or ignored
        self.ignore_null = True

        # Define the aggregation function to be used for each attribute (min, max, avg, sum, vote, random, concat)
        # For the ordering key only some aggregation functions (min, max, avg, vote) are supported
        random_aggregation = random.choice(['max', 'min'])
        self.aggregations = {'id': 'min', 'name': 'vote', 'brand': 'vote', 'size': 'vote', 'size_num': 'max',
                             'price': random_aggregation}

        # Define the attributes to be shown for the resulting entities
        self.attributes = ['name', 'brand', 'size', 'size_num', 'price']

        # FROM

        # Choose the dataset to be used (among: alaska_camera, funding)
        self.ds_name = 'altosight_sigmod'

        # Define the path of the dataset: it must be a CSV file
        self.ds_path = "data/" + self.ds_name + "_dataset.csv"

        # Define the path of the ground truth (a CSV file containing the matching pairs - couples ordered by id)
        self.gold_path = "data/" + self.ds_name + "_gold.csv"

        # With blocking or without blocking?
        self.blocking = False
        self.blocks_path = "data/" + self.ds_name + "_blocks.txt"
        self.block_costs = "data/" + self.ds_name + "_block_costs.txt"
        self.record_blocks = "data/" + self.ds_name + "_record_blocks.txt"

        # WHERE

        # HAVING

        # Define HAVING conditions as attribute-value pairs (for LIKE situation)

        brands = ['intenso', 'kingston', 'lexar', 'pny', 'samsung', 'sandisk', 'sony', 'toshiba', 'transcend']
        sizes = ['4', '8', '16', '32', '64', '128', '256', '512']
        random_brand = random.choice(brands)
        random_size = random.choice(sizes)
        self.having = [('brand', random_brand), ('size', random_size)]
        # self.having = [('brand', all_brands[self.counter - 1]), ('brand', all_brands[self.counter - 1])]

        # Define the logical operator to be used for HAVING conditions (and/or)
        self.operator = 'and'

        # ORDER BY

        # Define the numeric attribute to be used as ordering key (OK) and the ordering mode (asc or desc)
        self.ordering_key = 'price'
        if random_aggregation == 'max':
            self.ordering_mode = 'desc'
        else:
            self.ordering_mode = 'asc'

        # Get the query in SQL
        select_clause = "SELECT "
        if self.top_k > 0:
            select_clause = select_clause + "TOP(" + str(self.top_k) + ") "
        for i in range(0, len(self.attributes)):
            select_clause = select_clause + self.aggregations[self.attributes[i]] + "(" + self.attributes[i] + ")"
            if i < len(self.attributes) - 1:
                select_clause = select_clause + ", "
            else:
                select_clause = select_clause + "\n"
        from_clause = "FROM " + self.ds_name + "\n"
        group_by_clause = "GROUP BY _\n"
        having_clause = "HAVING "
        for i in range(0, len(self.having)):
            having_clause = having_clause + self.aggregations[self.having[i][0]] + \
                            "(" + self.having[i][0] + ") LIKE '%" + self.having[i][1] + "%'"
            if i < len(self.having) - 1:
                having_clause = having_clause + " " + self.operator + " "
            else:
                having_clause = having_clause + "\n"
        order_by_clause = "ORDER BY " + self.aggregations[self.ordering_key] + "(" + self.ordering_key + \
                          ") " + self.ordering_mode + "\n"
        self.query = (select_clause + from_clause + group_by_clause + having_clause + order_by_clause).upper()

    # Define the query for batch ER algorithm
    # It is a post filtering (application of HAVING clauses on solved entity), so AND/OR are maintained
    def batch_query(self, entities):
        if self.ordering_mode == 'asc':
            ascending = True
        else:
            ascending = False
        if self.operator == 'and':
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)
        else:
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)

    # Define the preliminary filtering for BrewER
    def brewer_pre_filtering(self, records, solved):
        # To reduce the number of comparisons, if HAVING conditions are in AND...
        # ...we check that all conditions are separately satisfied by at least one record appearing the block
        if self.operator == 'and':
            # If we consider already solved records (no neighbours), simply filter them in AND
            if solved:
                return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                                   (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
            # Otherwise, check that all conditions are separately satisfied (if not, return an empty DataFrame)
            else:
                condition = records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False))]
                if len(condition) == 0:
                    return condition
                condition = records.loc[(records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
                if len(condition) == 0:
                    return condition
            # If the conditions are all satisfied, proceed as in OR case

        # Otherwise, in OR case, check that at least one of the conditions is satisfied by the records of the block
        return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                           (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]

    # Define the post filtering for BrewER (AND/OR are maintained)
    def brewer_post_filtering(self, entity):
        if self.ignore_null:
            if pd.isna(entity[self.ordering_key]):
                return False
        if self.operator == 'and':
            return self.having[0][1] in str(entity[self.having[0][0]]) and \
                   self.having[1][1] in str(entity[self.having[1][0]])
        else:
            return self.having[0][1] in str(entity[self.having[0][0]]) or \
                   self.having[1][1] in str(entity[self.having[1][0]])


class AltosightSigmodNoNanTask(object):
    # Identify each instance (query) using an index
    def __init__(self, index):
        self.counter = index

        # Define the path of the files to be used to save the results of the query
        self.query_output = "results/" + str(self.counter) + "_query.txt"
        self.lazy_output = "results/" + str(self.counter) + "_lazy.csv"
        self.eager_output = "results/" + str(self.counter) + "_eager.csv"
        self.query_details = "results/queries.csv"

        # Define if batch version is required
        self.batch = True

        # SELECT

        # Top-K query: define the number of entities (K) to be returned (if all the entities must be returned, set <= 0)
        self.top_k = 0

        # Indicate if the entities with null value of the ordering key must be returned or ignored
        self.ignore_null = True

        # Define the aggregation function to be used for each attribute (min, max, avg, sum, vote, random, concat)
        # For the ordering key only some aggregation functions (min, max, avg, vote) are supported
        random_aggregation = random.choice(['max', 'min'])
        self.aggregations = {'id': 'min', 'name': 'vote', 'brand': 'vote', 'size': 'vote', 'size_num': 'max',
                             'price': random_aggregation}

        # Define the attributes to be shown for the resulting entities
        self.attributes = ['name', 'brand', 'size', 'size_num', 'price']

        # FROM

        # Choose the dataset to be used (among: alaska_camera, funding)
        self.ds_name = 'altosight_sigmod_no_nan'

        # Define the path of the dataset: it must be a CSV file
        self.ds_path = "data/" + self.ds_name + "_dataset.csv"

        # Define the path of the ground truth (a CSV file containing the matching pairs - couples ordered by id)
        self.gold_path = "data/" + self.ds_name + "_gold.csv"

        # With blocking or without blocking?
        self.blocking = False
        self.blocks_path = "data/" + self.ds_name + "_blocks.txt"
        self.block_costs = "data/" + self.ds_name + "_block_costs.txt"
        self.record_blocks = "data/" + self.ds_name + "_record_blocks.txt"

        # WHERE

        # HAVING

        # Define HAVING conditions as attribute-value pairs (for LIKE situation)

        brands = ['intenso', 'kingston', 'lexar', 'pny', 'samsung', 'sandisk', 'sony', 'toshiba', 'transcend']
        sizes = ['4', '8', '16', '32', '64', '128', '256', '512']
        random_brand = random.choice(brands)
        random_size = random.choice(sizes)
        self.having = [('brand', random.choice(brands)), ('brand', random.choice(brands))]
        # self.having = [('brand', all_brands[self.counter - 1]), ('brand', all_brands[self.counter - 1])]

        # Define the logical operator to be used for HAVING conditions (and/or)
        self.operator = 'or'

        # ORDER BY

        # Define the numeric attribute to be used as ordering key (OK) and the ordering mode (asc or desc)
        self.ordering_key = 'price'
        if random_aggregation == 'max':
            self.ordering_mode = 'asc'
        else:
            self.ordering_mode = 'desc'

        # Get the query in SQL
        select_clause = "SELECT "
        if self.top_k > 0:
            select_clause = select_clause + "TOP(" + str(self.top_k) + ") "
        for i in range(0, len(self.attributes)):
            select_clause = select_clause + self.aggregations[self.attributes[i]] + "(" + self.attributes[i] + ")"
            if i < len(self.attributes) - 1:
                select_clause = select_clause + ", "
            else:
                select_clause = select_clause + "\n"
        from_clause = "FROM " + self.ds_name + "\n"
        group_by_clause = "GROUP BY _\n"
        having_clause = "HAVING "
        for i in range(0, len(self.having)):
            having_clause = having_clause + self.aggregations[self.having[i][0]] + \
                            "(" + self.having[i][0] + ") LIKE '%" + self.having[i][1] + "%'"
            if i < len(self.having) - 1:
                having_clause = having_clause + " " + self.operator + " "
            else:
                having_clause = having_clause + "\n"
        order_by_clause = "ORDER BY " + self.aggregations[self.ordering_key] + "(" + self.ordering_key + \
                          ") " + self.ordering_mode + "\n"
        self.query = (select_clause + from_clause + group_by_clause + having_clause + order_by_clause).upper()

    # Define the query for batch ER algorithm
    # It is a post filtering (application of HAVING clauses on solved entity), so AND/OR are maintained
    def batch_query(self, entities):
        if self.ordering_mode == 'asc':
            ascending = True
        else:
            ascending = False
        if self.operator == 'and':
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)
        else:
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)

    # Define the preliminary filtering for BrewER
    def brewer_pre_filtering(self, records, solved):
        # To reduce the number of comparisons, if HAVING conditions are in AND...
        # ...we check that all conditions are separately satisfied by at least one record appearing the block
        if self.operator == 'and':
            # If we consider already solved records (no neighbours), simply filter them in AND
            if solved:
                return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                                   (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
            # Otherwise, check that all conditions are separately satisfied (if not, return an empty DataFrame)
            else:
                condition = records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False))]
                if len(condition) == 0:
                    return condition
                condition = records.loc[(records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
                if len(condition) == 0:
                    return condition
            # If the conditions are all satisfied, proceed as in OR case

        # Otherwise, in OR case, check that at least one of the conditions is satisfied by the records of the block
        return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                           (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]

    # Define the post filtering for BrewER (AND/OR are maintained)
    def brewer_post_filtering(self, entity):
        if self.ignore_null:
            if pd.isna(entity[self.ordering_key]):
                return False
        if self.operator == 'and':
            return self.having[0][1] in str(entity[self.having[0][0]]) and \
                   self.having[1][1] in str(entity[self.having[1][0]])
        else:
            return self.having[0][1] in str(entity[self.having[0][0]]) or \
                   self.having[1][1] in str(entity[self.having[1][0]])


class FundingTask(object):
    # Identify each instance (query) using an index
    def __init__(self, index):
        self.counter = index

        # Define the path of the files to be used to save the results of the query
        self.query_output = "results/" + str(self.counter) + "_query.txt"
        self.lazy_output = "results/" + str(self.counter) + "_lazy.csv"
        self.eager_output = "results/" + str(self.counter) + "_eager.csv"
        self.query_details = "results/queries.csv"

        # Define if batch version is required
        self.batch = True

        # SELECT

        # Top-K query: define the number of entities (K) to be returned (if all the entities must be returned, set <= 0)
        self.top_k = 0

        # Indicate if the entities with null value of the ordering key must be returned or ignored
        self.ignore_null = True

        # Define the aggregation function to be used for each attribute (min, max, avg, sum, vote, random, concat)
        # For the ordering key only some aggregation functions (min, max, avg, vote) are supported
        random_aggregation = random.choice(['max', 'min'])
        self.aggregations = {'id': 'min', 'legal_name': 'vote', 'address': 'vote', 'source': 'vote',
                             'council_member': 'vote', 'amount': random_aggregation}

        # Define the attributes to be shown for the resulting entities
        self.attributes = ['legal_name', 'address', 'source', 'council_member', 'amount']

        # FROM

        # Choose the dataset to be used (among: alaska_camera, funding)
        self.ds_name = 'funding'

        # Define the path of the dataset: it must be a CSV file
        self.ds_path = "data/" + self.ds_name + "_dataset.csv"

        # Define the path of the ground truth (a CSV file containing the matching pairs - couples ordered by id)
        self.gold_path = "data/" + self.ds_name + "_gold.csv"

        # With blocking or without blocking?
        self.blocking = True
        self.blocks_path = "data/" + self.ds_name + "_blocks.txt"
        self.block_costs = "data/" + self.ds_name + "_block_costs.txt"
        self.record_blocks = "data/" + self.ds_name + "_record_blocks.txt"

        # WHERE

        # HAVING

        # Define HAVING conditions as attribute-value pairs (for LIKE situation)
        sources = ['aging', 'aids', 'boro', 'casa', 'food', 'health', 'local', 'youth']
        legal_name = ['asian', 'association', 'christian', 'community', 'council', 'foundation', 'jewish', 'service']
        random_source = random.choice(sources)
        random_name = random.choice(legal_name)
        self.having = [('source', random.choice(sources)), ('source', random.choice(sources))]
        # self.having = [('brand', all_brands[self.counter - 1]), ('brand', all_brands[self.counter - 1])]

        # Define the logical operator to be used for HAVING conditions (and/or)
        self.operator = 'or'

        # ORDER BY

        # Define the numeric attribute to be used as ordering key (OK) and the ordering mode (asc or desc)
        self.ordering_key = 'amount'
        if random_aggregation == 'max':
            self.ordering_mode = 'desc'
        else:
            self.ordering_mode = 'asc'

        # Get the query in SQL
        select_clause = "SELECT "
        if self.top_k > 0:
            select_clause = select_clause + "TOP(" + str(self.top_k) + ") "
        for i in range(0, len(self.attributes)):
            select_clause = select_clause + self.aggregations[self.attributes[i]] + "(" + self.attributes[i] + ")"
            if i < len(self.attributes) - 1:
                select_clause = select_clause + ", "
            else:
                select_clause = select_clause + "\n"
        from_clause = "FROM " + self.ds_name + "\n"
        group_by_clause = "GROUP BY _\n"
        having_clause = "HAVING "
        for i in range(0, len(self.having)):
            having_clause = having_clause + self.aggregations[self.having[i][0]] + \
                            "(" + self.having[i][0] + ") LIKE '%" + self.having[i][1] + "%'"
            if i < len(self.having) - 1:
                having_clause = having_clause + " " + self.operator + " "
            else:
                having_clause = having_clause + "\n"
        order_by_clause = "ORDER BY " + self.aggregations[self.ordering_key] + "(" + self.ordering_key + \
                          ") " + self.ordering_mode + "\n"
        self.query = (select_clause + from_clause + group_by_clause + having_clause + order_by_clause).upper()

    # Define the query for batch ER algorithm
    # It is a post filtering (application of HAVING clauses on solved entity), so AND/OR are maintained
    def batch_query(self, entities):
        if self.ordering_mode == 'asc':
            ascending = True
        else:
            ascending = False
        if self.operator == 'and':
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)
        else:
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)

    # Define the preliminary filtering for BrewER
    def brewer_pre_filtering(self, records, solved):
        # To reduce the number of comparisons, if HAVING conditions are in AND...
        # ...we check that all conditions are separately satisfied by at least one record appearing the block
        if self.operator == 'and':
            # If we consider already solved records (no neighbours), simply filter them in AND
            if solved:
                return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                                   (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
            # Otherwise, check that all conditions are separately satisfied (if not, return an empty DataFrame)
            else:
                condition = records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False))]
                if len(condition) == 0:
                    return condition
                condition = records.loc[(records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
                if len(condition) == 0:
                    return condition
            # If the conditions are all satisfied, proceed as in OR case

        # Otherwise, in OR case, check that at least one of the conditions is satisfied by the records of the block
        return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                           (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]

    # Define the post filtering for BrewER (AND/OR are maintained)
    def brewer_post_filtering(self, entity):
        if self.ignore_null:
            if pd.isna(entity[self.ordering_key]):
                return False
        if self.operator == 'and':
            return self.having[0][1] in str(entity[self.having[0][0]]) and \
                   self.having[1][1] in str(entity[self.having[1][0]])
        else:
            return self.having[0][1] in str(entity[self.having[0][0]]) or \
                   self.having[1][1] in str(entity[self.having[1][0]])


class FundingNoNanTask(object):
    # Identify each instance (query) using an index
    def __init__(self, index):
        self.counter = index

        # Define the path of the files to be used to save the results of the query
        self.query_output = "results/" + str(self.counter) + "_query.txt"
        self.lazy_output = "results/" + str(self.counter) + "_lazy.csv"
        self.eager_output = "results/" + str(self.counter) + "_eager.csv"
        self.query_details = "results/queries.csv"

        # Define if batch version is required
        self.batch = True

        # SELECT

        # Top-K query: define the number of entities (K) to be returned (if all the entities must be returned, set <= 0)
        self.top_k = 0

        # Indicate if the entities with null value of the ordering key must be returned or ignored
        self.ignore_null = True

        # Define the aggregation function to be used for each attribute (min, max, avg, sum, vote, random, concat)
        # For the ordering key only some aggregation functions (min, max, avg, vote) are supported
        random_aggregation = random.choice(['max', 'min'])
        self.aggregations = {'id': 'min', 'legal_name': 'vote', 'address': 'vote', 'source': 'vote',
                             'council_member': 'vote', 'amount': random_aggregation}

        # Define the attributes to be shown for the resulting entities
        self.attributes = ['legal_name', 'address', 'source', 'council_member', 'amount']

        # FROM

        # Choose the dataset to be used (among: alaska_camera, funding)
        self.ds_name = 'funding_no_nan'

        # Define the path of the dataset: it must be a CSV file
        self.ds_path = "data/" + self.ds_name + "_dataset.csv"

        # Define the path of the ground truth (a CSV file containing the matching pairs - couples ordered by id)
        self.gold_path = "data/" + self.ds_name + "_gold.csv"

        # With blocking or without blocking?
        self.blocking = False
        self.blocks_path = "data/" + self.ds_name + "_blocks.txt"
        self.block_costs = "data/" + self.ds_name + "_block_costs.txt"
        self.record_blocks = "data/" + self.ds_name + "_record_blocks.txt"

        # WHERE

        # HAVING

        # Define HAVING conditions as attribute-value pairs (for LIKE situation)
        sources = ['aging', 'aids', 'boro', 'casa', 'food', 'health', 'local', 'youth']
        legal_name = ['asian', 'association', 'christian', 'community', 'council', 'foundation', 'jewish', 'service']
        random_source = random.choice(sources)
        random_name = random.choice(legal_name)
        self.having = [('source', random_source), ('legal_name', random_name)]
        # self.having = [('brand', all_brands[self.counter - 1]), ('brand', all_brands[self.counter - 1])]

        # Define the logical operator to be used for HAVING conditions (and/or)
        self.operator = 'and'

        # ORDER BY

        # Define the numeric attribute to be used as ordering key (OK) and the ordering mode (asc or desc)
        self.ordering_key = 'amount'
        if random_aggregation == 'max':
            self.ordering_mode = 'desc'
        else:
            self.ordering_mode = 'asc'

        # Get the query in SQL
        select_clause = "SELECT "
        if self.top_k > 0:
            select_clause = select_clause + "TOP(" + str(self.top_k) + ") "
        for i in range(0, len(self.attributes)):
            select_clause = select_clause + self.aggregations[self.attributes[i]] + "(" + self.attributes[i] + ")"
            if i < len(self.attributes) - 1:
                select_clause = select_clause + ", "
            else:
                select_clause = select_clause + "\n"
        from_clause = "FROM " + self.ds_name + "\n"
        group_by_clause = "GROUP BY _\n"
        having_clause = "HAVING "
        for i in range(0, len(self.having)):
            having_clause = having_clause + self.aggregations[self.having[i][0]] + \
                            "(" + self.having[i][0] + ") LIKE '%" + self.having[i][1] + "%'"
            if i < len(self.having) - 1:
                having_clause = having_clause + " " + self.operator + " "
            else:
                having_clause = having_clause + "\n"
        order_by_clause = "ORDER BY " + self.aggregations[self.ordering_key] + "(" + self.ordering_key + \
                          ") " + self.ordering_mode + "\n"
        self.query = (select_clause + from_clause + group_by_clause + having_clause + order_by_clause).upper()

    # Define the query for batch ER algorithm
    # It is a post filtering (application of HAVING clauses on solved entity), so AND/OR are maintained
    def batch_query(self, entities):
        if self.ordering_mode == 'asc':
            ascending = True
        else:
            ascending = False
        if self.operator == 'and':
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)
        else:
            return entities.loc[
                (entities[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                (entities[self.having[1][0]].str.contains(self.having[1][1], na=False)),
                self.attributes].sort_values(by=[self.ordering_key], ascending=ascending)

    # Define the preliminary filtering for BrewER
    def brewer_pre_filtering(self, records, solved):
        # To reduce the number of comparisons, if HAVING conditions are in AND...
        # ...we check that all conditions are separately satisfied by at least one record appearing the block
        if self.operator == 'and':
            # If we consider already solved records (no neighbours), simply filter them in AND
            if solved:
                return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) &
                                   (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
            # Otherwise, check that all conditions are separately satisfied (if not, return an empty DataFrame)
            else:
                condition = records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False))]
                if len(condition) == 0:
                    return condition
                condition = records.loc[(records[self.having[1][0]].str.contains(self.having[1][1], na=False))]
                if len(condition) == 0:
                    return condition
            # If the conditions are all satisfied, proceed as in OR case

        # Otherwise, in OR case, check that at least one of the conditions is satisfied by the records of the block
        return records.loc[(records[self.having[0][0]].str.contains(self.having[0][1], na=False)) |
                           (records[self.having[1][0]].str.contains(self.having[1][1], na=False))]

    # Define the post filtering for BrewER (AND/OR are maintained)
    def brewer_post_filtering(self, entity):
        if self.ignore_null:
            if pd.isna(entity[self.ordering_key]):
                return False
        if self.operator == 'and':
            return self.having[0][1] in str(entity[self.having[0][0]]) and \
                   self.having[1][1] in str(entity[self.having[1][0]])
        else:
            return self.having[0][1] in str(entity[self.having[0][0]]) or \
                   self.having[1][1] in str(entity[self.having[1][0]])
