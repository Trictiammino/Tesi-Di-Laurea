import os
import logging
import copy
import pandas
from pandas import DataFrame
from sklearn.model_selection import train_test_split
from ast import literal_eval
from ArtGraphOGM import ArtGraphDBConnector, Relation, Relations, NodeInstances, SplitInstances, Nodes

logging.basicConfig(level=logging.INFO)


class ArtGraphWithSplit():
    """
    Generate raw files from the neo4j database.
    It is used the convention of OGB (https://ogb.stanford.edu/)
    The 'mapping' folder will contains csv files which map id to instance name.
    The 'raw' folder will contains the entire graph in raw format.
    See the README for understanding better the output.

    No assumption are made about the artgraph schema (this is schema agnostic). Therefore, you
    need to provide your queries for mapping and relations. Optionally you can add other queries
    for some statistics about the database content.

    For node mapping you have to name the column returned as "name". (e.g. "MATCH (f:Field) RETURN f.name as name").
    For relation mapping you have to name the column returned as "rel_label" (e.g. "MATCH (n)-[r]->(n2) RETURN distinct toLower(type(r)) as rel_label").
    For relation you have to name the source node column as "source_name" and the destination node column as "dest_name".

    The creation of the raw data is distinct from the saving on the disk.
    First, the raw data are created and saved in memory. (get_* methods).
    Then, all the data are saved on the disk (write_* methods).

    Args:
        root (string): the root folder which will contain the whole raw dataset
        conf (dict): a dictionary with uri, username, password and database to access to the neo4j database
        queries (dict): a dictionary of dictionaries which contain queries for mapping, relations and stats.
        labels (list): a list of triple ('artwork', relation name, node label name)
        split_paths (dict): contains selected artwork instances (csv file for each split) for train ('train' key), validation ('valid' key) and test ('test' key)
    """

    def __init__(self, root='artgraph', conf={}, queries={}, labels=[], split_paths=None):

        self.create_folders(root)

        self.uri = conf['uri']
        self.username = conf['username']
        self.password = conf['password']
        self.database = conf['database']

        self.mapping_queries = queries['mapping']
        self.relation_queries = queries['relations']
        self.stat_queries = queries['stats']

        self.labels = labels
        self.split_paths = split_paths

        self.nodes = Nodes()
        self.relations = Relations()
        self.relations_train = Relations()
        self.artwork2labels = {}
        self.split = {}

    def create_folders(self, root_folder):
        """Create the needed folder on the disk, if they do not exist, following the OGB convention.

            Folder structure is:
            root
            ----mapping
            ----raw
                ---relations
                ---node-label
                   ---artwork
                ---split
                   ---artwork
        """
        self.root_folder = root_folder
        self.mapping_folder = os.path.join(self.root_folder, "mapping")
        self.raw_folder = os.path.join(self.root_folder, "raw")
        self.rel_folder = os.path.join(self.raw_folder, "relations")
        self.split_folder = os.path.join(self.raw_folder, "split", "artwork")
        self.label_folder = os.path.join(self.raw_folder, "node-label", "artwork")

        if not os.path.exists(self.root_folder):
            os.mkdir(self.root_folder)

        if not os.path.exists(self.mapping_folder):
            os.mkdir(self.mapping_folder)

        if not os.path.exists(self.raw_folder):
            os.mkdir(self.raw_folder)

        if not os.path.exists(self.rel_folder):
            os.mkdir(self.rel_folder)

        if not os.path.exists(self.split_folder):
            os, os.mkdir(os.path.join(self.raw_folder, "split"))
            os.mkdir(self.split_folder)

        if not os.path.exists(self.label_folder):
            os.mkdir(os.path.join(self.raw_folder, "node-label"))
            os.mkdir(self.label_folder)

    def get_mapping(self):
        """Create the mapping dataframes.

            The id corresponds to the dataframe index.Something like this (e.g. for genre)
                     name
            0        impressionism
            1        realism
            2        surrealism
            ...
        """
        print("Getting mapping...")
        agdb = ArtGraphDBConnector(self.uri, self.username, self.password, self.database)
        for name, query in self.mapping_queries.items():
            node = NodeInstances(name)
            node.add_instances(agdb.df_from_query(query))
            self.nodes.add_node(name, node)
        agdb.close()

    def get_relations(self):
        """Create the relation dataframes and the number of relation for each relation type.

            The first column correspond to the source node id and the second column to the destination node id.
            Something like this
                     source     destination
            0        0          1
            1        0          4
            2        0          23
        """
        print("Getting relations...")
        agdb = ArtGraphDBConnector(self.uri, self.username, self.password, self.database)
        for triple, query in self.relation_queries.items():
            source_name, relation_name, destination_name = literal_eval(triple)
            # get all the mappings
            df_triplet = agdb.df_from_query(query)  # name_source, name_dest
            df_mapping_source = self.nodes.get_node(source_name).get_instances()  # id, name
            df_mapping_destination = self.nodes.get_node(destination_name).get_instances()  # id, name
            df_mapping_relations = self.nodes.get_node('rel').get_instances()  # id, name

            # get the id of node source and node destination
            mapping_source = self.name2id_mapping(df_mapping_source, 'name', lambda x: x)
            mapping_destination = self.name2id_mapping(df_mapping_destination, 'name', lambda x: x)
            mapping_relation = self.name2id_mapping(df_mapping_relations, 'rel_label', lambda x: x.lower())

            df_relation_mapping = DataFrame()
            df_relation_mapping['source'] = df_triplet['source_name'].map(mapping_source)
            df_relation_mapping['destination'] = df_triplet['dest_name'].map(mapping_destination)

            id_relation = mapping_relation[relation_name]
            relation = Relation(id=id_relation, name=relation_name, source=source_name, destination=destination_name)
            relation.add_edges(df_relation_mapping)

            if 'weight' in df_triplet.columns:
                relation.add_attributes(df_triplet.weight)

            self.relations.add_relation((source_name, relation_name, destination_name), relation)
        agdb.close()

    def get_labels(self):
        """Get the labels of each artwork

            A column w9th of label id. The artwork id is identified by the row (row 1 corresponds to id 0)
        """
        print("Getting labels")
        agdb = ArtGraphDBConnector(self.uri, self.username, self.password, self.database)
        for label in self.labels:
            source, relation, destination = label
            df_relation = self.relations.get_relation(label).get_edges()
            df_relation = df_relation.sort_values(by=['source'])
            df_triplet = df_relation.drop_duplicates(subset='source',
                                                     keep='last')  # if an artworks has more than one label

            df_label = DataFrame()
            df_label['label'] = df_triplet['destination']

            self.artwork2labels[destination] = df_label
        agdb.close()

    def get_split(self):
        """Do a random split.

            Retained 90% for training purpose, 5% for testing and 5% for validation.
        """
        print("Getting split")
        df_artwork = copy.deepcopy(self.nodes.get_node('artwork').get_instances())
        df_artwork['idx'] = df_artwork.index  # add dataframe index as column
        df_train, df_test = train_test_split(df_artwork['idx'], test_size=0.1, random_state=42)
        df_test, df_valid = train_test_split(df_test, test_size=0.5, random_state=42)

        train_nodes = SplitInstances('train')
        print(train_nodes.__dict__)
        train_nodes.add_instances(df_train.drop(['name'], axis=1))
        self.split['train'] = train_nodes

        test_nodes = SplitInstances('test')
        test_nodes.add_instances(df_test.drop(['name'], axis=1))
        self.split['test'] = test_nodes

        valid_nodes = SplitInstances('valid')
        valid_nodes.add_instances(df_valid.drop(['name'], axis=1))
        self.split['valid'] = valid_nodes

    def get_split_custom(self, train_path, valid_path, test_path):
        """Use a custom split in which are specified artworks image name .

            The path for each split is the path of a csv file containing all the artworks we want in that split.
            Using those files we are able to get our artwork id using the artwork name.
        """
        print("Getting split...")
        df_artwork = copy.deepcopy(self.nodes.get_node('artwork').get_instances())

        df_train = pandas.read_csv(train_path, names=['idx', 'name'], header=0)
        df_valid = pandas.read_csv(valid_path, names=['idx', 'name'], header=0)
        df_test = pandas.read_csv(test_path, names=['idx', 'name'], header=0)

        artwork_mapping = self.name2id_mapping(df_artwork, 'name')

        training_list = []
        for _, i in enumerate(df_train.name):
            training_list.append(artwork_mapping[i])

        test_list = []
        for _, i in enumerate(df_test.name):
            test_list.append(artwork_mapping[i])

        valid_list = []
        for _, i in enumerate(df_valid.name):
            valid_list.append(artwork_mapping[i])

        train_nodes = SplitInstances('train')
        train_nodes.add_instances(pandas.DataFrame(training_list))
        self.split['train'] = train_nodes

        test_nodes = SplitInstances('test')
        test_nodes.add_instances(pandas.DataFrame(test_list))
        self.split['test'] = test_nodes

        valid_nodes = SplitInstances('valid')
        valid_nodes.add_instances(pandas.DataFrame(valid_list))
        self.split['valid'] = valid_nodes

    def name2id_mapping(self, df, column, fn=lambda x: x):
        """Create a mapping from node name to node id.

            e.g. >>> mapping['impressionism']
                     18
        """
        mapping = {fn(index): i for i, index in enumerate(df[column])}
        return mapping

    def write_mapping(self):
        print("Writing mapping...")

        self.nodes.save_on_disk(self.mapping_folder)

        instances = self.nodes.get_node('rel').get_instances()
        instances.to_csv(os.path.join(self.mapping_folder, "relidx2relname.csv"), index=True, header=False)

        for name in self.artwork2labels.keys():
            instances = self.nodes.get_node(name).get_instances()
            instances.to_csv(os.path.join(self.mapping_folder, "labelidx2" + name + "name.csv"), index=True,
                             header=False)

    def write_relations(self):
        self.relations.save_on_disk(self.rel_folder)

    def write_labels(self):
        print("Writing labels...")
        for label, df_label in self.artwork2labels.items():
            df_label.to_csv(os.path.join(self.label_folder, "node-label-" + label + ".csv"), index=False, header=False)

    def write_info(self):
        print("Writing info...")
        # triple type list
        with open(os.path.join(self.raw_folder, "triplet-type-list.csv"), 'w') as f:
            for name, _ in self.relations:
                f.write(",".join(name) + "\n")
        f.close()

        # number of instances for each node type
        node2num = {}
        node_has_label = {}
        node_has_feats = {}
        node_has_split = {}
        for name, node in self.nodes:
            if name != 'rel':
                node2num[name] = [node.instances_number]
                if name != 'artwork':
                    node_has_label[node.name] = [False]
                    node_has_feats[node.name] = [False]
                    node_has_split[node.name] = [False]
                else:
                    node_has_label[node.name] = [True]
                    node_has_feats[node.name] = [True]
                    node_has_split[node.name] = [True]

        pandas.DataFrame.from_dict(node2num).to_csv(os.path.join(self.raw_folder, "num-node-dict.csv"), header=True,
                                                    index=False)
        pandas.DataFrame.from_dict(node_has_label).to_csv(os.path.join(self.raw_folder, "nodetype-has-label.csv"),
                                                          header=True, index=False)
        pandas.DataFrame.from_dict(node_has_feats).to_csv(os.path.join(self.raw_folder, "nodetype-has-feat.csv"),
                                                          header=True, index=False)
        pandas.DataFrame.from_dict(node_has_feats).to_csv(os.path.join(self.split_folder, "nodetype-has-split.csv"),
                                                          header=True, index=False)

    def write_split(self):
        print("Writing split...")
        for _, instances in self.split.items():
            instances.save_on_disk(self.split_folder)

    def build(self):
        """Create mapping, relations, labels and split instances in memory."""
        self.get_mapping()
        if self.split_paths is not None:
            self.get_split_custom(self.split_paths['train'], self.split_paths['valid'], self.split_paths['test'])
        else:
            self.get_split()
        self.get_relations()
        self.get_labels()

    def write(self, full=True):
        """Write on disk mapping, relations, labels and split instances"""
        self.write_mapping()
        if full == False:
            valid_nodes = self.split['valid'].get_instances().iloc[:, 0].tolist()
            test_nodes = self.split['test'].get_instances().iloc[:, 0].tolist()
            self.relations.remove_relations(excluded_relation=self.labels, valid_nodes=valid_nodes,
                                            test_nodes=test_nodes)
        self.write_relations()
        self.write_labels()
        self.write_info()
        self.write_split()


