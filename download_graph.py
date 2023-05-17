import pandas as pd
import numpy as np
from neo4j import GraphDatabase
from raw_generation_nosplit import ArtGraphNoSplit
from raw_generation import ArtGraphWithSplit
import os
import argparse


def get_stat_queries():
    return {
        "node_labels_stats": "MATCH (n) RETURN distinct labels(n) as node_label, count(*) as count",
        "rel_labels_stats": "MATCH (n)-[r]->(n2) RETURN distinct type(r) as rel_label, count(*) as count",
        "triplet-type-list": "MATCH (x)-[r]->(y) RETURN distinct HEAD(labels(x)) as head, type(r), head(labels(y)) as tail"
    }


def get_split_paths():
    split_paths = {
        'train': os.path.join("split", "train.csv"),
        'valid': os.path.join("split", "valid.csv"),
        'test': os.path.join("split", "test.csv")
    }


class DBManager():
    def __init__(self, uri, user, pwd):
        self.driver = GraphDatabase.driver(uri=uri, auth=(user, pwd))
        self.uri = uri
        self.user = user
        self.pwd = pwd

    def get_mapping_queries(self, db):
        with self.driver.session(database=db) as session:
            node_types = session.run("MATCH(n) RETURN  DISTINCT labels(n)[0] as typen")  # getting all node types
            node_types = [record['typen'] for record in node_types]  # extracting data into a list
        mapping_queries = {node.lower(): f"MATCH (n:{node}) RETURN n.name as name" for node in
                           node_types}  # generating queries for node types
        mapping_queries['rel'] = """MATCH (n)-[r]-(n2)
            RETURN DISTINCT toLower(type(r)) as rel_label"""  # generating queries for edge types
        return mapping_queries

    def get_relation_queries(self, db):
        with self.driver.session(database=db) as session:
            triplets = session.run("""MATCH p=(a)-[r]->(b)
                    RETURN DISTINCT labels(a)[0] as source,
                        type(r) as relation,
                        labels(b)[0] as destination""")
            triplets = [(t['source'], t['relation'], t['destination']) for t in triplets]
        relation_queries = {str(tuple(map(lambda x: x.lower(),t))):
                                f"""MATCH (a:{t[0]})-[r:{t[1]}]->(b:{t[2]})
                                RETURN a.name as source_name, b.name as dest_name"""
                            for t in triplets}
        relation_queries["('artwork', 'elicits', 'emotion')"] = """
        match(a:Artwork)-[r]-(e:Emotion)
        with a, sum(r.arousal) as sum_arousal, e
        with a, max(sum_arousal) as max_arousal
        match(a)-[r2]-(e2:Emotion)
        with a, sum(r2.arousal) as sum2, e2, max_arousal
        where sum2 = max_arousal
        return a.name as source_name, collect(e2.name)[0] as dest_name
        """
        relation_queries["('user', 'rates', 'artwork')"] = """MATCH (a:User)-[r:rates]->(b:Artwork)
            RETURN a.name as source_name,
                b.name as dest_name,
                r.score as weight"""
        return relation_queries

    def get_artworks(self, db):
        with self.driver.session(database=db) as session:
            query = 'match (a:Artwork) return a.name as name'
            ans = list(map(lambda x: x['name'], session.run(query)))
        return ans


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--uri', required=False, default="bolt://localhost:7687", help='uri of the neo4j dbms', type=str)
    parser.add_argument('--user', required=False, default='neo4j', help='username of the target db', type=str)
    parser.add_argument('--pwd', required=False, default='neo4j', help='password of the target db', type=str)
    parser.add_argument('--db', required=False, default='recsys', help='name of the target db', type=str)
    parser.add_argument('--root', required=False, default='artgraph2recsys', help='target root folder', type=str)

    return parser.parse_args()


def main():
    args = parse_args()
    db_manager = DBManager(args.uri, args.user, args.pwd)
    queries = {
        'mapping': db_manager.get_mapping_queries(args.db),
        'relations': db_manager.get_relation_queries(args.db),
        'stats': get_stat_queries()
    }
    conf = {
        "uri": args.uri,
        "username": args.user,
        "password": args.pwd,
        "database": args.db,
    }
    artgraph = ArtGraphNoSplit(root=args.root,
                               conf=conf,
                               queries=queries,
                               artwork_subset=db_manager.get_artworks(args.db))
    artgraph.build()
    artgraph.write()


if __name__ == '__main__':
    main()