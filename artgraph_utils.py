import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm
import logging
from utils import BASE_AUTH
import warnings

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')

warnings.filterwarnings('ignore')

tqdm.pandas()

def get_artworks(driver, db, data):
    """
    :param driver: A GraphDBMS driver instance
    :param db: The name of the database from which the info is taken
    :param data: A list object containing the names of the artworks to filter
    :return Returns the list of nodes and relationships that refer to a specific set of artworks
    """
    with driver.session(database=db) as session:
        ans = list(iter(session.run(f"""match p = (a:Artwork)-[]->(n) where a.name in {data}
                                       return relationships(p) as rels, nodes(p) as nodes""")))
        rels = list(map(lambda x: x['rels'][0], ans))
    return rels


def get_artists(driver, db, data):
    """
    :param driver: A GraphDBMS driver instance
    :param db: The name of the database from which the info is taken
    :param data: A list object containing the names of the artworks to filter
    :return: Returns the list of nodes and relationships that refer to a specific set of artists
    """
    with driver.session(database=db) as session:
        artists_links = list(iter(session.run(f"""
        match (aw:Artwork)-->(a:Artist) where aw.name in {data}
        match p = (a)-->() return relationships(p) as rels, nodes(p) as nodes
        """)))
        artists_links = list(map(lambda x: x['rels'][0], artists_links))
    return artists_links


def get_galeries(driver, db, data):
    """
    :param driver: A GraphDBMS driver instance
    :param db: The name of the database from which the info is taken
    :param data: A list object containing the names of the artworks to filter
    :return Returns the list of nodes and relationships that refer to a specific set of galleries
    """
    with driver.session(database=db) as session:
        gallery_links = list(iter(session.run(f"""
        match (a:Artwork)-->(g:Gallery) where a.name in {data}
        match p = (g)-->() return relationships(p) as rels, nodes(p) as nodes
        """)))
        gallery_links = list(map(lambda x: x['rels'][0], gallery_links))
    return gallery_links


def get_cities(driver, db, data):
    """
    :param driver: A GraphDBMS driver instance
    :param db: The name of the database from which the info is taken
    :param data: A list object containing the names of the artworks to filter
    :return Returns the list of nodes and relationships that refer to a specific set of cities
    """
    with driver.session(database=db) as session:
        city_links = list(iter(session.run(f"""
        match (a:Artwork)-->(c:City) where a.name in {data}
        match p = (c)-->() return relationships(p) as rels, nodes(p) as nodes
        """)))
        city_links = list(map(lambda x: x['rels'][0], city_links))
    return city_links


def get_properties(n):
    """
    :param n: an instance from which it is necessary to extract the properties
    :return: A string containing all the properties that have to be set in the db
    """
    return ', '.join([f'{x}: "{n[x]}"' if isinstance(n[x], str) else f"{x}: {n[x]}" for x in n.keys()])


def update_rel_to_db(rel, driver, db):
    """
    :param rel:
    :param driver:
    :param db:
    :return:
    """
    a, b = rel.nodes
    query = f"""
    merge (a: {list(a.labels)[0]} {{ {get_properties(a._properties)} }})
    merge (b: {list(b.labels)[0]} {{ {get_properties(b._properties)} }})
    merge (a)-[:{rel.type} {{ {get_properties(rel._properties)} }}]->(b)
    """
    with driver.session(database=db) as session:
        session.run(query)
    return query


def update_graph(driver, db, rels, bar=True):
    """
    :param driver:
    :param db:
    :param rels:
    :return:
    """
    if bar:
        for rel in tqdm(rels):
            update_rel_to_db(rel, driver, db)
    else:
        for rel in rels:
            update_rel_to_db(rel, driver, db)


def add_code(artwork, code, session):
    """
    :param artwork:
    :param code:
    :param session:
    :return:
    """
    session.run(f"match (a:Artwork{{name: '{artwork}' }}) set a.code = '{code}' ")
    return


def main():
    artwork_info = pd.read_csv('artwork_info_sources.csv', index_col=0)
    driver = GraphDatabase.driver(**BASE_AUTH)
    # delete useless entries, that cannot be found in any way
    artwork_info.drop(artwork_info[(artwork_info.api_v1_artist == 0) &
                                   (artwork_info.name_in_artgraph == 0) &
                                   (artwork_info.api_v1_artist_1 == 0) &
                                   (artwork_info.api_v1_url == 0) &
                                   (artwork_info.api_v2 == 0)].index, inplace=True)

    # delete useless columns
    artwork_info.drop(['Category', 'Title', 'Year', 'name'], axis=1, inplace=True)
    artwork_info_artgraph = artwork_info[artwork_info.name_in_artgraph == 1]
    artwork_info_artgraph.drop(['name_in_artgraph', 'api_v1_artist',
                                'api_v1_artist_1', 'api_v1_url', 'api_v2'],
                               axis=1,
                               inplace=True)

    # get all the artworks
    artworks = artwork_info_artgraph['Image URL'].apply(lambda x: '_'.join(x.split('/')[-2:])).tolist()

    # add artworks to new db
    logging.info('adding artworks...')
    rels = get_artworks(driver, 'neo4j', artworks)
    update_graph(driver, 'recsys', rels)

    # add artists to new db
    logging.info('adding artists...')
    artist_links = get_artists(driver, 'neo4j', artworks)
    update_graph(driver, 'recsys', artist_links)

    # update gallery to new db
    logging.info('adding galleries...')
    gallery_links = get_galeries(driver, 'neo4j', artworks)
    update_graph(driver, 'recsys', gallery_links)

    # update cities to new db
    logging.info('adding cities...')
    city_links = get_cities(driver, 'neo4j', artworks)
    update_graph(driver, 'recsys', city_links)


    # add code to artwork
    logging.info('adding codes...')
    artwork_info_artgraph['artgraph_name'] = artwork_info_artgraph['Image URL'].apply(lambda x: '_'.join(x.split('/')[-2:]))
    with driver.session(database='recsys') as session:
        artwork_info_artgraph.progress_apply(lambda x: add_code(x['artgraph_name'], x['ID'], session), axis=1)

    logging.info('Done!')


if __name__ == '__main__':
    main()