import pandas as pd
from neo4j import GraphDatabase
from pandas import read_sql_query
from tqdm import tqdm
import requests
from utils import BASE_AUTH
import logging
import warnings
from artgraph_utils import update_graph

warnings.filterwarnings('ignore')

tqdm.pandas()
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')


def get_paintings_by_artist(artist_name):
    """
    :param artist_name: the name of an artist
    :return: a dict containing all the metadata regarding the artist
    """
    base_query = 'https://www.wikiart.org/en/App/Painting/PaintingsByArtist?artistUrl={artist}&json=2'
    return requests.get(base_query.format(artist=artist_name)).json()


def get_content_id(artist_name, artwork):
    """
    :param artist_name:
    :param artworks:
    :return:
    """
    title, name = artwork
    paintings = get_paintings_by_artist(artist_name)
    filtered = list(filter(lambda x: x['image'].split('/')[-1][:-10] == name or x['title'] == title, paintings))
    # special case: more than 1 retrieved-> filter again
    # priority to the image because
    assert(len(filtered) > 0), (artist_name, artwork)
    if len(filtered) > 1:
        filtered_out = list(filter(lambda x: x['image'].split('/')[-1][:-10] == name, filtered))
        if len(filtered_out) == 0:
            filtered = filtered[:1]
        else:
            filtered = filtered_out
    return filtered[0]['contentId']


def artist_in_artgraph(artist_name, driver, db):
    """
    :param artist_name: a generic artist name
    :param driver: connection with db
    :param db: name of db
    :return: boolean value. True if artist name is present in db, otherwise false
    """
    with driver.session(database=db) as session:
        ans = session.run(f'match (a:Artist{{name: "{artist_name}" }}) return count(distinct a) as num')
        return next(iter(ans))['num'] > 0


def get_artist_information(artist_name):
    """
    :param artist_name: a generic artist name
    :return: all the info retrieved for the given artist. Specifically, if the artist is present into artgraph,
            nodes and relationships regarding it are returned, otherwise a query on the base api is made, and the result
            is in dict format.
    """
    # if in artgraph -> migrate, else get it from the web
    driver = GraphDatabase.driver(**BASE_AUTH)
    if artist_in_artgraph(artist_name, driver, 'neo4j'):
        with driver.session(database='neo4j') as session:
            artists_links = list(iter(session.run(f"""
            match p=(:Artist{{name: "{artist_name}" }})-->(n) where labels(n)[0] <> "Artwork"
            return relationships(p) as rels, nodes(p) as nodes
            """)))
            artists_links = list(map(lambda x: x['rels'][0], artists_links))
        if not artists_links:
            base_query = 'https://www.wikiart.org/en/{artist_name}?json=2'
            return requests.get(base_query.format(artist_name=artist_name)).json(), 'web'
        return artists_links, 'artgraph'
    else:
        base_query = 'https://www.wikiart.org/en/{artist_name}?json=2'
        return requests.get(base_query.format(artist_name=artist_name)).json(), 'web'


def get_artwork_information(content_id):
    """
    :param content_id: id of specific artwork
    :return: all the info in dict format.
    """
    return {k: v for k, v in requests.get(f'https://www.wikiart.org/en/App/Painting/ImageJson/{content_id}').json()
            .items()if k != 'dictionaries'}


def stringfy_prop(props):
    """
    :param props: dict of properties
    :return: a string ready to be inserted for a query in neo4j
    """
    return ', '.join([f'{x}: "{props[x]}"' if isinstance(props[x], str) else f"{x}: {props[x]}" for x in props.keys()
                      if props[x] is not None])


def save_artist(raw, driver, db):
    """
    :param raw: general infotmation regarding the given artist
    :param driver: connection to db
    :param db: name of database
    :return: None. It has the effect of inserting specific nodes and edges in the graph.
    """
    artist_info, mode = get_artist_information(raw['artist_name'])
    # add artist
    if mode == 'artgraph':
        if not artist_in_artgraph(raw['artist_name'], driver, db):
            update_graph(driver=driver, db=db, rels=artist_info, bar=False)
        return raw['artist_name']
    else:
        attributes = {'birth_date': artist_info['birthDayAsString'] if artist_info['birthDayAsString'] else None,
                      'death_date': artist_info['deathDayAsString'] if artist_info['deathDayAsString'] else None,
                      'wikipedia_url': artist_info['wikipediaUrl'] if artist_info['wikipediaUrl'] else None,
                      'gender': artist_info['gender'] if artist_info['gender'] else None,
                      'image_url': artist_info['image'] if artist_info['image'] else None,
                      'name': artist_info['url'] if artist_info['url'] else None,
                      'printed_name': artist_info['artistName'] if artist_info['artistName'] else None,
                      }

        if not artist_in_artgraph(artist_info['url'], driver, db):
            query = f'''merge(:Artist{{ {", ".join([f'{k}: "{v}"' for k, v in attributes.items() if v is not None])} }})'''
            with driver.session(database=db) as session:
                session.run(query)
        return artist_info['url']


def save_artwork(raw, driver, db):
    """
    :param raw: general information about the artwork and the artist
    :param driver: connection to database
    :param db: database name
    :return: None. It has the effect of saving all metadata regarding the artwork in the graph.
    """
    try:
        metadata = get_artwork_information(raw['content_id'])
        artist = save_artist(raw, driver, db)
    except requests.JSONDecodeError:
        return
    # add other info
    with driver.session(database=db) as session:
        # merge just the artwork
        session.run(f'''merge(:Artwork{{ code: "{raw.ID}",
                                         title: "{metadata['title'].replace('"', '')}",
                                         year: "{metadata['yearAsString']}",
                                         dimensions: "{metadata['height']} X {metadata['width']}",
                                         image_url: "{metadata['image']}",
                                         name: "{metadata['artistUrl']}_{metadata['url']}.jpg"}})''')

        # getting properties
        style = [x.lower() for x in metadata['style'].split(', ')][0] if metadata['style'] else None
        genre = [x.lower() for x in metadata['genre'].split(', ')][0] if metadata['genre'] else None
        medias = metadata['material'].split(', ') if metadata['material'] else None
        serie = [x.lower() for x in metadata['serie'].split(', ')][0] if metadata['serie'] else None
        gallery = [x.lower() for x in metadata['galleryName'].split(', ')][0] if metadata['galleryName'] else None
        period = [x.lower() for x in metadata['period'].split(', ')][0] if metadata['period'] else None
        tags = [x.lower().replace('"', "'") for x in metadata['tags'].split(', ')] if metadata['tags'] else None

        session.run(f'''match (a:Artwork{{code: "{raw.ID}"}})
                        match (au:Artist{{name: "{artist}"}})
                        merge (a)-[:createdBy]->(au)''')

        # merge style
        if style:
            session.run(f'''match (a:Artwork{{code: "{raw.ID}"}})
                            merge(s:Style{{name: "{style}"}})
                            merge (a)-[:hasStyle]->(s)''')

        # merge genre
        if genre:
            session.run(f'''match (a:Artwork{{code: "{raw.ID}"}})
                            merge(g:Genre{{name: "{genre}"}})
                            merge (a)-[:hasGenre]->(g)''')

        # merge media if there are
        if medias:
            query = f'''match (a:Artwork{{code: "{raw.ID}"}})'''
            query += '\n'.join([f'merge (m{i}: Media{{name: "{media}"}})\n merge (a)-[:madeOf]->(m{i})' \
                                for i, media in enumerate(medias)])
            session.run(query)

        # merge serie
        if serie:
            session.run(f'''match (a:Artwork{{code: "{raw.ID}"}})
                            merge (s:Serie{{ name: "{metadata['serie']}" }})
                            merge (a)-[:partOf]->(s)''')

        # merge gallery
        if gallery:
            session.run(f'''match (a:Artwork{{code: "{raw.ID}"}})
                                        merge (g:Gallery{{ name: "{metadata['galleryName']}" }})
                                        merge (a)-[:locatedIn]->(g)''')

        # merge period
        if period:
            session.run(f'''match (a:Artwork{{code: "{raw.ID}"}})
                            merge (p:Period{{ name: "{metadata['period']}" }})
                            merge (a)-[:hasPeriod]->(p)''')

        # merge tags
        if tags:
            query = f'match (a:Artwork{{code: "{raw.ID}"}})'
            query += '\n'.join([f'merge (t{i}: Tag{{name: "{tag}"}})\n merge (a)-[:about]->(t{i})' \
                                for i, tag in enumerate(tags)])
            session.run(query)


def update_graph_artist(artwork_info, driver):
    artwork_info_artist = artwork_info[artwork_info.api_v1_artist == 1]
    artwork_info_artist.drop(['name_in_artgraph', 'api_v1_artist',
                              'api_v1_artist_1', 'api_v1_url', 'api_v2'],
                             axis=1,
                             inplace=True)
    artwork_info_artist['artist_name'] = artwork_info_artist['Image URL'].apply(lambda x: x.split('/')[-2])
    artwork_info_artist['artist_name_1'] = artwork_info_artist['Artist'] \
        .apply(lambda x: '-'.join(x.lower().split(' ')))
    # set content ids
    logging.info('Getting proper content ids...')
    artwork_info_artist['content_id'] = artwork_info_artist.progress_apply(lambda x: get_content_id(x.artist_name, x[
        ['Title', 'name']]), axis=1)

    logging.info("Saving new artworks and relations into db...")
    artwork_info_artist.progress_apply(lambda x: save_artwork(x, driver, 'recsys'), axis=1)


def update_graph_artist_1(artwork_info, driver):
    artwork_info_artist_1 = artwork_info[artwork_info.api_v1_artist_1 == 1]
    artwork_info_artist_1['artist_name'] = artwork_info_artist_1['artist_1']
    artwork_info_artist_1.drop(['name_in_artgraph', 'api_v1_artist',
                                'api_v1_artist_1', 'api_v1_url', 'api_v2'],
                               axis=1,
                               inplace=True)
    logging.info("Getting proper content ids (1) ...")
    artwork_info_artist_1['content_id'] = artwork_info_artist_1.progress_apply(lambda x: get_content_id(x.artist_name, x[
        ['Title', 'name']]), axis=1)
    logging.info("Saving new artworks and relations (1) into db...")
    artwork_info_artist_1.progress_apply(lambda x: save_artwork(x, driver, 'recsys'), axis=1)


def get_url_content_id(artist_name, artwork):
    paintings = get_paintings_by_artist(artist_name)
    paintings = map(lambda x: x['contentId'], paintings)
    paintings = map(lambda x: (x, f'{get_artwork_information(x)["url"]}.jpg'), paintings)
    return list(filter(lambda x: x[1] == artwork, paintings))[0][0]


def update_graph_url(artwork_info, driver):
    artwork_info_url = artwork_info[artwork_info.api_v1_url == 1]
    artwork_info_url['artist_name'] = artwork_info_url['artist']
    artwork_info_url.drop(['name_in_artgraph', 'api_v1_artist',
                           'api_v1_artist_1', 'api_v1_url', 'api_v2'],
                          axis=1,
                          inplace=True)
    logging.info('getting content ids (url) ...')
    artwork_info_url['content_id'] = artwork_info_url.progress_apply(lambda x: get_url_content_id(x.artist_name, x['name']),
                                                                     axis=1)
    logging.info('Saving new artworks and relations (url) into db...')
    artwork_info_url.progress_apply(lambda x: save_artwork(x, driver, 'recsys'), axis=1)


def main():
    artwork_info = pd.read_csv('artwork_info_sources.csv', index_col=0)
    driver = GraphDatabase.driver(**BASE_AUTH)

    # delete useless entries
    artwork_info.drop(artwork_info[(artwork_info.api_v1_artist == 0) &
                                   (artwork_info.name_in_artgraph == 0) &
                                   (artwork_info.api_v1_artist_1 == 0) &
                                   (artwork_info.api_v1_url == 0) &
                                   (artwork_info.api_v2 == 0)].index, inplace=True)

    # delete useless columns
    artwork_info.drop(['Category', 'Year'], axis=1, inplace=True)

    update_graph_artist(artwork_info, driver)
    update_graph_artist_1(artwork_info, driver)
    update_graph_url(artwork_info, driver)


if __name__ == '__main__':
    main()