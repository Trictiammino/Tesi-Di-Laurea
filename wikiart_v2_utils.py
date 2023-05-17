import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm
import requests
from utils import BASE_AUTH
import logging
import warnings
from wikiart_v1_utils import save_artist

warnings.filterwarnings('ignore')

tqdm.pandas()
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')


def get_painting(id: str):
    base = 'https://www.wikiart.org/en/api/2/Painting?id={id}'
    return requests.get(base.format(id=id)).json()


def save_artwork(raw, driver, db, v2=False):
    metadata = get_painting(raw.ID)
    if v2:
        raw['artist_name'] = metadata['artistUrl']
    artist=save_artist(raw, driver, db)

    with driver.session(database=db) as session:
        # merge just the artwork
        session.run(f'''merge(:Artwork{{ code: "{raw.ID}",
                                         title: "{metadata['title'].replace('"', '')}",
                                         year: "{metadata['completitionYear']}",
                                         dimensions: "{metadata['height']} X {metadata['width']}",
                                         image_url: "{metadata['image']}",
                                         name: "{metadata['artistUrl']}_{metadata['url']}.jpg" }})''')

        # getting properties
        style = [x.lower() for x in metadata['styles']][0] if metadata['styles'] else None
        genre = [x.lower() for x in metadata['genres']][0] if metadata['genres'] else None
        medias = metadata['media'] if metadata['media'] else None
        serie = metadata['serie']['title'] if metadata['serie'] else None
        gallery = [x.lower() for x in metadata['galleries']][0] if metadata['galleries'] else None
        period = [x.lower() for x in metadata['period'].split(', ')][0] if metadata['period'] else None
        tags = [x.lower().replace('"', "'") for x in metadata['tags']] if metadata['tags'] else None


        session.run(f'''match (a:Artwork{{code: "{raw.ID}"}})
                        match (au:Artist{{name: "{artist}" }})
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
                            merge (s:Serie{{ name: "{serie}" }})
                            merge (a)-[:partOf]->(s)''')

        # merge gallery
        if gallery:
            session.run(f'''match (a:Artwork{{code: "{raw.ID}"}})
                                        merge (g:Gallery{{ name: "{gallery}" }})
                                        merge (a)-[:locatedIn]->(g)''')

        # merge period
        if period:
            session.run(f'''match (a:Artwork{{code: "{raw.ID}"}})
                            merge (p:Period{{ name: "{period}" }})
                            merge (a)-[:hasPeriod]->(p)''')

        # merge tags
        if tags:
            query = f'match (a:Artwork{{code: "{raw.ID}"}})'
            query += '\n'.join([f'merge (t{i}: Tag{{name: "{tag}"}})\n merge (a)-[:about]->(t{i})' \
                                for i, tag in enumerate(tags)])
            session.run(query)


def main():
    artwork_info = pd.read_csv('artwork_info_sources.csv', index_col=0)
    driver = GraphDatabase.driver(**BASE_AUTH)
    artwork_info_v2 = artwork_info[artwork_info.api_v2 == 1]

    artwork_info_v2.drop(['Category', 'Year'], axis=1, inplace=True)
    artwork_info_v2['artist_name'] = artwork_info_v2['artist']

    artwork_info_v2.drop(['name_in_artgraph', 'api_v1_artist',
                          'api_v1_artist_1', 'api_v1_url', 'api_v2'],
                         axis=1,
                         inplace=True)

    artwork_info_v2.progress_apply(lambda x: save_artwork(x, driver, 'recsys', v2=True), axis=1)


if __name__ == '__main__':
    main()