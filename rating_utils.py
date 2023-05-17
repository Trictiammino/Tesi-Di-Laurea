from utils import BASE_AUTH
import pandas as pd
from neo4j import GraphDatabase
import logging
import warnings
from tqdm import tqdm


logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
warnings.filterwarnings('ignore')
tqdm.pandas()


def add_rating(raw, driver, db):
    with driver.session(database=db) as session:
        session.run(f'''match (a:Artwork{{ code: "{raw.artwork}" }})
                        merge (u:User{{ name: "{raw.user}" }})
                        merge (u)-[:rates{{score: {raw.score} }}]->(a)''')


def main():
    ratings = pd.read_csv('ratings.csv', index_col=0)
    driver = GraphDatabase.driver(**BASE_AUTH)
    logging.info('adding ratings to db...')
    ratings.progress_apply(lambda x: add_rating(x, driver, 'recsys'), axis=1)
    logging.info('Done!')


if __name__ == '__main__':
    main()