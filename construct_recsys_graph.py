from artgraph_utils import main as artgraph
from wikiart_v1_utils import main as wikiart_v1
from wikiart_v2_utils import main as wikiart_v2
from rating_utils import main as ratings
import logging
import warnings

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')


if __name__ == '__main__':
    logging.info('migrating from ArtGraph...')
    artgraph()

    logging.info('loading into ArtGraph from WikiArt v1 API...')
    wikiart_v1()

    logging.info('loading into Artgraph from WikiArt v2 API...')
    wikiart_v2()

    logging.info('loading ratings...')
    ratings()