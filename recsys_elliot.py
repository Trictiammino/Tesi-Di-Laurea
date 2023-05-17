import pandas as pd
from elliot.run import run_experiment

if __name__ == '__main__':

    # caricamento del file delle relazioni utente-opera
    # user_artwork = pd.read_csv('artgraph2recsys/raw/relations/user___rates___artwork/edge.csv', header=None,
    #                           names=['user_id', 'item_id'])

    # caricamento del file delle valutazioni degli utenti
    # scores = pd.read_csv('artgraph2recsys/raw/relations/user___rates___artwork/attributes.csv', header=None,
    #                     names=['score'])

    # ratings = user_artwork.merge(scores, left_index=True, right_index=True)

    # 55ratings.to_csv('config_files/data/dataset.csv', index=False, header=False)
    print('FATTO')

    run_experiment("config_files/configuration.yml")
