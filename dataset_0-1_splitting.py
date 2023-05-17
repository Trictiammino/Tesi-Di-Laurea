import pandas as pd
from sklearn.model_selection import train_test_split

if __name__ == '__main__':

    dataset = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/config_files/data/dataset_0-1.tsv',
                          sep='\t', header=None)

    terza_colonna = dataset.iloc[:, 2]
    conteggio_valori = terza_colonna.value_counts()

    print("Il numero di valori del dataset è:")
    print(conteggio_valori)

    print("\nLa proporzione di valori del dataset è:")
    proporzione_valori = terza_colonna.value_counts(normalize=True)

    print(proporzione_valori)

    # Divido il dataset
    X = dataset.iloc[:, :-1].values
    y = dataset.iloc[:, -1].values

    # Divide il DataFrame in set di addestramento e set di test (80% addestramento, 20% test)
    # Suddivisione dell'80% per l'addestramento, 10% per la validazione, 10% per il test
    X_train, X_test_val, y_train, y_test_val = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
    X_val, X_test, y_val, y_test = train_test_split(X_test_val, y_test_val, test_size=0.5, stratify=y_test_val,
                                                    random_state=42)

    # Converti le suddivisioni in DataFrame di pandas senza nomi di colonne
    train_df = pd.DataFrame(X_train)
    train_df[''] = y_train

    val_df = pd.DataFrame(X_val)
    val_df[''] = y_val

    test_df = pd.DataFrame(X_test)
    test_df[''] = y_test

    train_df.to_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/splitting/dataset_0-1/'
                    'train.tsv', sep='\t', header=False, index=False)
    val_df.to_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/splitting/dataset_0-1/'
                  'validation.tsv', sep='\t', header=False, index=False)
    test_df.to_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/splitting/dataset_0-1/'
                   'test.tsv', sep='\t', header=False, index=False)

    #dataset = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/splitting/dataset_0-1/'
    #                      'test.tsv', sep='\t', header=None)
    #print("\n", dataset)
