import pandas as pd


if __name__ == '__main__':

    dataset = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/config_files/data/dataset.tsv', sep='\t',
                          header=None)

    # Verifica iniziale del dataframe
    print("Dataframe originale:")
    print(dataset)

    # Trasformazione dei valori
    dataset.iloc[:, 2] = dataset.iloc[:, 2].apply(lambda x: 0 if x <= 0 else 1)

    # Controlla i valori unici nella terza colonna
    unique_values = dataset.iloc[:, 2].unique()

    # Verifica se i valori unici sono solo 0 e 1
    if set(unique_values) == {0, 1}:
        print("La terza colonna contiene solo valori 0 e 1")
    else:
        print("La terza colonna contiene altri valori oltre a 0 e 1")

    # Esporta il dataframe in formato TSV
    dataset.to_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/config_files/data/dataset_0-1.tsv', sep='\t',
                   index=False, header=None)
