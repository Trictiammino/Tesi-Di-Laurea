import pandas as pd
from sklearn.model_selection import train_test_split

if __name__ == '__main__':

    dataset = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/config_files/data/dataset.tsv', sep='\t',
                          header=None)

    # Divide il DataFrame in set di addestramento e set di test (80% addestramento, 20% test)
    train_df, test_df = train_test_split(dataset, test_size=0.2, random_state=42)

    # Divide il set di test in set di test e set di validazione (50% test, 50% validazione)
    test_df, validation_df = train_test_split(test_df, test_size=0.5, random_state=42)

    train_df.to_csv("C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/splitting/dataset/train.tsv",
                    sep="\t", index=False, header=False)
    test_df.to_csv("C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/splitting/dataset/test.tsv",
                   sep="\t", index=False, header=False)
    validation_df.to_csv("C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/splitting/dataset/"
                         "validation.tsv", sep="\t", index=False, header=False)

