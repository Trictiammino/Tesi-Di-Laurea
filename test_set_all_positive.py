import pandas as pd

if __name__ == '__main__':

    test_set = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/splitting/'
                           'dataset_0-1/test.tsv', sep='\t', header=None)

    test_set_filtered = test_set[test_set.iloc[:, 2] == 1]

    print(test_set_filtered.to_string())

    test_set_filtered.to_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/splitting/'
                             'test_set_positive/test.tsv', sep='\t', header=False, index=False)
