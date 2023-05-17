import pandas as pd

if __name__ == '__main__':
    values = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/recs/MostPop.tsv',
                         sep='\t', header=None)

    valid_values = (values.iloc[:, 2] >= 0) & (values.iloc[:, 2] <= 100)

    # Calcola il minimo e il massimo valore della terza colonna
    min_value = values.iloc[:, 2].min()
    max_value = values.iloc[:, 2].max()

    # Mostro i valori che non soddisfano il requisito
    invalid_values = values.loc[~valid_values]

    # Stampo i valori non validi
    print("Valori non validi nella terza colonna:")
    print(invalid_values)

    # Stampa il minimo e il massimo valore
    print("Valore minimo della terza colonna:", min_value)
    print("Valore massimo della terza colonna:", max_value)