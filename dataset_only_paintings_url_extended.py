if __name__ == '__main__':
    import pandas as pd
    import csv

    df1 = pd.read_csv("C:/Users/ReadyToUse/PycharmProjects/pythonProject/"
                      "WikiArt-Emotions/dataset_only_paintings_with_url.csv")

    df2 = pd.read_table("C:/Users/ReadyToUse/PycharmProjects/pythonProject/WikiArt-Emotions/WikiArt-info.tsv")

    df2.drop(df2.columns[[0]], inplace=True, axis=1)

    #print(df1, 3)

    df2.rename(columns={'Image URL': 'OPERA', 'Category': 'CATEGORY', 'Artist': 'ARTIST', 'Title': 'TITLE',
                        'Year': 'YEAR', 'Painting Info URL': 'PAINTING INFO URL',
                        'Artist Info URL': 'ARTIST INFO URL'}, inplace=True)

    result = pd.merge(df1, df2, on="OPERA")

    result.to_csv('WikiArt-Emotions/dataset_only_paintings_url_extended.csv', index=False)
