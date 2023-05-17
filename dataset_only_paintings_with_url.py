if __name__ == '__main__':
    import pandas as pd
    import csv

    with open("C:/Users/ReadyToUse/PycharmProjects/pythonProject/WikiArt-Emotions/WikiArt-info.tsv", newline="",
              encoding="ISO-8859-1") as filetsv:
        file_tsv = csv.reader(filetsv, delimiter="\t")

        df = pd.read_csv("C:/Users/ReadyToUse/PycharmProjects/pythonProject/"
                         "WikiArt-Emotions/dataset_only_paintings.csv")

        for row in file_tsv:
            opera = row[0]
            url = row[5]

            df.loc[df["OPERA"] == opera, "OPERA"] = url

        print(df.to_string())
        df.to_csv("WikiArt-Emotions/dataset_only_paintings_with_url.csv", index=False)
