# This is a sample Python script.
import pandas

# Press Maiusc+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# Press the green button in the gutter to run the script.

if __name__ == '__main__':
    import csv
    import pandas as pd

    # data = pd.read_csv("WikiArt-Emotions/WikiArt-annotations.csv")
    # data.drop(data.columns[[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
    # ,22,23,24,25,26,27,28,29,30,32,33,34,35,38,39,40,41,44,45,46,47,50,51,52,53,56,57,58,59]], inplace=True, axis=1)
    # print(data.to_string(max_rows=7))
    # data.to_csv('WikiArt-Emotions/dataset.csv', index=False)

    # data = pd.read_csv("WikiArt-Emotions/dataset.csv")
    # print(data)

    #with open("C:/Users/ReadyToUse/PycharmProjects/pythonProject/WikiArt-Emotions/dataset.csv", newline="",
    #         encoding="ISO-8859-1") as filecsv:
    #   lettore = csv.reader(filecsv, delimiter=",")

    #   i = 0

    #   with open('C:/Users/ReadyToUse/PycharmProjects/pythonProject/WikiArt-Emotions/dataset_finale.csv',
    #             'w', newline='') as csvfile:
    #       for row in lettore:

    #           persona = row[0]
    #           quadro_uno = row[1]
    #           rating_uno = row[2]
    #           quadro_due = row[3]
    #           rating_due = row[4]
    #           quadro_tre = row[5]
    #           rating_tre = row[6]
    #           quadro_quattro = row[7]
    #           rating_quattro = row[8]
    #           quadro_cinque = row[9]
    #           rating_cinque = row[10]

    #           if i == 0:

    #               writer = csv.writer(csvfile, delimiter=',')
    #               writer.writerow(['UTENTE', 'OPERA', 'RATING'])
    #               i += 1

    #           elif i > 0:

    #               writer = csv.writer(csvfile, delimiter=',')
    #               writer.writerow([persona, quadro_uno, rating_uno])
    #               writer.writerow([persona, quadro_due, rating_due])
    #               writer.writerow([persona, quadro_tre, rating_tre])
    #               writer.writerow([persona, quadro_quattro, rating_quattro])
    #               writer.writerow([persona, quadro_cinque, rating_cinque])

    #with open("C:/Users/ReadyToUse/PycharmProjects/pythonProject/WikiArt-Emotions/WikiArt-info.tsv", newline="",
    #          encoding="ISO-8859-1") as filetsv:
    #    file_tsv = csv.reader(filetsv, delimiter="\t")

    #    df = pd.read_csv("C:/Users/ReadyToUse/PycharmProjects/pythonProject/WikiArt-Emotions/dataset_finale.csv")

    #    for row in file_tsv:
    #        opera = row[0]
    #        url = row[5]

    #        df.loc[df["OPERA"] == opera, "OPERA"] = url

    #    df.to_csv("test.csv", index=False)

from neo4j import GraphDatabase
import pandas as pd

df = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/test.csv')


driver = GraphDatabase.driver(uri="bolt://localhost:7687", auth=('neo4j', 'neo4j'))
with driver.session(database='neo4j') as session:
    query = f'match (a:Artwork) where a.image_url in {df.OPERA.unique().tolist()} return a.name, a.image_url'
    ans = list(map(tuple, session.run(query)))
    print(ans)

    #with open('C:/Users/ReadyToUse/PycharmProjects/pythonProject/test2.csv',
    #          'w', newline='') as filecsv:
    #    writer = csv.writer(filecsv, delimiter=',')
    #    writer.writerow(["NOME", "URL", "RATING"])

    #    with open("C:/Users/ReadyToUse/PycharmProjects/pythonProject/test.csv", newline="",
    #              encoding="ISO-8859-1") as csvfile:
    #        file_tsv = csv.reader(csvfile, delimiter=",")
    #    for row in ans:
    #        url = row[1]
    #        for line in df:
    #            url2 = line[1]
    #            if url == url2:
    #                rating = line[2]
    #                writer.writerow([row[0], row[1], rating])

# df2 = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/test2.csv')
# df = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/test.csv')

# for row in df2:
#    URL = row[1]
#    for line in df:
#        if URL == line[1]:
#            RATING = line[2]
#    row[3] = RATING
