if __name__ == '__main__':
    import pandas as pd
    import csv

    with open("C:/Users/ReadyToUse/PycharmProjects/pythonProject/WikiArt-Emotions/dataset.csv", newline="",
              encoding="ISO-8859-1") as filecsv:
        lettore = csv.reader(filecsv, delimiter=",")

        i = 0

        with open('C:/Users/ReadyToUse/PycharmProjects/pythonProject/WikiArt-Emotions/dataset_reordered.csv',
                  'w', newline='') as csvfile:
            for row in lettore:

                persona = row[0]
                quadro_uno = row[1]
                is_painting_uno = row[2]
                rating_uno = row[3]
                quadro_due = row[4]
                is_painting_due = row[5]
                rating_due = row[6]
                quadro_tre = row[7]
                is_painting_tre = row[8]
                rating_tre = row[9]
                quadro_quattro = row[10]
                is_painting_quattro = row[11]
                rating_quattro = row[12]
                quadro_cinque = row[13]
                is_painting_cinque = row[14]
                rating_cinque = row[15]

                if i == 0:

                    writer = csv.writer(csvfile, delimiter=',')
                    writer.writerow(['UTENTE', 'OPERA', 'IS_PAINTING', 'RATING'])
                    i += 1

                elif i > 0:

                    writer = csv.writer(csvfile, delimiter=',')
                    writer.writerow([persona, quadro_uno, is_painting_uno, rating_uno])
                    writer.writerow([persona, quadro_due, is_painting_due, rating_due])
                    writer.writerow([persona, quadro_tre, is_painting_tre, rating_tre])
                    writer.writerow([persona, quadro_quattro, is_painting_quattro, rating_quattro])
                    writer.writerow([persona, quadro_cinque, is_painting_cinque, rating_cinque])

        data = pd.read_csv("WikiArt-Emotions/dataset_reordered.csv")
        print(data)
        data = data[data["IS_PAINTING"] != 'no']
        data.to_csv('WikiArt-Emotions/dataset_only_paintings.csv', index=False)
        print(data)
