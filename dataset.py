if __name__ == '__main__':
    import pandas as pd

    data = pd.read_csv("WikiArt-Emotions/WikiArt-annotations.csv")
    data.drop(data.columns[[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
    ,22,23,24,25,26,27,28,29,30,32,33,35,38,39,41,44,45,47,50,51,53,56,57,59]], inplace=True, axis=1)
    print(data.to_string(max_rows=7))
    data.to_csv('WikiArt-Emotions/dataset.csv', index=False)

