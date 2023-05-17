import clayrs.content_analyzer as ca
import clayrs.recsys as rs
import clayrs.evaluation as eva


if __name__ == '__main__':

    raw_source = ca.CSVFile('')

    movies_ca_config = ca.ItemAnalyzerConfig(
        source=raw_source,
        id='artwork',  # id which uniquely identifies each item
        output_directory='artwork_codified/'  # where items complexly represented will be stored
    )

    ca.ContentAnalyzer(movies_ca_config).fit()

    ratings = ca.Ratings(ca.CSVFile(''))

    train_list, test_list = rs.KFoldPartitioning(n_splits=2).split_all(ratings)

    centroid_vec = rs.CentroidVector(
        {'Genre': [0, 1]},

        similarity=rs.CosineSimilarity()
    )

    result_list = []

    for train_set, test_set in zip(train_list, test_list):
        cbrs = rs.ContentBasedRS(centroid_vec, train_set, 'artwork_codified/')
        rank = cbrs.fit_rank(test_set, n_recs=10)

        result_list.append(rank)

    em = eva.EvalModel(
        pred_list=result_list,
        truth_list=test_list,
        metric_list=[
            eva.NDCG(),
            eva.Precision(),
            eva.RecallAtK(k=5)
        ]
    )

    sys_result, users_result = em.fit()
