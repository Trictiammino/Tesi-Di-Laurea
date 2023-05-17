import pandas as pd

if __name__ == '__main__':

    test_set = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/splitting/'
                           'dataset_0-1/test.tsv', sep='\t', header=None)

    # Carico le recs del modello FunkSVD
    train_set_funksvd = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/recs/'
                                    'FunkSVD_seed=42_e=2_bs=49337_factors=10_lr=0$001_reg_w=0$1_reg_b=0$001_it=2.tsv',
                                    sep='\t', header=None)

    # Filtro il train set basandomi sulle prime 2 colonne user-item
    filtered_train_set_funksvd = train_set_funksvd.merge(test_set.iloc[:, :2], on=[0, 1])

    # Stampo il train set filtrato
    print('RECS FUNKSVD\n', filtered_train_set_funksvd)

    filtered_train_set_funksvd.to_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/'
                                      'artgraph/valutation_file/FunkSVD/train.tsv', sep='\t',
                                      header=False, index=False)

    print('---------------------------------------------------------------------------')

    # Carico le recs del modello BPRMF
    train_set_bprmf = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/recs/'
                                  'BPRMF_seed=42_e=30_bs=1_f=10_lr=0$01_bias_reg=0_u_reg=0$01_pos_i_reg=0$01_'
                                  'neg_i_reg=0$00025_up_neg_i_f=True_up_u=True_up_i=True_up_b=True_it=30.tsv',
                                  sep='\t', header=None)

    # Filtro il train set basandomi sulle prime 2 colonne user-item
    filtered_train_set_bprmf = train_set_bprmf.merge(test_set.iloc[:, :2], on=[0, 1])

    # Stampo il train set filtrato
    print('RECS BPRMF\n', filtered_train_set_bprmf)

    filtered_train_set_bprmf.to_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/'
                                    'artgraph/valutation_file/BPRMF/train.tsv', sep='\t',
                                    header=False, index=False)

    print('---------------------------------------------------------------------------')

    # Carico le recs del modello MultiVAE
    train_set_multivae = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/recs/'
                                     'MultiVAE_seed=42_e=20_bs=128_intermediate_dim=500_latent_dim=300_reg_lambda=10_'
                                     'lr=0$01_dropout_pkeep=0$0_it=20.tsv',
                                     sep='\t', header=None)

    # Filtro il train set basandomi sulle prime 2 colonne user-item
    filtered_train_set_multivae = train_set_multivae.merge(test_set.iloc[:, :2], on=[0, 1])

    # Stampo il train set filtrato
    print('RECS MULTIVAE\n', filtered_train_set_multivae)

    filtered_train_set_multivae.to_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/'
                                       'artgraph/valutation_file/MultiVAE/train.tsv', sep='\t',
                                       header=False, index=False)

    print('---------------------------------------------------------------------------')

    # Carico le recs del modello NeuMF
    train_set_neumf = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/recs/'
                                  'NeuMF_seed=42_e=20_bs=512_lr=0$0006216443713932542_mffactors=11_drop=0_'
                                  'mftrain=True_mlptrain=True_m=0_it=18.tsv',
                                  sep='\t', header=None)

    # Filtro il train set basandomi sulle prime 2 colonne user-item
    filtered_train_set_neumf = train_set_neumf.merge(test_set.iloc[:, :2], on=[0, 1])

    # Stampo il train set filtrato
    print('RECS NEUMF\n', filtered_train_set_neumf)

    filtered_train_set_neumf.to_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/'
                                    'artgraph/valutation_file/NeuMF/train.tsv', sep='\t',
                                     header=False, index=False)

    print('---------------------------------------------------------------------------')

    # Carico le recs del modello MostPop
    #train_set_mostpop = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/recs/'
    #                                'MostPop.tsv',
    #                                sep='\t', header=None)

    # Filtro il train set basandomi sulle prime 2 colonne user-item
    #filtered_train_set_mostpop = train_set_mostpop.merge(test_set.iloc[:, :2], on=[0, 1])

    # Stampo il train set filtrato
    #print('RECS MOSTPOP\n', filtered_train_set_mostpop)

    #filtered_train_set_mostpop.to_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/'
    #                                  'artgraph/valutation_file/MostPop/train.tsv', sep='\t',
    #                                  header=False, index=False)

    #print('---------------------------------------------------------------------------')

    # Carico le recs del modello Random
    train_set_random = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/recs/'
                                   'Random_seed=42.tsv',
                                   sep='\t', header=None)

    # Filtro il train set basandomi sulle prime 2 colonne user-item
    filtered_train_set_random = train_set_random.merge(test_set.iloc[:, :2], on=[0, 1])

    # Stampo il train set filtrato
    print('RECS RANDOM\n', filtered_train_set_random)

    filtered_train_set_random.to_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/'
                                     'artgraph/valutation_file/Random/train.tsv', sep='\t',
                                     header=False, index=False)

    print('---------------------------------------------------------------------------')

    # Carico le recs del modello ItemKNN
    train_set_itemknn = pd.read_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/artgraph/recs/'
                                    'ItemKNN_nn=5_sim=cosine_imp=standard_bin=False_shrink=0_norm=True_'
                                    'asymalpha=_tvalpha=_tvbeta=_rweights=.tsv',
                                    sep='\t', header=None)

    # Filtro il train set basandomi sulle prime 2 colonne user-item
    filtered_train_set_itemknn = train_set_itemknn.merge(test_set.iloc[:, :2], on=[0, 1])

    # Stampo il train set filtrato
    print('RECS ITEMKNN\n', filtered_train_set_itemknn)

    filtered_train_set_itemknn.to_csv('C:/Users/ReadyToUse/PycharmProjects/pythonProject/results/'
                                      'artgraph/valutation_file/ItemKNN/train.tsv', sep='\t',
                                      header=False, index=False)

    print('---------------------------------------------------------------------------')

