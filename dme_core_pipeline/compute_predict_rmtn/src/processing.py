from libs.model.train import ModelTrain

if __name__ == '__main__':
    mt = ModelTrain('compute_erm_train', 'compute_erm_test', 'rmtn', 'erm_test_scored.csv')
    mt.predict()
