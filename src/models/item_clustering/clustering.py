from sklearn.cluster import KMeans
import pandas as pd
import logging


class ModelKMeans:
    def __init__(self, model: KMeans = None, params: dict = None):
        self.model_obj = model
        self.params = params

    def cluster_items(self, data: pd.DataFrame, values_column: str, index_column: str = "week_num") -> pd.DataFrame:
        X, X_labels = self._prepare_dataset(data, values_column, index_column)
        self._fit_model(X)
        Y_labels = self.model_obj.labels_
        df_item_labels = pd.DataFrame(Y_labels, X_labels).reset_index()
        df_item_labels.columns = ['item', 'label']
        return df_item_labels

    @staticmethod
    def _prepare_dataset(data: pd.DataFrame, values_column, index_column):
        dataset = data.pivot(columns="item", values=values_column, index=index_column).fillna(0)
        X = dataset.to_numpy().transpose()
        X_labels = dataset.columns
        return X, X_labels

    def _fit_model(self, X) -> None:
        self.model_obj = KMeans(
            n_clusters=self.params['n_clusters']
            , random_state=self.params['random_state']
            , n_init=self.params['n_init']
            , max_iter=self.params['max_iter']
            , tol=self.params['tol']
            , n_jobs=self.params['n_jobs']
        ).fit(X)
        logging.info('Model trained')
