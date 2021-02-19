class ModelKMeansConfig:
    def __init__(self):
        self.model_hp = {
            "n_clusters": 400,
            "random_state": 0,
            "n_init": 10,
            "max_iter": 100,
            "tol": 0.01,
            "n_jobs": -1,
        }
