import optuna
from optuna.samplers import RandomSampler

from optuna_mongodb_storage import MongoDBStorage


def objective(trial):
    x = trial.suggest_float("x", -100, 100)
    y = trial.suggest_categorical("y", [-1, 0, 1])
    return x**2 + y


if __name__ == "__main__":
    storage = MongoDBStorage()
    storage._study_table.delete_many({})
    storage._trial_table.delete_many({})

    study = optuna.create_study(storage=storage, sampler=RandomSampler())

    study.optimize(objective, n_trials=1)
    print("Best value: {} (params: {})\n".format(study.best_value, study.best_params))
