import optuna

from optuna_mongodb_storage import MongoDBStorage


# TODO: implement get_best_trial() and set_trial_state_values()


def objective(trial):
    x = trial.suggest_float("x", -100, 100)
    y = trial.suggest_categorical("y", [-1, 0, 1])
    return x**2 + y


if __name__ == "__main__":
    study = optuna.create_study(
        storage=MongoDBStorage()
    )
    study.optimize(objective, n_trials=10)
    print("Best value: {} (params: {})\n".format(study.best_value, study.best_params))
