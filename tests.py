from optuna.distributions import FloatDistribution

from optuna_mongodb_storage import MongoDBStorage


def clean_up():
    storage = MongoDBStorage()
    storage._study_table.delete_many({})
    storage._trial_table.delete_many({})


def test_create_new_studies():
    storage = MongoDBStorage()
    study_id = storage.create_new_study()
    studies = storage.get_all_study_summaries(include_best_trial=False)
    assert len(studies) == 1
    storage.delete_study(study_id)


def test_create_new_trials():
    storage = MongoDBStorage()
    study_id = storage.create_new_study()
    trial_id = storage.create_new_trial(study_id)
    assert 1 == len(storage.get_all_trials(study_id))
    trial = storage.get_trial(trial_id)


def test_set_trial_param():
    storage = MongoDBStorage()
    study_id = storage.create_new_study()
    trial_id = storage.create_new_trial(study_id)
    storage.set_trial_param(trial_id, "foo", 0.1, FloatDistribution(low=0.0, high=1.0))
    trial = storage.get_trial(trial_id)
    assert trial.params == {"foo": 0.1}
    assert trial.distributions == {"foo": FloatDistribution(low=0.0, high=1.0)}


def main():
    clean_up()
    test_create_new_studies()
    test_create_new_trials()
    test_set_trial_param()


if __name__ == "__main__":
    main()
