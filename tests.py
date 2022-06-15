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

def main():
    clean_up()
    test_create_new_studies()
    test_create_new_trials()

if __name__ == "__main__":
    main()
