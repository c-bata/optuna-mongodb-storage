from optuna_mongodb_storage import MongoDBStorage


def test_create_new_studies():
    storage = MongoDBStorage()
    storage.create_new_study("foo")
    studies = storage.get_all_study_summaries(include_best_trial=False)
    assert len(studies) == 1


def main():
    test_create_new_studies()


if __name__ == '__main__':
    main()
