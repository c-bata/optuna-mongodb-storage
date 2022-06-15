import datetime
from typing import Optional, Container, List, Any, Sequence, Dict

import optuna
from optuna import exceptions
from optuna.distributions import BaseDistribution
from optuna.storages import BaseStorage
from optuna.storages._base import DEFAULT_STUDY_NAME_PREFIX
from optuna.study import StudySummary, StudyDirection
from optuna.trial import TrialState, FrozenTrial
from pymongo import MongoClient


_logger = optuna.logging.get_logger(__name__)


class MongoDBStorage(BaseStorage):
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None) -> None:
        self._client = MongoClient(host=host, port=port)
        self._mongodb = self._client.optuna_study_database
        self._study_table = self._mongodb.studies
        self._trial_table = self._mongodb.trials

    def create_new_study(self, study_name: Optional[str] = None) -> int:
        if study_name is not None and self._study_table.count_documents({"study_name": study_name}) != 0:
            raise exceptions.DuplicatedStudyError

        study_id = self._study_table.count_documents({})

        if study_name is None:
            study_name = "{}{:010d}".format(DEFAULT_STUDY_NAME_PREFIX, study_id)

        default_study_record = {
            "study_name": study_name,
            "directions": [StudyDirection.NOT_SET],
            "user_attrs": {},
            "system_attrs": {},
            "study_id": study_id,
            "deleted": False,
            "datetime_start": datetime.datetime.now()
        }

        self._set_study_record(study_id, default_study_record)

        _logger.info("A new study created in MongoDB with name: {}".format(study_name))

        return study_id

    def _set_study_record(self, study_id: int, study_record) -> None:
        self._study_table.replace_one({"study_id", study_id}, study_record, upsert=True)

    def delete_study(self, study_id: int) -> None:
        pass

    def set_study_user_attr(self, study_id: int, key: str, value: Any) -> None:
        pass

    def set_study_system_attr(self, study_id: int, key: str, value: Any) -> None:
        pass

    def set_study_directions(
        self, study_id: int, directions: Sequence[StudyDirection]
    ) -> None:
        pass

    def get_study_id_from_name(self, study_name: str) -> int:
        pass

    def get_study_name_from_id(self, study_id: int) -> str:
        pass

    def get_study_directions(self, study_id: int) -> List[StudyDirection]:
        pass

    def get_study_user_attrs(self, study_id: int) -> Dict[str, Any]:
        pass

    def get_study_system_attrs(self, study_id: int) -> Dict[str, Any]:
        pass

    def _convert_study_record_to_summary(self, study_record: Dict[str, Any]) -> StudySummary:
        return StudySummary(
            study_name=study_record["study_name"],
            direction=None,
            best_trial=None,
            user_attrs=study_record["user_attrs"],
            system_attrs=study_record["system_attrs"],
            n_trials=0,
            datetime_start=study_record["datetime_start"],
            study_id=study_record["study_id"],
            directions=study_record["directions"]
        )

    def get_all_study_summaries(self, include_best_trial: bool) -> List[StudySummary]:
        # TODO: n_trials, best_trial
        study_records = self._study_table.find({"deleted": False})
        study_summaries = [self._convert_study_record_to_summary(
            study_record) for study_record in study_records]
        return study_summaries

    def create_new_trial(
        self, study_id: int, template_trial: Optional[FrozenTrial] = None
    ) -> int:
        pass

    def set_trial_param(
        self,
        trial_id: int,
        param_name: str,
        param_value_internal: float,
        distribution: BaseDistribution,
    ) -> None:
        pass

    def get_trial_id_from_study_id_trial_number(
        self, study_id: int, trial_number: int
    ) -> int:
        pass

    def set_trial_state_values(
        self, trial_id: int, state: TrialState, values: Optional[Sequence[float]] = None
    ) -> bool:
        pass

    def set_trial_intermediate_value(
        self, trial_id: int, step: int, intermediate_value: float
    ) -> None:
        pass

    def set_trial_user_attr(self, trial_id: int, key: str, value: Any) -> None:
        pass

    def set_trial_system_attr(self, trial_id: int, key: str, value: Any) -> None:
        pass

    def get_trial(self, trial_id: int) -> FrozenTrial:
        pass

    def get_all_trials(
        self,
        study_id: int,
        deepcopy: bool = True,
        states: Optional[Container[TrialState]] = None,
    ) -> List[FrozenTrial]:
        pass

    def read_trials_from_remote_storage(self, study_id: int) -> None:
        pass
