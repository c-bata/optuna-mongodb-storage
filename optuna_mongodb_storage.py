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


def _study_direction_to_string(study_direction: StudyDirection) -> str:
    if study_direction == StudyDirection.MAXIMIZE:
        return "maximize"
    elif study_direction == StudyDirection.MINIMIZE:
        return "minimize"
    else:
        return "not_set"


def _string_to_study_direction(direction: str) -> StudyDirection:
    if direction == "maximize":
        return StudyDirection.MAXIMIZE
    elif direction == "minimize":
        return StudyDirection.MINIMIZE
    elif direction == "not_set":
        return StudyDirection.NOT_SET


def _trial_state_to_string(trial_state: TrialState) -> str:
    if trial_state == TrialState.RUNNING:
        return "running"
    elif trial_state == TrialState.COMPLETE:
        return "complete"
    elif trial_state == TrialState.PRUNED:
        return "pruned"
    elif trial_state == TrialState.FAIL:
        return "fail"
    elif trial_state == TrialState.WAITING:
        return "waiting"


def _string_to_trial_state(state: str) -> TrialState:
    if state == "running":
        return TrialState.RUNNING
    elif state == "complete":
        return TrialState.COMPLETE
    elif state == "pruned":
        return TrialState.PRUNED
    elif state == "fail":
        return TrialState.FAIL
    elif state == "waiting":
        return TrialState.WAITING


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
            study_name = "{}{:010d}".format(
                DEFAULT_STUDY_NAME_PREFIX, study_id)

        default_study_record = {
            "study_name": study_name,
            "directions": [_study_direction_to_string(StudyDirection.NOT_SET)],
            "user_attrs": {},
            "system_attrs": {},
            "study_id": study_id,
            "deleted": False,
            "datetime_start": datetime.datetime.now()
        }

        self._study_table.insert_one(default_study_record)
        # self._set_study_record(study_id, default_study_record)

        _logger.info(
            "A new study created in MongoDB with name: {}".format(study_name))

        return study_id

    def _set_study_record(self, study_id: int, study_record) -> None:
        self._study_table.replace_one(
            {"study_id": study_id}, study_record, upsert=True)

    def _check_study_id(self, study_id: int) -> None:
        if self._study_table.count_documents({"study_id": study_id}) != 1:
            raise KeyError("study_id {} does not exist.".format(study_id))

    def delete_study(self, study_id: int) -> None:
        self._check_study_id(study_id)
        self._study_table.update_one({"study_id": study_id}, {
                                     "$set": {"deleted": True}})

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
            directions=[_string_to_study_direction(
                d) for d in study_record["directions"]]
        )

    def get_all_study_summaries(self, include_best_trial: bool) -> List[StudySummary]:
        # TODO: n_trials, best_trial
        study_records = self._study_table.find({"deleted": False})
        study_summaries = [self._convert_study_record_to_summary(
            study_record) for study_record in study_records]
        return study_summaries

    def _convert_frozen_trial_to_record(self, trial: FrozenTrial) -> Dict[str, Any]:
        return {
            "trial_id": trial._trial_id,
            "number": trial.number,
            "state": _trial_state_to_string(trial.state),
            "params": trial.params,
            "distributions": trial.distributions,
            "user_attrs": trial.user_attrs,
            "system_attrs": trial.system_attrs,
            "values": trial.values,
            "intermediate_values": trial.intermediate_values,
            "datetime_start": trial.datetime_start,
            "datetime_complete": trial.datetime_complete
        }

    def create_new_trial(
        self, study_id: int, template_trial: Optional[FrozenTrial] = None
    ) -> int:
        if template_trial is None:
            default_trial_record = {
                "trial_id": -1,
                "number": -1,
                "state": _trial_state_to_string(TrialState.RUNNING),
                "params": {},
                "distributions": {},
                "user_attrs": {},
                "system_attrs": {},
                "values": {},
                "intermediate_values": {},
                "datetime_start": datetime.datetime.now(),
                "datetime_complete": None,
            }
        else:
            default_trial_record = self._convert_frozen_trial_to_record(
                template_trial)

        self._check_study_id(study_id)
        trial_id = self._trial_table.count_documents({})
        trial_number = self._trial_table.count_documents(
            {"study_id": study_id})
        default_trial_record["trial_id"] = trial_id
        default_trial_record["number"] = trial_number

        self._trial_table.insert_one(default_trial_record)

        return default_trial_record

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

    def _check_trial_id(self, trial_id: int) -> None:
        if self._trial_table.count_documents({"trial_id": trial_id}) != 1:
            raise KeyError("trial_id {} does not exist.".format(trial_id))

    def _get_trial_record(self, trial_id: int) -> Dict[str, Any]:
        return self._trial_table.find_one({"trial_id": trial_id})

    def _convert_record_to_frozen_trial(self, trial_record: Dict[str, Any]) -> FrozenTrial:
        return FrozenTrial(
            trial_id=trial_record["trial_id"],
            number=trial_record["number"],
            state=_string_to_trial_state(trial_record["state"]),
            params=trial_record["params"],
            distributions=trial_record["distributions"],
            user_attrs=trial_record["user_attrs"],
            system_attrs=trial_record["system_attrs"],
            values=trial_record["values"],
            intermediate_values=trial_record["intermediate_values"],
            datetime_start=trial_record["datetime_start"],
            datetime_complete=trial_record["datetime_complete"]
        )

    def get_trial(self, trial_id: int) -> FrozenTrial:
        self._check_trial_id(trial_id)
        trial_record = self._get_trial_record(trial_id)
        return self._convert_record_to_frozen_trial(trial_record)

    def get_all_trials(
        self,
        study_id: int,
        deepcopy: bool = True,
        states: Optional[Container[TrialState]] = None,
    ) -> List[FrozenTrial]:
        pass

    def read_trials_from_remote_storage(self, study_id: int) -> None:
        pass
