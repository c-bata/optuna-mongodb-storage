from typing import Optional, Container, List, Any, Sequence, Dict

from optuna.distributions import BaseDistribution
from optuna.storages import BaseStorage
from optuna.study import StudySummary, StudyDirection
from optuna.trial import TrialState, FrozenTrial


class MongoDBStorage(BaseStorage):
    def create_new_study(self, study_name: Optional[str] = None) -> int:
        pass

    def delete_study(self, study_id: int) -> None:
        pass

    def set_study_user_attr(self, study_id: int, key: str, value: Any) -> None:
        pass

    def set_study_system_attr(self, study_id: int, key: str, value: Any) -> None:
        pass

    def set_study_directions(self, study_id: int, directions: Sequence[StudyDirection]) -> None:
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

    def get_all_study_summaries(self, include_best_trial: bool) -> List[StudySummary]:
        pass

    def create_new_trial(self, study_id: int, template_trial: Optional[FrozenTrial] = None) -> int:
        pass

    def set_trial_param(self, trial_id: int, param_name: str, param_value_internal: float,
                        distribution: BaseDistribution) -> None:
        pass

    def get_trial_id_from_study_id_trial_number(self, study_id: int, trial_number: int) -> int:
        pass

    def set_trial_state_values(self, trial_id: int, state: TrialState,
                               values: Optional[Sequence[float]] = None) -> bool:
        pass

    def set_trial_intermediate_value(self, trial_id: int, step: int, intermediate_value: float) -> None:
        pass

    def set_trial_user_attr(self, trial_id: int, key: str, value: Any) -> None:
        pass

    def set_trial_system_attr(self, trial_id: int, key: str, value: Any) -> None:
        pass

    def get_trial(self, trial_id: int) -> FrozenTrial:
        pass

    def get_all_trials(self, study_id: int, deepcopy: bool = True, states: Optional[Container[TrialState]] = None) -> \
    List[FrozenTrial]:
        pass

    def read_trials_from_remote_storage(self, study_id: int) -> None:
        pass