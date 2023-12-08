from datetime import datetime
from typing import Any, Mapping, Optional

import dagster._check as check
from dateutil.parser import isoparse

from ..types import DbtOutput


class ParadimeOutput(DbtOutput):
    def __init__(
        self,
        run_details: Mapping[str, Any],
        result: Mapping[str, Any],
    ):
        self._run_details = check.mapping_param(
            run_details, "run_details", key_type=str
        )
        super().__init__(result)

    @property
    def run_details(self) -> Mapping[str, Any]:
        return self._run_details

    @property
    def job_id(self) -> int:
        return self.run_details["job_id"]

    @property
    def job_name(self) -> Optional[str]:
        job = self.run_details["job"]
        return job.get("name") if job else None

    @property
    def docs_url(self) -> Optional[str]:
        return "https://app.paradime.io/catalog/search"

    @property
    def run_id(self) -> int:
        return self.run_details["id"]

    @property
    def created_at(self) -> datetime:
        return isoparse(self.run_details["created_at"])

    @property
    def updated_at(self) -> datetime:
        return isoparse(self.run_details["updated_at"])

    @property
    def dequeued_at(self) -> datetime:
        return isoparse(self.run_details["dequeued_at"])

    @property
    def started_at(self) -> datetime:
        return isoparse(self.run_details["started_at"])

    @property
    def finished_at(self) -> datetime:
        return isoparse(self.run_details["finished_at"])
