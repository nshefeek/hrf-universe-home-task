from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class DaysToHireStatsBase(BaseModel):
    id: UUID
    standard_job_id: str
    country_code: Optional[str] = None
    min_days_to_hire: Optional[float] = None
    max_days_to_hire: Optional[float] = None
    avg_days_to_hire: Optional[float] = None
    count_of_job_postings: Optional[int] = None

    class Config:
        json_encoders = {UUID: str}


class DaysToHireStatsCreate(DaysToHireStatsBase):
    pass


class DaysToHireStatsUpdate(DaysToHireStatsBase):
    standard_job_id: str
    country_code: Optional[str] = None
    min_days_to_hire: Optional[float] = None
    max_days_to_hire: Optional[float] = None
    avg_days_to_hire: Optional[float] = None
    count_of_job_postings: Optional[int] = None


class DaysToHireStatsInDB(DaysToHireStatsBase):
    standard_job_id: str
    country_code: Optional[str] = None
    min_days_to_hire: Optional[float] = None
    max_days_to_hire: Optional[float] = None
    avg_days_to_hire: Optional[float] = None
    count_of_job_postings: Optional[int] = None


class DaysToHireStatsResponse(DaysToHireStatsInDB):
    pass
