from dataclasses import dataclass
from typing import Optional

from sqlalchemy import (
    Column,
    Integer,
    Float,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.orm import registry
from sqlalchemy.dialects.postgresql import UUID


mapper_registry = registry()


class Model:
    pass


@mapper_registry.mapped
@dataclass
class StandardJobFamily(Model):
    __table__ = Table(
        "standard_job_family",
        mapper_registry.metadata,
        Column("id", String, nullable=False, primary_key=True),
        Column("name", String, nullable=False),
        schema="public",
    )

    id: str
    name: str


@mapper_registry.mapped
@dataclass
class StandardJob(Model):
    __table__ = Table(
        "standard_job",
        mapper_registry.metadata,
        Column("id", String, nullable=False, primary_key=True),
        Column("name", String, nullable=False),
        Column("standard_job_family_id", String, nullable=False),
        schema="public",
    )

    id: str
    name: str
    standard_job_family_id: str


@mapper_registry.mapped
@dataclass
class JobPosting(Model):
    __table__ = Table(
        "job_posting",
        mapper_registry.metadata,
        Column("id", String, nullable=False, primary_key=True),
        Column("title", String, nullable=False),
        Column("standard_job_id", String, nullable=False),
        Column("country_code", String, nullable=True),
        Column("days_to_hire", Integer, nullable=True),
        schema="public",
    )

    id: str
    title: str
    standard_job_id: str
    country_code: Optional[str] = None
    days_to_hire: Optional[int] = None


@mapper_registry.mapped
@dataclass
class DaysToHireStats(Model):
    __table__ = Table(
        "days_to_hire_stats",
        mapper_registry.metadata,
        Column("id", UUID(as_uuid=True), nullable=False, primary_key=True),
        Column("standard_job_id", String, nullable=False, index=True),
        Column("country_code", String, nullable=True, index=True),
        Column("avg_days_to_hire", Float, nullable=True),
        Column("min_days_to_hire", Float, nullable=True),
        Column("max_days_to_hire", Float, nullable=True),
        Column("count_of_job_postings", Integer, nullable=True),
        schema="public",
    )

    __table_args__ = (
        UniqueConstraint(
            "standard_job_id",
            "country_code",
            name="pk_days_to_hire_stats",
        ),
    )

    id: str
    standard_job_id: str
    country_code: Optional[str] = None
    avg_days_to_hire: Optional[float] = None
    min_days_to_hire: Optional[float] = None
    max_days_to_hire: Optional[float] = None
    count_of_job_postings: Optional[int] = None
