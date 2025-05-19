import uuid
import logging
import traceback

from typing import Optional, List, Iterator, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from home_task.db import get_session
from home_task.models import DaysToHireStats
from home_task.schemas import DaysToHireStatsCreate
from home_task.utils import calculate_stats


logger = logging.getLogger(__name__)


def get_stats_by_job_and_country(
        db: Session,
        standard_job_id: str,
        country_code: Optional[str] = None,
) -> Optional[DaysToHireStats]:
    logger.info("Fetching stats for standard_job_id: %s, country_code: %s", standard_job_id, country_code)
    try:
        query = db.query(DaysToHireStats).filter(
        DaysToHireStats.standard_job_id == standard_job_id
    )
        if country_code:
            query = query.filter(DaysToHireStats.country_code == country_code)
        else:
            query = query.filter(DaysToHireStats.country_code.is_(None))
        result = query.first()
        if result:
            logger.debug("Found stats: id=%s, min_days=%.2f, avg_days=%.2f, max_days=%.2f, count=%d", 
                         result.id, result.min_days_to_hire, result.avg_days_to_hire, 
                         result.max_days_to_hire, result.count_of_job_postings)
            return result
        else:
            logger.debug("No stats found for standard_job_id: %s, country_code: %s", standard_job_id, country_code)
            return None
    except SQLAlchemyError as e:
        raise RuntimeError(f"Error fetching stats: {e}") from e


    

def get_distinct_standard_job_ids(db: Session) -> List[str]:
    query = text(
        "SELECT DISTINCT standard_job_id  \
        FROM job_posting \
        WHERE standard_job_id IS NOT NULL AND days_to_hire IS NOT NULL;")
    result = db.execute(query)
    return [row[0] for row in result.fetchall()]


def get_distinct_coutry_codes(db: Session, standard_job_id: str) -> List[str]:
    query = text(
        "SELECT DISTINCT country_code  \
        FROM job_posting \
        WHERE standard_job_id = :sjid AND country_code IS NOT NULL;")
    result = db.execute(query, {"sjid": standard_job_id})
    return [row[0] for row in result.fetchall()]


def get_days_to_hire_for_sjid_country(
        db: Session,
        standard_job_id: str,
        country_code: Optional[str] = None,
        batch_size: int = 1000,
) -> Iterator[List[int]]:
    
    params = {
        "standard_job_id": standard_job_id,
        "limit": batch_size,
    }
    country_filer = ""

    if country_code:
        country_filer = "AND country_code = :country_code"
        params["country_code"] = country_code
    else:
        country_filer = "AND country_code IS NULL"

    offset = 0
        
    query_str = f"""
        SELECT days_to_hire
        FROM job_posting
        WHERE standard_job_id = :standard_job_id
        AND days_to_hire IS NOT NULL
        {country_filer}
        LIMIT :limit OFFSET :offset;
    """

    query = text(query_str)
    while True:
        params["offset"] = offset
        try:
            result = db.execute(query, params)
            current_batch = [row[0] for row in result.fetchall() if row[0] is not None]
        except Exception as e:
            print(f"Error executing query: {e}")
            break
        if not current_batch:
            break

        yield current_batch
        offset += batch_size


def upsert_stats(
        db: Session,
        stats_data: DaysToHireStatsCreate,
) -> DaysToHireStats:
    logger.info("Upserting stats for standard_job_id=%s, country_code=%s", 
                stats_data.standard_job_id, stats_data.country_code)
    try:
        existing_stats = get_stats_by_job_and_country(
            db, stats_data.standard_job_id, stats_data.country_code
        )
        if existing_stats:
            logger.debug("Updating existing stats: id=%s, min_days=%.2f, avg_days=%.2f, max_days=%.2f, count=%d", 
                         existing_stats.id, existing_stats.min_days_to_hire, existing_stats.avg_days_to_hire, 
                         existing_stats.max_days_to_hire, existing_stats.count_of_job_postings)
            existing_stats.min_days_to_hire = stats_data.min_days_to_hire
            existing_stats.max_days_to_hire = stats_data.max_days_to_hire
            existing_stats.avg_days_to_hire = stats_data.avg_days_to_hire

            db.commit()
            db.refresh(existing_stats)
            logger.info("Updated stats for standard_job_id=%s, country_code=%s", 
                        stats_data.standard_job_id, stats_data.country_code)
            return existing_stats
        else:
            logger.debug("Inserting new stats: min_days=%.2f, avg_days=%.2f, max_days=%.2f, count=%d", 
                         stats_data.min_days_to_hire, stats_data.avg_days_to_hire, 
                         stats_data.max_days_to_hire, stats_data.count_of_job_postings)
            db_stats = DaysToHireStats(**stats_data.dict())
            db.add(db_stats)
            db.commit()
            db.refresh(db_stats)
            logger.info("Inserted new stats for standard_job_id=%s, country_code=%s", 
                        stats_data.standard_job_id, stats_data.country_code)
            return db_stats
    except SQLAlchemyError as e:
        db.rollback()
        logger.error("Error upserting stats for standard_job_id=%s, country_code=%s: %s\n%s", 
                     stats_data.standard_job_id, stats_data.country_code, str(e), traceback.format_exc())
        raise RuntimeError(f"Error upserting stats: {e}") from e
    

def save_stats(
        db: Session,
        standard_job_id: str,
        stats: Tuple[float, float, float, int],
        country_code: Optional[str] = None,
) -> None:
    logger.info("Saving stats for standard_job_id=%s, country_code=%s", standard_job_id, country_code)
    min_days, avg_days, max_days, count_of_job_postings = stats
    stats_data = DaysToHireStatsCreate(
        id=str(uuid.uuid4()),
        standard_job_id=standard_job_id,
        country_code=country_code,
        min_days_to_hire=min_days,
        avg_days_to_hire=avg_days,
        max_days_to_hire=max_days,
        count_of_job_postings=count_of_job_postings,
    )
    logger.debug("Prepared stats_data: min_days=%.2f, avg_days=%.2f, max_days=%.2f, count=%d", 
                 min_days, avg_days, max_days, count_of_job_postings)
    upsert_stats(db, stats_data)


def calculate_and_save_stats_in_batches(batch_size: int = 1000) -> None:
    logger.info("Starting calculate_and_save_stats_in_batches with batch_size=%d, min_postings=%d", batch_size)
    with get_session() as db:
        standard_job_ids = get_distinct_standard_job_ids(db)
        logger.info("Processing %d standard_job_ids", len(standard_job_ids))
        for standard_job_id in standard_job_ids:
            logger.info("Processing standard_job_id=%s", standard_job_id)
            country_codes = get_distinct_coutry_codes(db, standard_job_id)
            logger.info("Found %d country_codes for standard_job_id=%s", len(country_codes), standard_job_id)
            for country_code in country_codes:
                logger.info("Processing country_code=%s for standard_job_id=%s", country_code, standard_job_id)
                country_days = []
                for batch in get_days_to_hire_for_sjid_country(db, standard_job_id, country_code, batch_size=batch_size):
                    logger.debug("Received batch of %d days_to_hire: %s", len(batch), batch[:5])
                    country_days.extend(batch)
                
                if country_days:
                    stats = calculate_stats(country_days)
                    if stats:
                        logger.debug("Calculated stats for country_code=%s: %s", country_code, stats)
                        save_stats(db, standard_job_id, stats, country_code)
                    else:
                        logger.warning("No valid stats calculated for country_code=%s", country_code)

            all_days = []
            for batch in get_days_to_hire_for_sjid_country(db, standard_job_id, None, batch_size=batch_size):
                logger.debug("Received batch of %d days_to_hire: %s", len(batch), batch[:5])
                all_days.extend(batch)
            if all_days:
                stats = calculate_stats(all_days)
                if stats:
                    logger.debug("Calculated global stats for standard_job_id=%s: %s", standard_job_id, stats)
                    save_stats(db, standard_job_id, stats, None)
                else:
                    logger.warning("No valid global stats calculated for standard_job_id=%s", standard_job_id)

