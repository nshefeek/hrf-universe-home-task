from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, Query

from .db import get_session
from .schemas import DaysToHireStatsResponse
from .services import get_stats_by_job_and_country


app = FastAPI()


@app.get("/stats/", response_model=DaysToHireStatsResponse)
def get_stats(
    standard_job_id: str = Query(..., description="Standard job ID"),
    country_code: Optional[str] = Query(None, description="Country code"),
    session=Depends(get_session),
):
    """
    Get statistics for a given standard job ID and optional country code.
    """
    stats = get_stats_by_job_and_country(session, standard_job_id, country_code)
    if not stats:
        raise HTTPException(status_code=404, detail="Statistics not found")
    return stats