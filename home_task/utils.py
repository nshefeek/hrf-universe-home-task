import logging

from typing import List, Optional, Tuple

import numpy as np


logger = logging.getLogger(__name__)


def calculate_stats(days_to_hire: List[int]) -> Optional[Tuple[float, float, float, int]]:
    """
    Calculate the stats for days to hire as per the requirements: 10th percentile, average, 90th percentile, count.
    """
    logger.info("Calculating stats for days to hire with days to hire: %d, len(days_to_hire)")

    
    data = np.array(days_to_hire)
    logger.debug("Data with shape %d for stats calculation", data.shape)

    min_days = float(np.percentile(data, 10))
    max_days = float(np.percentile(data, 90))

    logger.debug("Calculated min_days: %f, max_days: %f", min_days, max_days)

    filtered_data = data[(data >= min_days) & (data <= max_days)]
    logger.debug("Filtered data with shape %d", filtered_data.shape)
    
    avg_days = float(np.mean(filtered_data))
    count_of_job_postings = len(filtered_data)

    logger.info("Calculated stats - min_days: %f, avg_days: %f, max_days: %f, count_of_job_postings: %d", min_days, avg_days, max_days, count_of_job_postings)

    return min_days, avg_days, max_days, count_of_job_postings
