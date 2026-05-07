import time

from loguru import logger


def log_time(func):
    """
    Decorator to log execution time of a function.
    """

    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed = end_time - start_time
        if elapsed < 60:
            logger.info(
                f"Function '{func.__name__}' took {elapsed:.4f} secs to complete."
            )
        else:
            minutes = int(elapsed // 60)
            seconds = elapsed % 60
            logger.info(
                f"Function '{func.__name__}' took {minutes} min {seconds:.1f} secs to complete."
            )
        return result

    return wrapper
