import cProfile
import gc
import io
import pstats
import time
from functools import wraps

from loguru import logger


def timeit(fun):
    @wraps(fun)
    def wrap_timeit(*args, **kwargs):
        start = time.time()
        fun_out = fun(*args, **kwargs)
        end = time.time()
        logger.info(f"Elapsed Time in {fun.__name__}: {end - start}s")
        return fun_out

    return wrap_timeit


def profileit(fun):
    @wraps(fun)
    def wrap_profileit(*args, **kwargs):
        logger.info(f"Profiling {fun.__name__}")
        profiler = cProfile.Profile()
        profiler.enable()
        out = fun(*args, **kwargs)
        profiler.disable()
        stream = io.StringIO()
        process_stats = pstats.Stats(profiler, stream=stream).sort_stats(
            pstats.SortKey.CUMULATIVE
        )
        process_stats.print_stats()
        print(stream.getvalue())
        return out

    return wrap_profileit


def clearit(fun):
    @wraps(fun)
    def wrap_clearit(*args, **kwargs):
        logger.info(f"Memory will be deleted for{fun.__name__} in future!")
        gc.collect()
        out = fun(*args, **kwargs)
        return out

    return wrap_clearit


@timeit
def measure_time(expr: str, context: dict = None):
    if context is None:
        context = {}
    return eval(expr, context)
