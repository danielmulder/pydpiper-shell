import logging
import sys
from tqdm import tqdm

class LogWithTqdm(logging.Handler):
    """
    A custom logging handler that redirects logging output to `tqdm.write()`,
    ensuring that log messages do not interfere with the progress bar display.
    """
    def emit(self, record):
        try:
            msg = self.format(record)
            # Write to stderr for consistency with tqdm's default stream.
            tqdm.write(msg, file=sys.stderr)
            self.flush()
        except Exception:
            self.handleError(record)


def configure_logger(general_level='INFO', module_specific_levels=None, silenced_loggers=None):
    """
    Configures the root logger and specific module loggers with a
    TQDM-friendly handler.
    """
    # 1. Create a TQDM-friendly handler and a standard formatter.
    # 👇 Use the new, clearer class name.
    tqdm_aware_handler = LogWithTqdm()
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s"
    )
    tqdm_aware_handler.setFormatter(formatter)

    # 2. Configure the root logger.
    root_logger = logging.getLogger()
    log_level = getattr(logging, general_level.upper(), logging.INFO) if isinstance(general_level, str) else general_level
    root_logger.setLevel(log_level)

    # 3. Clear any existing handlers and add the new one.
    root_logger.handlers.clear()
    root_logger.addHandler(tqdm_aware_handler)

    # 4. Configure levels for specific modules.
    if module_specific_levels:
        for name, level in module_specific_levels.items():
            level_to_set = getattr(logging, level.upper(), logging.INFO) if isinstance(level, str) else level
            logging.getLogger(name).setLevel(level_to_set)

    # 5. Muzzle noisy loggers by setting their level high.
    if silenced_loggers:
        for name, level in silenced_loggers.items():
            level_to_set = getattr(logging, level.upper(), logging.CRITICAL) if isinstance(level, str) else level
            logging.getLogger(name).setLevel(level_to_set)

