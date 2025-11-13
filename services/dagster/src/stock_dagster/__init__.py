from .definitions import defs as _defs
import logging
logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")

defs = _defs()
__all__ = ["defs"]