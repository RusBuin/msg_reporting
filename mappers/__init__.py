# mappers/__init__.py
# ESG metric extractor package.
# Each module contains a company-specific extractor function.

from .audi_mapper import extract_audi_core
from .hmc_mapper import extract_hmc_core
from .iljin_mapper import extract_iljin_core
from .skoda_mapper import extract_skoda_core
from .sungwoo_mapper import extract_sungwoo_core

__all__ = [
    "extract_audi_core",
    "extract_hmc_core",
    "extract_iljin_core",
    "extract_skoda_core",
    "extract_sungwoo_core",
]
