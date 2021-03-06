"""
Schema representing a request for the query endpoint for Grafana SimpleJson.
"""
from typing import List, Dict

from pydantic.main import BaseModel


class Target(BaseModel):
    """
    Target object
    """
    refId: str
    target: str
    type: str


class AdhocFilter(BaseModel):
    """
      Adhoc filter object
    """
    key: str
    value: str
    operator: str


class QueryRequest(BaseModel):
    """
    Request object for query endpont
    """
    maxDataPoints: int
    adhocFilters: List[AdhocFilter]
    intervalMs: int
    targets: List[Target]
    range: Dict


class TagValuesRequest(BaseModel):
    """
    Request object for key-values
    """
    key: str
