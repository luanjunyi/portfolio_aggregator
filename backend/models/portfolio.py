import json
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field
import pandas as pd


class Holding(BaseModel):
    """Represents a single holding in a portfolio"""
    symbol: str
    description: str
    quantity: float
    price: float
    unit_cost: float
    cost_basis: float
    current_value: float
    day_change_percent: float
    day_change_dollars: float
    unrealized_gain_loss: float
    unrealized_gain_loss_percent: float
    portfolio_percentage: Optional[float] = None
    brokers: Dict[str, float] = Field(default_factory=dict)


class Portfolio(BaseModel):
    """Aggregated portfolio data from all brokers"""
    holdings: List[Holding]
    total_value: float
    total_cost_basis: float
    total_unrealized_gain_loss: float
    total_unrealized_gain_loss_percent: float
    last_updated: datetime
    brokers_updated: List[str]  # Which brokers were successfully scraped

    def to_dataframe(self) -> pd.DataFrame:
        rows: List[Dict[str, object]] = []
        for holding in self.holdings:
            row = holding.model_dump()
            brokers_mapping = row.get("brokers", {}) or {}
            row["brokers"] = json.dumps(brokers_mapping, sort_keys=True)
            rows.append(row)

        return pd.DataFrame(rows)


class CrawlerResult(BaseModel):
    """Result from a single broker crawler"""
    broker: str
    success: bool
    holdings: List[Holding] = []
    error_message: Optional[str] = None
    requires_2fa: bool = False
    session_valid: bool = True
