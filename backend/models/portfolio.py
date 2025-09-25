from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from decimal import Decimal
from datetime import datetime


class Holding(BaseModel):
    """Represents a single holding in a portfolio"""
    symbol: str
    description: str
    quantity: Decimal
    price: Decimal
    unit_cost: Decimal
    cost_basis: Decimal
    current_value: Decimal
    day_change_percent: Decimal
    day_change_dollars: Decimal
    unrealized_gain_loss: Decimal
    unrealized_gain_loss_percent: Decimal
    portfolio_percentage: Optional[Decimal] = None
    brokers: Dict[str, Decimal] = Field(default_factory=dict)


class Portfolio(BaseModel):
    """Aggregated portfolio data from all brokers"""
    holdings: List[Holding]
    total_value: Decimal
    total_cost_basis: Decimal
    total_unrealized_gain_loss: Decimal
    total_unrealized_gain_loss_percent: Decimal
    last_updated: datetime
    brokers_updated: List[str]  # Which brokers were successfully scraped


class CrawlerResult(BaseModel):
    """Result from a single broker crawler"""
    broker: str
    success: bool
    holdings: List[Holding] = []
    error_message: Optional[str] = None
    requires_2fa: bool = False
    session_valid: bool = True
