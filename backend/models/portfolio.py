import json
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, model_validator
import pandas as pd


class Holding(BaseModel):
    """Represents a single holding in a portfolio"""
    symbol: str
    description: str
    quantity: float
    price: float
    unit_cost: Optional[float]
    cost_basis: Optional[float]
    current_value: float
    day_change_percent: float
    day_change_dollars: float
    # Prior-period closing price per unit. Stored as a raw input so the day
    # change is independently auditable: day_change_dollars should equal
    # (price - previous_close) * quantity. Auto-derived when not supplied.
    previous_close: Optional[float] = None
    unrealized_gain_loss: Optional[float]
    unrealized_gain_loss_percent: Optional[float]
    portfolio_percentage: Optional[float] = None
    brokers: Dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _derive_previous_close(self) -> "Holding":
        # current_value == price * quantity for every holding (incl. options),
        # so the prior unit price is (prior position value) / quantity.
        if self.previous_close is None and self.quantity not in (0, None):
            self.previous_close = (self.current_value - self.day_change_dollars) / self.quantity
        return self


def day_change_consistency_warning(
    holding: "Holding", rel_tol: float = 0.05, abs_tol: float = 0.001
) -> Optional[str]:
    """Cross-check a holding's reported day % against the % implied by its day $.

    Returns a human-readable warning string when they disagree, else None.

    This is only meaningful on a *raw* per-broker holding whose
    ``day_change_percent`` is sourced independently from ``day_change_dollars``
    (Merrill, E*Trade). It catches unit/quantity bugs such as a per-share dollar
    change stored as the position total — which is off by a factor of quantity
    and would otherwise pass unnoticed once the combine step re-derives the %.
    """
    dcd = holding.day_change_dollars
    reported = holding.day_change_percent
    if reported in (None, 0) or dcd in (None, 0):
        return None
    prior_value = holding.current_value - dcd
    if prior_value == 0:
        return None
    implied = dcd / prior_value
    if abs(implied - reported) <= abs_tol + rel_tol * abs(reported):
        return None
    return (
        f"{holding.symbol}: day-change inconsistency — reported "
        f"{reported * 100:.2f}% but ${dcd:,.2f} implies {implied * 100:.2f}% "
        f"(price={holding.price}, qty={holding.quantity}, value={holding.current_value:,.2f}). "
        f"Possible per-share vs total error."
    )


class Portfolio(BaseModel):
    """Aggregated portfolio data from all brokers"""
    holdings: List[Holding]
    total_value: float
    total_cost_basis: Optional[float]
    total_unrealized_gain_loss: Optional[float]
    total_unrealized_gain_loss_percent: Optional[float]
    last_updated: datetime
    day_change_percent: float
    day_change_dollars: float

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
