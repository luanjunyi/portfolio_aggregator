from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Type

# Set up logging
log = logging.getLogger(__name__)

if __package__:
    from .crawlers.base_crawler import BaseCrawler
    from .crawlers.chase_crawler import ChaseCrawler
    from .crawlers.etrade_crawler import EtradeCrawler
    from .crawlers.merrill_crawler import MerrillCrawler
    from .models.portfolio import CrawlerResult, Holding, Portfolio
else:  # pragma: no cover - allows running as a script for quick tests
    from crawlers.base_crawler import BaseCrawler
    from crawlers.chase_crawler import ChaseCrawler
    from crawlers.etrade_crawler import EtradeCrawler
    from crawlers.merrill_crawler import MerrillCrawler
    from models.portfolio import CrawlerResult, Holding, Portfolio

CrawlerType = Type[BaseCrawler]
BROKER_CRAWLERS: Sequence[CrawlerType] = (
    MerrillCrawler,
    ChaseCrawler,
    EtradeCrawler,
)

# Each broker is retried this many times before the whole run is failed.
CRAWLER_MAX_ATTEMPTS = 3
CRAWLER_RETRY_BACKOFF_SECONDS = 10


def _float_sum(values: Iterable[float]) -> float:
    return sum(values, 0.0)


def _optional_float_sum(values: Iterable[Optional[float]]) -> Optional[float]:
    value_list = list(values)
    if any(value is None for value in value_list):
        return None
    return sum(value_list, 0.0)


def _merge_broker_maps(holdings: Iterable[Holding]) -> Dict[str, float]:
    merged: Dict[str, float] = {}
    for holding in holdings:
        broker_map = holding.brokers or {}
        for broker_name, value in broker_map.items():
            merged[broker_name] = merged.get(broker_name, 0.0) + value
    return merged


def _combine_symbol_group(symbol: str, holdings: Sequence[Holding]) -> Holding:
    if not holdings:
        raise ValueError(f"No holdings provided for symbol {symbol}")

    base = holdings[0]

    total_quantity = _float_sum(h.quantity for h in holdings)
    total_cost_basis = _optional_float_sum(h.cost_basis for h in holdings)
    total_current_value = _float_sum(h.current_value for h in holdings)
    total_day_change_dollars = _float_sum(h.day_change_dollars for h in holdings)
    total_unrealized_gain_loss = _optional_float_sum(h.unrealized_gain_loss for h in holdings)

    weighted_price = 0.0
    weighted_unit_cost = None
    if total_quantity != 0:
        weighted_price = total_current_value / total_quantity
        if total_cost_basis is not None:
            weighted_unit_cost = total_cost_basis / total_quantity

    day_change_percent = 0.0
    prior_value = total_current_value - total_day_change_dollars
    if prior_value != 0:
        day_change_percent = total_day_change_dollars / prior_value

    unrealized_gain_loss_percent = None
    if total_cost_basis not in (None, 0) and total_unrealized_gain_loss is not None:
        unrealized_gain_loss_percent = total_unrealized_gain_loss / total_cost_basis
    elif total_cost_basis == 0 and total_unrealized_gain_loss is not None:
        unrealized_gain_loss_percent = 0.0

    combined_brokers = _merge_broker_maps(holdings)

    return Holding(
        symbol=symbol,
        description=base.description,
        quantity=total_quantity,
        price=weighted_price,
        unit_cost=weighted_unit_cost,
        cost_basis=total_cost_basis,
        current_value=total_current_value,
        day_change_percent=day_change_percent,
        day_change_dollars=total_day_change_dollars,
        unrealized_gain_loss=total_unrealized_gain_loss,
        unrealized_gain_loss_percent=unrealized_gain_loss_percent,
        portfolio_percentage=None,
        brokers=combined_brokers,
    )


def _combine_successful_holdings(results: Sequence[CrawlerResult]) -> List[Holding]:
    grouped: Dict[str, List[Holding]] = {}
    for result in results:
        if not result.success:
            continue
        for holding in result.holdings:
            grouped.setdefault(holding.symbol, []).append(holding)

    combined: List[Holding] = []
    for symbol in sorted(grouped.keys()):
        combined.append(_combine_symbol_group(symbol, grouped[symbol]))
    return combined


def _assign_portfolio_percentages(holdings: List[Holding]) -> List[Holding]:
    total_value = _float_sum(h.current_value for h in holdings)
    if total_value == 0:
        return holdings

    updated: List[Holding] = []
    for holding in holdings:
        percentage = holding.current_value / total_value
        updated.append(holding.copy(update={"portfolio_percentage": percentage}))
    return updated


async def _run_crawler(crawler_cls: CrawlerType) -> CrawlerResult:
    crawler = crawler_cls()
    async with crawler:
        return await crawler.crawl()


async def _run_crawler_with_retries(
    crawler_cls: CrawlerType,
    attempts: int = CRAWLER_MAX_ATTEMPTS,
    backoff: float = CRAWLER_RETRY_BACKOFF_SECONDS,
) -> CrawlerResult:
    """Run a crawler, retrying transient failures. Each fresh attempt relaunches
    the browser. Logs the reason for every failed attempt (with traceback) so
    failures are debuggable. Raises after the final attempt — the caller then
    fails the whole run rather than saving partial data.
    """
    name = crawler_cls.__name__
    last_error: object = None
    for attempt in range(1, attempts + 1):
        try:
            result = await _run_crawler(crawler_cls)
            if result.success:
                if attempt > 1:
                    log.info(f"{name} succeeded on attempt {attempt}/{attempts}")
                return result
            last_error = result.error_message or "crawler returned success=False"
            log.warning(f"{name} attempt {attempt}/{attempts} failed: {last_error}")
        except Exception as exc:
            last_error = exc
            log.warning(
                f"{name} attempt {attempt}/{attempts} raised: {type(exc).__name__}: {exc}",
                exc_info=True,
            )
        if attempt < attempts:
            log.info(f"Retrying {name} in {backoff:.0f}s...")
            await asyncio.sleep(backoff)

    raise RuntimeError(
        f"{name} failed after {attempts} attempts; last error: {last_error}. "
        f"Manual retry/fix needed."
    )


async def fetch_all_positions() -> Portfolio:
    results: List[CrawlerResult] = []
    for crawler_cls in BROKER_CRAWLERS:
        # A crawler that fails all retries aborts the whole run: partial
        # snapshots are not useful, so we save all brokers or nothing.
        result = await _run_crawler_with_retries(crawler_cls)
        results.append(result)

    combined_holdings = _combine_successful_holdings(results)
    holdings_with_percentages = _assign_portfolio_percentages(combined_holdings)

    total_value = _float_sum(h.current_value for h in holdings_with_percentages)
    total_cost_basis = _optional_float_sum(h.cost_basis for h in holdings_with_percentages)
    total_unrealized = _optional_float_sum(h.unrealized_gain_loss for h in holdings_with_percentages)

    total_unrealized_percent = None
    if total_cost_basis not in (None, 0) and total_unrealized is not None:
        total_unrealized_percent = total_unrealized / total_cost_basis

    total_day_change_dollars = _float_sum(h.day_change_dollars for h in holdings_with_percentages)

    total_day_change_percent = 0.0
    prior_value = total_value - total_day_change_dollars
    if prior_value != 0:
        total_day_change_percent = total_day_change_dollars / prior_value

    portfolio = Portfolio(
        holdings=holdings_with_percentages,
        total_value=total_value,
        total_cost_basis=total_cost_basis,
        total_unrealized_gain_loss=total_unrealized,
        total_unrealized_gain_loss_percent=total_unrealized_percent,
        # Local time, not UTC: the snapshot date must match the local trading
        # day (run_daily_portfolio uses date.today()). With UTC, an evening run
        # in a behind-UTC timezone (e.g. PDT) rolls over to tomorrow's date.
        last_updated=datetime.now(),
        day_change_percent=total_day_change_percent,
        day_change_dollars=total_day_change_dollars,
    )

    for result in results:
        status = "SUCCESS" if result.success else "FAILED"
        log.info(f"[{result.broker}] {status} - holdings: {len(result.holdings)}")
        if result.error_message:
            log.error(f"    Error: {result.error_message}")    

    return portfolio


async def main() -> None:
    portfolio = await fetch_all_positions()

    log.info("\nCombined Holdings:")
    for holding in portfolio.holdings:
        broker_details = ", ".join(
            f"{broker}: ${value}" for broker, value in sorted(holding.brokers.items())
        ) or "(none)"
        log.info(
            f"- {holding.symbol}: qty={holding.quantity}, value=${holding.current_value}"
            f" ({broker_details})"
        )

    print("\nPortfolio Totals:")
    print(f"  Total Value: ${portfolio.total_value}")
    print(f"  Cost Basis: ${portfolio.total_cost_basis}")
    print(f"  Unrealized G/L: ${portfolio.total_unrealized_gain_loss}")
    print(f"  Unrealized %: {portfolio.total_unrealized_gain_loss_percent}")
    print(f"  Day Gain: ${portfolio.day_change_dollars:.2f} ({portfolio.day_change_percent * 100:.2f}%)")
    
    print(
        "  Unrealized %: "
        + (
            f"{(portfolio.total_unrealized_gain_loss_percent * 100):.2f}%"
            if portfolio.total_unrealized_gain_loss_percent is not None
            else "N/A"
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
