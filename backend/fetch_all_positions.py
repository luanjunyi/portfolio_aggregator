from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Dict, Iterable, List, Sequence, Tuple, Type

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


def _float_sum(values: Iterable[float]) -> float:
    return sum(values, 0.0)


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
    total_cost_basis = _float_sum(h.cost_basis for h in holdings)
    total_current_value = _float_sum(h.current_value for h in holdings)
    total_day_change_dollars = _float_sum(h.day_change_dollars for h in holdings)
    total_unrealized_gain_loss = _float_sum(h.unrealized_gain_loss for h in holdings)

    weighted_price = 0.0
    weighted_unit_cost = 0.0
    if total_quantity != 0:
        weighted_price = total_current_value / total_quantity
        weighted_unit_cost = total_cost_basis / total_quantity

    day_change_percent = 0.0
    prior_value = total_current_value - total_day_change_dollars
    if prior_value != 0:
        day_change_percent = total_day_change_dollars / prior_value

    unrealized_gain_loss_percent = 0.0
    if total_cost_basis != 0:
        unrealized_gain_loss_percent = total_unrealized_gain_loss / total_cost_basis

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


async def fetch_all_positions() -> Portfolio:
    results: List[CrawlerResult] = []
    for crawler_cls in BROKER_CRAWLERS:
        try:
            result = await _run_crawler(crawler_cls)
            results.append(result)
        except Exception as exc:
            log.fatal(f"Error running crawler {crawler_cls}: {exc}")
            
    combined_holdings = _combine_successful_holdings(results)
    holdings_with_percentages = _assign_portfolio_percentages(combined_holdings)

    total_value = _float_sum(h.current_value for h in holdings_with_percentages)
    total_cost_basis = _float_sum(h.cost_basis for h in holdings_with_percentages)
    total_unrealized = _float_sum(h.unrealized_gain_loss for h in holdings_with_percentages)

    total_unrealized_percent = 0.0
    if total_cost_basis != 0:
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
        last_updated=datetime.utcnow(),
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
