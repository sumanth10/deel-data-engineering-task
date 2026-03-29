import os
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from fastapi import FastAPI, Request, HTTPException

from kpi_queries import (
    OPEN_ORDERS_BY_DATE_STATUS,
    TOP3_DELIVERY_DATES,
    PENDING_ITEMS_BY_PRODUCT,
    TOP3_CUSTOMERS_PENDING,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    jdbc_url = os.environ["ANALYTICS_JDBC_URL"]
    dsn = jdbc_url.replace("jdbc:postgresql://", "postgresql://")

    app.state.pool = await asyncpg.create_pool(
        dsn=dsn,
        user=os.environ["ANALYTICS_DB_USER"],
        password=os.environ["ANALYTICS_DB_PASSWORD"],
        min_size=2,
        max_size=10,
    )
    yield
    await app.state.pool.close()


app = FastAPI(
    title="ACME Delivery Analytics API",
    description=(
        "Real-time KPI endpoints powered by Spark Structured Streaming + Delta Lake. "
        "All KPIs are served from analytics.open_orders, a pre-joined, pre-filtered "
        "table refreshed every 20 seconds by the Gold streaming layer."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

async def _run_query(request: Request, sql: str) -> list[dict[str, Any]]:
    try:
        async with request.app.state.pool.acquire() as conn:
            rows = await conn.fetch(sql)
            return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Endpoints

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get(
    "/kpis/open-orders",
    summary="Open orders by delivery date and status",
    description=(
        "Number of open orders (status != COMPLETED) grouped by delivery_date and status."
    ),
)
async def open_orders_by_date_status(request: Request):
    rows = await _run_query(request, OPEN_ORDERS_BY_DATE_STATUS)
    return {"data": rows, "count": len(rows)}


@app.get(
    "/kpis/top-delivery-dates",
    summary="Top 3 delivery dates by open order volume",
    description=(
        "The 3 delivery dates with the most open orders. "
    ),
)
async def top_delivery_dates(request: Request):
    rows = await _run_query(request, TOP3_DELIVERY_DATES)
    return {"data": rows, "count": len(rows)}


@app.get(
    "/kpis/pending-items-by-product",
    summary="Pending item count by product",
    description=(
        "Number of pending order items grouped by product_id. "
    ),
)
async def pending_items_by_product(request: Request):
    rows = await _run_query(request, PENDING_ITEMS_BY_PRODUCT)
    return {"data": rows, "count": len(rows)}


@app.get(
    "/kpis/top-customers-pending",
    summary="Top 3 customers by pending order count",
    description=(
        "The 3 customers with the most pending orders. "
    ),
)
async def top_customers_pending(request: Request):
    rows = await _run_query(request, TOP3_CUSTOMERS_PENDING)
    return {"data": rows, "count": len(rows)}