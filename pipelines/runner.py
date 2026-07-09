"""Centralized pipeline runner helper.

Provides `run_refresh` and a pipeline configuration map so source definitions
and refresh execution are centralized.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from logging import getLogger
from typing import Callable

import dlt

from pipelines import RAW_SCHEMA, SECRET_STORE
from pipelines.common.utils import (
    validate_required_secrets,
    get_refresh_mode,
    get_write_disposition,
    log_refresh_mode,
)
from pipelines.sources.notion import notion_source
from pipelines.sources.hubspot import hubspot_source
from pipelines.sources.fitbit import fitbit_source, get_fitbit_token
from pipelines.sources.google_health import (
    google_health_source,
    get_google_health_token,
)

logger = getLogger(__name__)


@dataclass(frozen=True)
class PipelineConfig:
    source_factory: Callable
    pipeline_name: str
    display_name: str
    required_secret_keys: list[str] | None = None
    loader_file_format: str | None = "jsonl"
    progress: str | None = "log"
    token_getter: Callable | None = None
    source_kwargs: dict | None = None


PIPELINE_CONFIG: dict[str, PipelineConfig] = {
    "notion": PipelineConfig(
        source_factory=notion_source,
        pipeline_name="notion_habits_pipeline",
        display_name="Notion Habits",
        required_secret_keys=["sources.notion.api_key"],
        source_kwargs={"db_name": "Disciplines"},
    ),
    "hubspot": PipelineConfig(
        source_factory=hubspot_source,
        pipeline_name="hubspot_crm_pipeline",
        display_name="HubSpot CRM",
        required_secret_keys=["sources.hubspot.api_key"],
    ),
    "fitbit": PipelineConfig(
        source_factory=fitbit_source,
        pipeline_name="fitbit_health_pipeline",
        display_name="Fitbit Health",
        required_secret_keys=[
            "sources.fitbit.client_id",
            "sources.fitbit.client_secret",
            "sources.fitbit.refresh_token",
        ],
        token_getter=get_fitbit_token,
    ),
    "google_health": PipelineConfig(
        source_factory=google_health_source,
        pipeline_name="google_health_v4_pipeline",
        display_name="Google Health v4",
        required_secret_keys=["sources.google_health.access_token"],
        token_getter=get_google_health_token,
    ),
}


def run_refresh(
    *,
    source_factory: Callable,
    pipeline_name: str,
    display_name: str,
    required_secret_keys: list[str] | None = None,
    is_incremental: bool | None = None,
    pipeline: object | None = None,
    initial_date: str | None = None,
    end_date: str | None = None,
    select: list[str] | None = None,
    loader_file_format: str | None = None,
    progress: str | None = None,
    token_getter: Callable | None = None,
    source_kwargs: dict | None = None,
):
    if required_secret_keys:
        validate_required_secrets(
            secret_store=SECRET_STORE,
            required_secret_keys=required_secret_keys,
            pipeline_name=display_name,
        )

    if is_incremental is None:
        is_incremental = get_refresh_mode(default_incremental=True)

    log_refresh_mode(display_name, is_incremental, RAW_SCHEMA)

    sk = dict(source_kwargs or {})
    if initial_date is not None:
        sk.setdefault("initial_date", initial_date)
    if end_date is not None:
        sk.setdefault("end_date", end_date)

    if token_getter is not None:
        token = token_getter()
        if "api_key" in source_factory.__code__.co_varnames:
            sk.setdefault("api_key", token)
        elif "access_token" in source_factory.__code__.co_varnames:
            sk.setdefault("access_token", token)

    src = source_factory(**sk)
    if select:
        src = src.with_resources(*select)

    if not pipeline:
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            dataset_name=RAW_SCHEMA,
            destination="bigquery",
            progress=progress,
        )

    write_disposition = get_write_disposition(is_incremental)

    run_kwargs: dict = {"write_disposition": write_disposition}
    if loader_file_format:
        run_kwargs["loader_file_format"] = loader_file_format

    info = pipeline.run(src, **run_kwargs)
    logger.info(info)
    return info


def refresh_pipeline(
    pipeline_key: str,
    *,
    is_incremental: bool | None = None,
    pipeline: object | None = None,
    initial_date: str | None = None,
    end_date: str | None = None,
    select: list[str] | None = None,
):
    config = PIPELINE_CONFIG[pipeline_key]
    return run_refresh(
        source_factory=config.source_factory,
        pipeline_name=config.pipeline_name,
        display_name=config.display_name,
        required_secret_keys=config.required_secret_keys,
        is_incremental=is_incremental,
        pipeline=pipeline,
        initial_date=initial_date,
        end_date=end_date,
        select=select,
        loader_file_format=config.loader_file_format,
        progress=config.progress,
        token_getter=config.token_getter,
        source_kwargs=config.source_kwargs,
    )
