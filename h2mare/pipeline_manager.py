"""
PipelineManager class to orchestrate the download and processing of datasets based on the provided configuration and registry
"""

from pathlib import Path
from typing import List, Optional, Type, Union

import pandas as pd
from loguru import logger

from h2mare import AppConfig, settings
from h2mare.format_converters.netcdf2zarr import Netcdf2Zarr


class PipelineManager:
    def __init__(
        self,
        app_config: AppConfig,
        registry: dict[str, Type],
        store_root: Union[str, Path],
        dry_run: bool = False,
        start_date: Union[pd.Timestamp, None] = None,
        end_date: Union[pd.Timestamp, None] = None,
        no_convert: bool = False,
        no_compile: bool = False,
    ):

        self.app_config = app_config
        self.registry = registry
        self.store_root = Path(store_root)
        self.dry_run = dry_run
        self.start_date = start_date
        self.end_date = end_date
        self.no_convert = no_convert
        self.no_compile = no_compile

    def run(self, variables: Optional[List[str] | None] = None):
        if variables is None:
            variables = list(self.app_config.variables.keys())

        for var_key in variables:
            if var_key in ["h2ds", "bathy", "moon"]:
                continue

            var_config = self.app_config.variables.get(var_key)
            if not var_config:
                logger.warning(f"⚠️ Variable '{var_key}' not found in config. Skipping.")
                continue

            DownloaderClass = self.registry.get(var_config.source)
            if not DownloaderClass:
                logger.error(f"❌ Downloader {var_config.source} not found.")
                continue

            downloader = DownloaderClass(
                var_key=var_key,
                app_config=self.app_config,
                store_root=self.store_root,
            )

            try:
                downloaded = downloader.run(
                    dry_run=self.dry_run,
                    start_date=self.start_date,
                    end_date=self.end_date,
                )
            except Exception as e:
                logger.error(f"Download failed for '{var_key}': {e}")
                continue

            if self.no_convert or self.dry_run or not downloaded:
                continue

            try:
                Netcdf2Zarr(var_key).run()
            except Exception as e:
                logger.error(f"Processing failed for '{var_key}': {e}")

        if not self.no_compile and not self.no_convert and not self.dry_run:
            from h2mare.processing.compiler import Compiler

            logger.info("Starting compile step (h2ds)")
            try:
                Compiler(remote_store_root=self.store_root).run(
                    start_date=self.start_date,
                    end_date=self.end_date,
                    var_keys=variables,
                )
            except Exception as e:
                logger.error(f"Compile step failed: {e}")

        self._cleanup_empty_download_dirs(variables)

    def _cleanup_empty_download_dirs(self, variables: List[str]) -> None:
        """Remove per-variable download subdirectories that are empty after the pipeline run."""
        downloads_root = settings.DOWNLOADS_DIR
        for var_key in variables:
            var_config = self.app_config.variables.get(var_key)
            if var_config is None:
                continue
            folder = downloads_root / var_config.local_folder
            if folder.exists() and not any(folder.iterdir()):
                folder.rmdir()
                logger.debug(f"Removed empty download directory: {folder}")
