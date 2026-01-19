import os
import shutil
import subprocess
import tempfile
import asyncio
from pathlib import Path, PurePosixPath
from typing import Optional

import aiohttp
from tqdm import tqdm
from bdt_common.constants import BINANCE_AWS_DATA_PREFIX
from bdt_common.log_kit import divider, logger


async def aiohttp_download_files(download_infos: list[tuple[str, Path]], http_proxy: Optional[str] = None) -> int:
    """
    Download files from AWS S3 using aiohttp library with parallel downloads.

    Args:
        download_infos: List of tuples containing (aws_url, local_file_path) pairs
        http_proxy: HTTP proxy URL string, or None for no proxy

    Returns:
        int: Number of failed downloads (0 for all success)
    """
    failed_count = 0
    # Limit concurrent connections to 20
    connector = aiohttp.TCPConnector(limit=20)
    timeout = aiohttp.ClientTimeout(total=300, connect=60, sock_connect=60, sock_read=60)
    
    # Semaphore to limit parallel downloads to 20
    semaphore = asyncio.Semaphore(20)
    
    async def download_file(aws_url: str, local_file: Path) -> bool:
        """Download a single file and return True if successful, False otherwise."""
        nonlocal failed_count
        
        # Ensure parent directory exists
        local_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            async with semaphore:
                async with session.get(aws_url, proxy=http_proxy) as response:
                    response.raise_for_status()
                    with open(local_file, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            f.write(chunk)
            return True
        except Exception as e:
            logger.error(f"Failed to download {aws_url} to {local_file}: {e}")
            failed_count += 1
            return False
        finally:
            # Update progress bar regardless of success or failure
            pbar_total.update(1)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Progress bar for all files
        with tqdm(total=len(download_infos), desc="Total Downloads", unit="file") as pbar_total:
            # Create download tasks
            tasks = [download_file(aws_url, local_file) for aws_url, local_file in download_infos]
            # Wait for all tasks to complete
            await asyncio.gather(*tasks)
    
    return failed_count


def get_aria2c_exec() -> str:
    # Check if aria2c exists in the PATH
    aria2c_path = shutil.which("aria2c")
    if not aria2c_path:
        raise FileNotFoundError(f"aria2c executable not found in system PATH: {os.getenv('PATH')}")
    return aria2c_path


def aria2_download_files(download_infos: list[tuple[str, Path]], http_proxy: Optional[str] = None) -> int:
    """
    Download files from AWS S3 using aria2c command-line tool.

    Args:
        download_infos: List of tuples containing (aws_url, local_file_path) pairs
        http_proxy: HTTP proxy URL string, or None for no proxy

    Returns:
        int: Exit code from aria2c process (0 for success, non-zero for failure)
    """
    # Create temporary file containing download URLs and directory mappings for aria2c
    with tempfile.NamedTemporaryFile(mode="w", delete=False, prefix="bhds_") as aria_file:
        # Write each download URL and its target directory to the temp file
        for aws_url, local_file in download_infos:
            aria_file.write(f"{aws_url}\n  dir={local_file.parent}\n")
        aria_file.close()

        # Build aria2c command with optimized settings for parallel downloads
        aria2c_path = get_aria2c_exec()
        cmd = [aria2c_path, "-i", aria_file.name, "-j32", "-x4", "-q"]

        # Add proxy configuration if provided
        if http_proxy is not None:
            cmd.append(f"--https-proxy={http_proxy}")

        # Execute aria2c download process
        run_result = subprocess.run(cmd, env={})
        returncode = run_result.returncode
    return returncode


def find_missings(download_infos: list[tuple[str, Path]]) -> list[tuple[str, Path]]:
    """
    Identify missing files that need to be downloaded from AWS S3.

    Args:
        download_infos: List of tuples containing (aws_url, local_file_path) pairs

    Returns:
        list[tuple[str, Path]]: Filtered list containing only files that don't exist locally
    """
    # Initialize empty list to collect missing file entries
    download_infos_missing: list[tuple[str, Path]] = []

    # Check each file to see if it exists locally
    for aws_url, local_file in download_infos:
        # Only include files that haven't been downloaded yet
        if not local_file.exists():
            download_infos_missing.append((aws_url, local_file))
    return download_infos_missing


class AwsDownloader:
    """
    AWS S3 file downloader for Binance data with retry and batching capabilities.
    """

    def __init__(self, local_dir: Path, http_proxy: str = None, verbose: bool = True):
        """
        Initialize the AWS downloader with configuration parameters.

        Args:
            local_dir: Local directory path where files will be downloaded
            http_proxy: HTTP proxy URL string for downloads, or None for direct connection
            verbose: Enable verbose logging for download progress and status
        """
        self.local_dir = local_dir
        self.http_proxy = http_proxy
        self.verbose = verbose

    async def aws_download(self, aws_files: list[PurePosixPath], max_tries=3):
        """
        Download multiple files from AWS S3 with retry logic and batch processing.

        Args:
            aws_files: List of PurePosixPath objects representing AWS S3 file paths
            max_tries: Maximum number of retry attempts for failed downloads (default: 3)
        """
        # Build list of download information (URL, local path) for all files
        download_infos = []
        for aws_file in aws_files:
            # Construct local file path by combining local_dir with AWS file path
            local_file = self.local_dir / aws_file
            # Construct full AWS URL using the predefined prefix
            aws_url = f"{BINANCE_AWS_DATA_PREFIX}/{str(aws_file)}"
            download_infos.append((aws_url, local_file))

        # Retry loop for handling failed downloads
        for try_id in range(max_tries):
            # Find which files are still missing (need to be downloaded)
            missing_infos = find_missings(download_infos)

            # Exit if all files have been successfully downloaded
            if not missing_infos:
                break

            # Log retry attempt information if verbose mode is enabled
            if self.verbose:
                divider(f"Aria2 Download, try_id={try_id}, {len(missing_infos)} files", sep="-")

            # Process downloads in batches to avoid overwhelming the system
            batch_size = 4096
            for i in range(0, len(missing_infos), batch_size):
                # Extract current batch of files to download
                batch_infos = missing_infos[i : i + batch_size]
                batch_idx = i // batch_size + 1

                # Log batch information if verbose mode is enabled
                if self.verbose:
                    logger.info(
                        f"Download Batch{batch_idx}, num_files={len(batch_infos)}, "
                        f"{batch_infos[0][1].name} -- {batch_infos[-1][1].name}"
                    )

                # Try to use aria2c first, fall back to aiohttp if aria2c is not available
                try:
                    # Execute download for current batch using aria2c
                    import asyncio
                    returncode = await asyncio.to_thread(aria2_download_files, batch_infos, self.http_proxy)

                    # Log batch completion status if verbose mode is enabled
                    if self.verbose:
                        if returncode == 0:
                            logger.ok(f"Batch{batch_idx}, Aria2 download successfully")
                        else:
                            logger.error(f"Batch{batch_idx}, Aria2 exited with code {returncode}")
                except FileNotFoundError:
                    # Fall back to aiohttp download if aria2c is not available
                    if self.verbose:
                        logger.info(f"Batch{batch_idx}, Aria2 not available, using aiohttp for download")
                    
                    failed_count = await aiohttp_download_files(batch_infos, self.http_proxy)
                    
                    # Log batch completion status if verbose mode is enabled
                    if self.verbose:
                        if failed_count == 0:
                            logger.ok(f"Batch{batch_idx}, aiohttp download successfully")
                        else:
                            logger.error(f"Batch{batch_idx}, aiohttp download failed for {failed_count} files")
