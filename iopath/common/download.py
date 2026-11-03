# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.


import logging
import os
import shutil
from collections.abc import Callable
from urllib import request
from urllib.parse import urlsplit


def download(
    url: str, dir: str, *, filename: str | None = None, progress: bool = True
) -> str:
    """
    Download a file from a given URL to a directory. If file exists, will not
        overwrite the existing file.

    Args:
        url (str):
        dir (str): the directory to download the file
        filename (str or None): the basename to save the file.
            Will use the name in the URL if not given.
        progress (bool): whether to use tqdm to draw a progress bar.

    Returns:
        str: the path to the downloaded file or the existing one.
    """
    os.makedirs(dir, exist_ok=True)
    if filename is None:
        filename = urlsplit(url).path.rsplit("/", 1)[-1]
        assert len(filename), f"Cannot obtain filename from url {url}"
    fpath = os.path.join(dir, filename)
    logger = logging.getLogger(__name__)

    if os.path.isfile(fpath):
        logger.info(f"File {filename} exists! Skipping download.")
        return fpath

    tmp = fpath + ".tmp"  # download to a tmp file first, to be more atomic.
    try:
        logger.info(f"Downloading from {url} ...")
        if progress:
            import tqdm

            def hook(t: tqdm.tqdm) -> Callable[[int, int, int | None], None]:
                last_b: list[int] = [0]

                def inner(b: int, bsize: int, tsize: int | None = None) -> None:
                    if tsize is not None:
                        t.total = tsize
                    t.update((b - last_b[0]) * bsize)  # type: ignore
                    last_b[0] = b

                return inner

            with tqdm.tqdm(
                unit="B", unit_scale=True, miniters=1, desc=filename, leave=True
            ) as t:
                tmp, _ = request.urlretrieve(url, filename=tmp, reporthook=hook(t))

        else:
            tmp, _ = request.urlretrieve(url, filename=tmp)
        statinfo = os.stat(tmp)
        size = statinfo.st_size
        if size == 0:
            raise OSError(f"Downloaded an empty file from {url}!")
        # download to tmp first and move to fpath, to make this function more
        # atomic.
        shutil.move(tmp, fpath)
    except OSError:
        logger.error(f"Failed to download {url}")
        raise
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass

    logger.info("Successfully downloaded " + fpath + ". " + str(size) + " bytes.")
    return fpath
