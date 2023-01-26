from dataclasses import dataclass
from typing import Union, Literal
from pathlib import Path


@dataclass(frozen=True)
class S3Destination:
    uri: str
    region: str


@dataclass(frozen=True)
class GCSDestination:
    uri: str


@dataclass(frozen=True)
class FileDestination:
    path: Path


ArrowDestination = Union[S3Destination, GCSDestination, FileDestination]


@dataclass(frozen=True)
class ArrowOutputConfig:
    destination: ArrowDestination
    batch_size: int
    format: Literal["parquet", "csv"]
