from dataclasses import dataclass
from typing import Union, Literal, final
from pathlib import Path


@final
@dataclass(frozen=True)
class S3Bucket:
    region: str


@final
@dataclass(frozen=True)
class GCSBucket:
    pass


@final
@dataclass(frozen=True)
class FileDestination:
    path: Path


CloudBucketType = Union[S3Bucket, GCSBucket]


@final
@dataclass(frozen=True)
class CloudBucketDestination:
    bucket_name: str
    cloud_bucket: CloudBucketType


ArrowDestination = Union[FileDestination, CloudBucketDestination]


@dataclass(frozen=True)
class ArrowOutputConfig:
    destination: ArrowDestination
    batch_size: int
    format: Literal["parquet", "csv"]
