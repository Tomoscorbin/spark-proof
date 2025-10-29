import pyspark.sql.types as T
from spark_proof.core.metadata import DomainMetadata, TypeName
from spark_proof.core.constraints import RangeConstraint
from spark_proof.core.strategies import (
    FloatStrategies,
    IntegerStrategies,
    DateStrategies,
    TimestampStrategies,
)
from spark_proof.domain.range import RangeField
from spark_proof.core.limits import (
    FLOAT32_MAX,
    FLOAT32_MIN,
    FLOAT64_MAX,
    FLOAT64_MIN,
    INT16_MAX,
    INT16_MIN,
    INT32_MAX,
    INT32_MIN,
    INT64_MAX,
    INT64_MIN,
    DATE_MAX,
    DATE_MIN,
    TIMESTAMP_MAX,
    TIMESTAMP_MIN,
)
import datetime


# TODO: rename this file

# Short
_short_meta = DomainMetadata[int](
    name=TypeName.SHORT,
    spark_type=T.ShortType(),
    python_type=int,
)
_short_constraint = RangeConstraint[int](
    domain_min=INT16_MIN,
    domain_max=INT16_MAX,
)
_short_strategies = IntegerStrategies()

short = RangeField(
    meta=_short_meta,
    constraint=_short_constraint,
    strategies=_short_strategies,
)

# Integer
_integer_meta = DomainMetadata[int](
    name=TypeName.INTEGER,
    spark_type=T.IntegerType(),
    python_type=int,
)
_integer_constraint = RangeConstraint[int](
    domain_min=INT32_MIN,
    domain_max=INT32_MAX,
)
_integer_strategies = IntegerStrategies()

integer = RangeField(
    meta=_integer_meta,
    constraint=_integer_constraint,
    strategies=_integer_strategies,
)


# Long
_long_meta = DomainMetadata[int](
    name=TypeName.LONG,
    spark_type=T.LongType(),
    python_type=int,
)
_long_constraint = RangeConstraint[int](
    domain_min=INT64_MIN,
    domain_max=INT64_MAX,
)
_long_strategies = IntegerStrategies()

long = RangeField(
    meta=_long_meta,
    constraint=_long_constraint,
    strategies=_long_strategies,
)


# Float
_float_meta = DomainMetadata[float](
    name=TypeName.FLOAT32,
    spark_type=T.FloatType(),
    python_type=float,
)
_float_constraint = RangeConstraint[float](
    domain_min=FLOAT32_MIN,
    domain_max=FLOAT32_MAX,
)
_float_strategies = FloatStrategies(width=32)

float32 = RangeField(
    meta=_float_meta,
    constraint=_float_constraint,
    strategies=_float_strategies,
)

# Double
_double_meta = DomainMetadata[float](
    name=TypeName.DOUBLE,
    spark_type=T.DoubleType(),
    python_type=float,
)
_double_constraint = RangeConstraint[float](
    domain_min=FLOAT64_MIN,
    domain_max=FLOAT64_MAX,
)
_double_strategies = FloatStrategies(width=64)

double = RangeField(
    meta=_double_meta,
    constraint=_double_constraint,
    strategies=_double_strategies,
)


# Date
_date_meta = DomainMetadata[datetime.date](
    name=TypeName.DATE,
    spark_type=T.DateType(),
    python_type=datetime.date,
)
_date_constraint = RangeConstraint[datetime.date](
    domain_min=DATE_MIN,
    domain_max=DATE_MAX,
)
_date_strategies = DateStrategies()

date_field = RangeField[datetime.date](
    meta=_date_meta,
    constraint=_date_constraint,
    strategies=_date_strategies,
)

# Timestamp
_timestamp_meta = DomainMetadata[datetime.datetime](
    name=TypeName.TIMESTAMP,
    spark_type=T.TimestampType(),
    python_type=datetime.datetime,
)
_timestamp_constraint = RangeConstraint[datetime.datetime](
    domain_min=TIMESTAMP_MIN,
    domain_max=TIMESTAMP_MAX,
)
_timestamp_strategies = TimestampStrategies()

timestamp = RangeField[datetime.datetime](
    meta=_timestamp_meta,
    constraint=_timestamp_constraint,
    strategies=_timestamp_strategies,
)
