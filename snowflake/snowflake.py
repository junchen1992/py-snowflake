from dataclasses import dataclass
from datetime import datetime, timedelta, tzinfo
from time import time
from typing import Optional

__all__ = ('SnowflakeID', 'SnowflakeGenerator')

# Start timestamp
START_TS = int(datetime(2023, 1, 1, 0, 0, 0).timestamp() * 1000)

# 41 bits timestamp
MAX_TS = 0b11111111111111111111111111111111111111111

# 10 bits instance ID
MAX_INSTANCE = 0b1111111111

# 12 bits sequence number
MAX_SEQ = 0b111111111111

# Instance ID bits
INSTANCE_BITS = 10

# Sequence number bits
SEQ_BITS = 12


@dataclass(frozen=True)
class SnowflakeID:
    timestamp: int
    instance: int
    seq: int = 0

    def __post_init__(self):
        if self.timestamp < 0 or self.timestamp > MAX_TS:
            raise ValueError(f"timestamp must not be negative and must be less than {MAX_TS}!")

        if self.instance < 0 or self.instance > MAX_INSTANCE:
            raise ValueError(f"instance must not be negative and must be less than {MAX_INSTANCE}!")

        if self.seq < 0 or self.seq > MAX_SEQ:
            raise ValueError(f"seq must not be negative and must be less than {MAX_SEQ}!")

    @classmethod
    def parse(cls, snowflake: int) -> 'SnowflakeID':
        return cls(
            timestamp=snowflake >> 22,
            instance=snowflake >> 12 & MAX_INSTANCE,
            seq=snowflake & MAX_SEQ
        )

    @property
    def milliseconds(self) -> int:
        return self.timestamp + START_TS

    @property
    def seconds(self) -> float:
        return self.milliseconds / 1000

    @property
    def datetime(self) -> datetime:
        return datetime.utcfromtimestamp(self.seconds)

    def datetime_tz(self, tz: Optional[tzinfo] = None) -> datetime:
        return datetime.fromtimestamp(self.seconds, tz=tz)

    @property
    def timedelta(self) -> timedelta:
        return timedelta(milliseconds=START_TS)

    @property
    def value(self) -> int:
        return self.timestamp << 22 | self.instance << 12 | self.seq

    def __int__(self) -> int:
        return self.value


class SnowflakeGenerator:
    def __init__(self, instance: int = 0, seq: int = 0,
                 timestamp: Optional[int] = None):

        current = int(time() * 1000)
        if current - START_TS >= MAX_TS:
            raise OverflowError(f"The maximum current timestamp has been reached in selected epoch,"
                                f"so Snowflake cannot generate more IDs!")

        timestamp = timestamp or current
        if timestamp < 0 or timestamp > current:
            raise ValueError(f"timestamp must not be negative and must be less than {current}!")

        self._ts = timestamp - START_TS

        if instance < 0 or instance > MAX_INSTANCE:
            raise ValueError(f"instance must not be negative and must be less than {MAX_INSTANCE}!")

        if seq < 0 or seq > MAX_SEQ:
            raise ValueError(f"seq must not be negative and must be less than {MAX_SEQ}!")

        self._instance = instance << 12
        self._seq = seq

    @classmethod
    def from_snowflake(cls, sf: SnowflakeID) -> 'SnowflakeGenerator':
        return cls(sf.instance, seq=sf.seq, timestamp=sf.timestamp)

    def __iter__(self):
        return self

    def __next__(self) -> Optional[int]:
        current = int(time() * 1000) - START_TS

        if current >= MAX_TS:
            raise OverflowError(f"The maximum current timestamp has been reached in selected epoch,"
                                f"so Snowflake cannot generate more IDs!")

        if self._ts == current:
            if self._seq == MAX_SEQ:
                return None
            self._seq += 1
        elif self._ts > current:
            return None
        else:
            self._seq = 0

        self._ts = current

        return self._ts << 22 | self._instance | self._seq
