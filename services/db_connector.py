import contextlib
import datetime
import traceback
from typing import Any, Dict, Generator, List, Literal

from loguru import logger
from sqlalchemy import (
    Index,
    PrimaryKeyConstraint,
    UniqueConstraint,
    create_engine,
    delete,
    insert,
    inspect,
    select,
    update,
)
from sqlalchemy.exc import IntegrityError, InternalError
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Query,
    Session,
    attributes,
    mapped_column,
    relationship,
    sessionmaker,
)


class VolDeclBase(DeclarativeBase):
    pass


class Event(VolDeclBase):
    __tablename__ = "economic_events"

    event_name: Mapped[str]
    date: Mapped[str]
    time_et: Mapped[str]

    __table_args__ = (PrimaryKeyConstraint("event_name", "date", "time_et"),)


class EventVol(VolDeclBase):
    __tablename__ = "event_vols"

    id: Mapped[str] = mapped_column(primary_key=True)
    event_name: Mapped[str]
    symbol: Mapped[str]
    utc_dt: Mapped[datetime.datetime]
    vol_before: Mapped[float]
    vol_after: Mapped[float]
    event_vol: Mapped[float]
    update_dt: Mapped[datetime.datetime]


class KLine(VolDeclBase):
    __tablename__ = "kline"

    symbol: Mapped[str]
    interval: Mapped[str]
    """Values: 1m / 5m / 60m / 1d"""
    timestamp: Mapped[int]
    """Unit: seconds in UTC"""
    exchange: Mapped[str]
    open: Mapped[float]
    high: Mapped[float]
    low: Mapped[float]
    close: Mapped[float]
    volume: Mapped[float]

    __table_args__ = (
        PrimaryKeyConstraint("symbol", "interval", "timestamp"),
        # 为 timestamp 单独加索引以优化时间范围查询
        Index("ix_klines_timestamp", "timestamp"),
    )


class DbConnector:
    def __init__(self, db_url: str, debug: bool = False) -> None:
        self._db_url = db_url
        self._debug = debug
        if self._debug:
            logger.debug(f"{self.__class__.__name__}: {self._db_url}")
        self._engine = create_engine(self._db_url, echo=self._debug)
        self._Session = sessionmaker(self._engine)

    @contextlib.contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        s = self._Session()
        try:
            yield s
            s.commit()
        except Exception as e:
            s.rollback()
            raise e
        finally:
            s.close()


class VolDbConnector(DbConnector):
    def __init__(self, db_url: str = "", debug: bool = False) -> None:
        if not db_url:
            db_url = "postgresql+psycopg2://fbg:fbg@localhost:5432/fbg_db"
        super().__init__(db_url=db_url, debug=debug)
        self._init_tables()
        self._post_init_tables()

    def _init_tables(self) -> None:
        VolDeclBase.metadata.create_all(bind=self._engine, checkfirst=True)

    def _post_init_tables(self) -> None:
        pass

    def insert_events(self, event_records: list[tuple]) -> bool:
        _stmt_l = [
            insert(Event).values(
                {
                    "event_name": event_record[0],
                    "date": event_record[1],
                    "time_et": event_record[2],
                }
            )
            for event_record in event_records
        ]
        try:
            for _stmt in _stmt_l:
                with self.get_session() as s:
                    try:
                        s.execute(_stmt)
                    except (IntegrityError, InternalError) as e:
                        continue
        except Exception as e:
            logger.error("Fail to insert event records, reason={}".format(str(e)))
            if self._debug:
                logger.debug(traceback.format_exc())
            return False
        else:
            return True

    def get_events(self) -> list[tuple]:
        try:
            with self.get_session() as _session:
                cursor = _session.query(Event)
                return [
                    (
                        data.event_name,
                        data.date,
                        data.time_et,
                    )
                    for data in cursor.all()
                ]
        except Exception as e:
            logger.error(str(e))
            return []

    def insert_event_vols(self, vol_records: list[tuple]):
        for event_vol_record in vol_records:
            id = event_vol_record[0]
            event_name = event_vol_record[1]
            symbol = event_vol_record[2]
            utc_dt = event_vol_record[3]
            vol_before = event_vol_record[4]
            vol_after = event_vol_record[5]
            event_vol = event_vol_record[6]
            update_dt = event_vol_record[7]
            _existing_records = self.get_event_vol_by_id(id=id)
            if len(_existing_records) != 0:
                _stmt = (
                    update(EventVol)
                    .where(EventVol.id == id)
                    .values(
                        event_name=event_name,
                        symbol=symbol,
                        utc_dt=utc_dt,
                        vol_before=vol_before,
                        vol_after=vol_after,
                        event_vol=event_vol,
                        update_dt=update_dt,
                    )
                )
            else:
                _stmt = insert(EventVol).values(
                    id=id,
                    event_name=event_name,
                    symbol=symbol,
                    utc_dt=utc_dt,
                    vol_before=vol_before,
                    vol_after=vol_after,
                    event_vol=event_vol,
                    update_dt=update_dt,
                )

            try:
                with self.get_session() as s:
                    s.execute(_stmt)
            except Exception as e:
                logger.error("Fail to insert/update event vol records, reason={}".format(str(e)))
                if self._debug:
                    logger.debug(traceback.format_exc())

    def get_event_vol_by_id(self, id: str) -> list[tuple]:
        try:
            with self.get_session() as _session:
                cursor = _session.query(EventVol).where(EventVol.id == id)
                return [
                    (
                        data.id,
                        data.event_name,
                        data.utc_dt,
                        data.vol_before,
                        data.vol_after,
                        data.event_vol,
                    )
                    for data in cursor.all()
                ]
        except Exception as e:
            logger.error(str(e))
            return []

    def get_event_vols(self) -> list[tuple]:
        try:
            with self.get_session() as _session:
                cursor = _session.query(EventVol)
                return [
                    (
                        data.id,
                        data.event_name,
                        data.utc_dt,
                        data.vol_before,
                        data.vol_after,
                        data.event_vol,
                    )
                    for data in cursor.all()
                ]
        except Exception as e:
            logger.error(str(e))
            return []

    def insert_klines(self, kline_data: list[tuple]) -> int:
        _stmt_l = [
            insert(KLine).values(
                dict(
                    zip(
                        [
                            "symbol",
                            "interval",
                            "timestamp",
                            "exchange",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                        ],
                        kline,
                    )
                )
            )
            for kline in kline_data
        ]
        try:
            for _stmt in _stmt_l:
                with self.get_session() as s:
                    try:
                        s.execute(_stmt)
                    except (InternalError, IntegrityError) as e:
                        continue
        except Exception as e:
            logger.error("Fail to insert kline, reason={}".format(str(e)))
            if self._debug:
                logger.debug(traceback.format_exc())
            return False
        else:
            return True

    def get_klines(
        self,
        symbol: str,
        interval: str,
        from_timestamp: int,  # Unit: UTC seconds
        to_timestamp: int,  # Unit: UTC seconds
        exchange: str,
    ) -> list[tuple]:
        try:
            with self.get_session() as _session:
                cursor = (
                    _session.query(KLine)
                    .where(KLine.symbol == symbol)
                    .where(KLine.interval == interval)
                    .where(KLine.exchange == exchange)
                    .where(KLine.timestamp >= from_timestamp)
                    .where(KLine.timestamp < to_timestamp)
                )
                return [
                    (
                        data.symbol,
                        data.interval,
                        data.timestamp,
                        data.exchange,
                        data.open,
                        data.high,
                        data.low,
                        data.close,
                        data.volume,
                    )
                    for data in cursor.all()
                ]
        except Exception as e:
            logger.error(str(e))
            return []
