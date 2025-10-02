import contextlib
import datetime
import os
import traceback
from typing import Any, Dict, Generator, List, Literal

from loguru import logger
from sqlalchemy import (
    DateTime,
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


class EventVol(VolDeclBase):
    __tablename__ = "event_vols"

    id: Mapped[str] = mapped_column(primary_key=True)
    event_name: Mapped[str] = mapped_column(nullable=False)
    ticker: Mapped[str] = mapped_column(nullable=False)
    utc_dt: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    vol_before: Mapped[float] = mapped_column(nullable=False)
    vol_after: Mapped[float] = mapped_column(nullable=False)
    event_vol: Mapped[float] = mapped_column(nullable=False)
    update_dt: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class DailyRV(VolDeclBase):
    __tablename__ = "daily_rv"

    dt: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)  # day end time
    ticker: Mapped[str] = mapped_column(nullable=False)
    exchange: Mapped[str] = mapped_column(nullable=False)
    rv_raw: Mapped[float] = mapped_column(nullable=False)
    rv_er: Mapped[float] = mapped_column(nullable=False)
    er_duration: Mapped[int] = mapped_column(nullable=False)  # how many minutes after the event are removed
    event_id: Mapped[str] = mapped_column(nullable=False)  # the event removed
    update_dt: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (PrimaryKeyConstraint("dt", "ticker", "exchange"),)


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
            db_url = os.getenv("VOL_DB", "")
        if not db_url:
            raise RuntimeError("Need to configure os env VOL_DB")
        super().__init__(db_url=db_url, debug=debug)
        self._init_tables()
        self._post_init_tables()

    def _init_tables(self) -> None:
        # XXX should avoid use it in production!
        VolDeclBase.metadata.create_all(bind=self._engine, checkfirst=True)

    def _post_init_tables(self) -> None:
        pass

    def insert_event_vols(self, vol_records: list[tuple]):
        for event_vol_record in vol_records:
            id = event_vol_record[0]
            event_name = event_vol_record[1]
            ticker = event_vol_record[2]
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
                        ticker=ticker,
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
                    ticker=ticker,
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
                        data.ticker,
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
                        data.ticker,
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

    def insert_daily_rv(self, rv_data: tuple) -> bool:
        dt, ticker, exchange, rv_raw, rv_er, er_duration, event_id, update_dt = rv_data
        try:
            with self.get_session() as _session:
                cursor = (
                    _session.query(DailyRV)
                    .where(DailyRV.dt == dt)
                    .where(DailyRV.ticker == ticker)
                    .where(DailyRV.exchange == exchange)
                )
                if len(cursor.all()) == 0:
                    _stmt = insert(DailyRV).values(
                        dt=dt,
                        ticker=ticker,
                        exchange=exchange,
                        rv_raw=rv_raw,
                        rv_er=rv_er,
                        er_duration=er_duration,
                        event_id=event_id,
                        update_dt=update_dt,
                    )
                else:
                    _stmt = (
                        update(DailyRV)
                        .where(DailyRV.dt == dt)
                        .where(DailyRV.ticker == ticker)
                        .where(DailyRV.exchange == exchange)
                        .values(
                            rv_raw=rv_raw,
                            rv_er=rv_er,
                            er_duration=er_duration,
                            event_id=event_id,
                            update_dt=update_dt,
                        )
                    )
                _session.execute(_stmt)
        except Exception as e:
            logger.error("Fail to insert daily rv, reason={}".format(str(e)))
            if self._debug:
                logger.debug(traceback.format_exc())
            return False
        else:
            return True

    # def get_daily_rv(
    #     self,
    #     ticker: str,
    #     exchange: str,
    #     from_date: datetime.datetime,
    #     to_date: datetime.datetime,
    # ) -> list[tuple]:
    #     """return value ordered by date asc"""
    #     # remove the tzinfo because the dt field has no tzinfo, make sure the tz is in UTC!
    #     from_date = from_date.replace(tzinfo=None)
    #     to_date = to_date.replace(tzinfo=None)
    #     try:
    #         with self.get_session() as _session:
    #             cursor = (
    #                 _session.query(DailyRV)
    #                 .where(DailyRV.ticker == ticker)
    #                 .where(DailyRV.exchange == exchange)
    #                 .where(DailyRV.dt >= from_date)
    #                 .where(DailyRV.dt < to_date)
    #                 .order_by(DailyRV.dt.asc())
    #             )
    #             return [
    #                 (
    #                     data.dt,
    #                     data.ticker,
    #                     data.exchange,
    #                     data.rv_raw,
    #                     data.rv_er,
    #                     data.er_duration,
    #                     data.event_id,
    #                     data.update_dt,
    #                 )
    #                 for data in cursor.all()
    #             ]
    #     except Exception as e:
    #         logger.error(str(e))
    #         return []
