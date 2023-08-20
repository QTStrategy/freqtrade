import logging
from freqtrade.persistence.base import ModelBase, SessionType
from sqlalchemy import (Enum, Float, ForeignKey, Integer, ScalarResult, Select, String,BigInteger,
                                UniqueConstraint, desc, func, select, Text)
from sqlalchemy.orm import Mapped, lazyload, mapped_column, relationship, validates
from typing import Any, ClassVar, Dict, List, Optional, Sequence, cast
from typing import NewType
from typing import Literal
from freqtrade.exceptions import OperationalException

from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy_utils import database_exists, create_database

import sqlalchemy
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import DateTime

Frame = Literal['5m', '30m', '1h', '1d', '1w']

logger = logging.getLogger(__name__)

from collections import namedtuple
Ohlcv = namedtuple('Ohlcv', ['date', 'open', 'high', 'low', 'close', 'volume'])

def GenerateTableName(table:str, frame:Frame):
    chg = table.replace('/','_')
    return f'{chg}_{frame}'

def OhlcvClassCreate(table:str, frame: Frame):
    tableName = GenerateTableName(table, frame)
    uniq_name = f'_{tableName}_uniq'
    class DynamicModel(ModelBase):
        __tablename__ = tableName
        __table_args__ = (UniqueConstraint('date', name=uniq_name),)
        session: ClassVar[SessionType]
        date: Mapped[int] = mapped_column(BigInteger(), primary_key=True, autoincrement=False, nullable=False)
        open: Mapped[float] = mapped_column(Float(), nullable=False)
        high: Mapped[float] = mapped_column(Float(), nullable=False)
        low: Mapped[float] = mapped_column(Float(), nullable=False)
        close: Mapped[float] = mapped_column(Float(), nullable=False)
        volume: Mapped[float] = mapped_column(Float(), nullable=False)

        @staticmethod
        def lookup_ohlcv(start_time: int, end_time: int, column = None):
            if column is None:
                query = select(DynamicModel).filter(DynamicModel.date >= start_time, DynamicModel.date <= end_time)
            else:
                selected_columns = [getattr(DynamicModel, col) for col in column]
                query = select(*selected_columns).filter(DynamicModel.date >= start_time, DynamicModel.date <= end_time)
            return DynamicModel.session.execute(query).scalars().all()
        
        @staticmethod
        def load(column = None):
            if column is None:
                query = select(DynamicModel)
            else:
                selected_columns = [getattr(DynamicModel, col) for col in column]
                query = select(*selected_columns)
            return DynamicModel.session.execute(query).scalars().all()
        
        @staticmethod
        def toList(data: List[OhlcvClassCreate]):
            return [Ohlcv(row.date, row.open, row.high, row.low, row.close, row.volume) for row in data]
        
        @staticmethod
        def store_ohlcv(date, open_price, high_price, low_price, close_price, volume):
            new_ohlcv = DynamicModel(date=date, open=open_price, high=high_price, low=low_price, close=close_price, volume=volume)
            DynamicModel.session.add(new_ohlcv)
            DynamicModel.session.commit()

        @staticmethod
        def store_batch(data):
            insert_ohlcv_list = [DynamicModel(**row) for row in data.to_dict(orient='records')]
            DynamicModel.session.add_all(insert_ohlcv_list)
            DynamicModel.session.commit()

        @staticmethod
        def update_batch(data):
            insert_ohlcv_list = [DynamicModel(**row) for row in data.to_dict(orient='records')]
            for entry in insert_ohlcv_list:
                DynamicModel.session.merge(entry)
            DynamicModel.session.commit()

    return DynamicModel

def OhlcvConnect(dynamicModle, url):
    try:
        engine = sqlalchemy.create_engine(url)
        if not database_exists(engine.url): create_database(engine.url)
    except NoSuchModuleError:
        raise OperationalException(f"Given value for db_url: '{url}' ")
    dynamicModle.session = scoped_session(sessionmaker(bind=engine))
    ModelBase.metadata.create_all(engine)