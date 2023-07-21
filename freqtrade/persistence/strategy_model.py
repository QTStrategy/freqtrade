import logging
from freqtrade.persistence.base import ModelBase, SessionType
from sqlalchemy import (Enum, Float, ForeignKey, Integer, ScalarResult, Select, String,
                                UniqueConstraint, desc, func, select, Text)
from sqlalchemy.orm import Mapped, lazyload, mapped_column, relationship, validates
from typing import Any, ClassVar, Dict, List, Optional, Sequence, cast

logger = logging.getLogger(__name__)

class StrategyStr(ModelBase):
    __tablename__ = 'strategy'
    url = ''
    session: ClassVar[SessionType]
    __table_args__ = (UniqueConstraint('strategy', name="_strategy_uniq"),)
    strategy: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    code: Mapped[str] = mapped_column(Text(), nullable=False)

    @staticmethod
    def get_strategy(strategy_name: str):
        return StrategyStr.session.scalars(select(StrategyStr).filter(StrategyStr.strategy == strategy_name)).first()

    @staticmethod
    def list_strategy():
        return StrategyStr.session.scalars(select(StrategyStr).filter()).all()