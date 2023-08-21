import logging
from typing import Optional
from typing import Tuple

import numpy as np
from pandas import DataFrame, read_json, to_datetime

from freqtrade import misc
from freqtrade.configuration import TimeRange
from freqtrade.constants import DEFAULT_DATAFRAME_COLUMNS, TradeList
from freqtrade.data.converter import trades_dict_to_list
from freqtrade.enums import CandleType

from .idatahandler import IDataHandler
from freqtrade.persistence.ohlcv_model import OhlcvClassCreate, OhlcvConnect, GenerateTableName
from pathlib import Path

logger = logging.getLogger(__name__)


class MysqlDataHandler(IDataHandler):

    _use_zip = False
    _columns = DEFAULT_DATAFRAME_COLUMNS
    
    def __init__(self, datadir: Path, **Extra):
        super().__init__(datadir, **Extra)
        self.Classes = {}

    def ohlcv_store(
            self, pair: str, timeframe: str, data: DataFrame, candle_type: CandleType) -> None:
        """
        Store data in json format "values".
            format looks as follows:
            [[<date>,<open>,<high>,<low>,<close>]]
        :param pair: Pair - used to generate filename
        :param timeframe: Timeframe - used to generate filename
        :param data: Dataframe containing OHLCV data
        :param candle_type: Any of the enum CandleType (must match trading mode!)
        :return: None
        """
        if 'db_history' not in self._extra or self._extra['db_history'] in ["", None]:
            logger.error(f"Could not load data because no db_history.")
            return DataFrame(columns=self._columns)
        genName = GenerateTableName(pair, timeframe)
        if genName not in self.Classes:
            self.Classes[genName] = OhlcvClassCreate(pair, timeframe)
            OhlcvConnect(self.Classes[genName], self._extra['db_history'])

        dClass = self.Classes[genName]
        _data = data.copy()
        _data['date'] = _data['date'].view(np.int64) // 1000 // 1000
        dClass.update_batch(_data)

    def _ohlcv_load(self, pair: str, timeframe: str,
                    timerange: Optional[TimeRange], candle_type: CandleType
                    ) -> DataFrame:
        """
        Internal method used to load data for one pair from disk.
        Implements the loading and conversion to a Pandas dataframe.
        Timerange trimming and dataframe validation happens outside of this method.
        :param pair: Pair to load data
        :param timeframe: Timeframe (e.g. "5m")
        :param timerange: Limit data to be loaded to this timerange.
                        Optionally implemented by subclasses to avoid loading
                        all data where possible.
        :param candle_type: Any of the enum CandleType (must match trading mode!)
        :return: DataFrame with ohlcv data, or empty DataFrame
        """
        if 'db_history' not in self._extra or self._extra['db_history'] in ["", None]:
            logger.error(f"Could not load data because no db_history.")
            return DataFrame(columns=self._columns)
        genName = GenerateTableName(pair, timeframe)
        if genName not in self.Classes:
            self.Classes[genName] = OhlcvClassCreate(pair, timeframe)
            OhlcvConnect(self.Classes[genName], self._extra['db_history'])

        dClass = self.Classes[genName]
        if timerange is None:
            DownloadData = dClass.load()
        else:
            DownloadData = dClass.lookup_ohlcv(timerange.startts * 1000, timerange.stopts * 1000)

        if DownloadData == None or len(DownloadData) == 0:
            return DataFrame(columns=self._columns)
        else:
            listData = dClass.toList(DownloadData)
            pairdata = DataFrame(listData)

            pairdata['date'] = to_datetime(pairdata['date'], unit='ms', utc=True)
            return pairdata
        
    def ohlcv_append(
        self,
        pair: str,
        timeframe: str,
        data: DataFrame,
        candle_type: CandleType
    ) -> None:
        """
        Append data to existing data structures
        :param pair: Pair
        :param timeframe: Timeframe this ohlcv data is for
        :param data: Data to append.
        :param candle_type: Any of the enum CandleType (must match trading mode!)
        """
        raise NotImplementedError()

    def trades_store(self, pair: str, data: TradeList) -> None:
        """
        Store trades data (list of Dicts) to file
        :param pair: Pair - used for filename
        :param data: List of Lists containing trade data,
                     column sequence as in DEFAULT_TRADES_COLUMNS
        """
        raise NotImplementedError()

    def trades_append(self, pair: str, data: TradeList):
        """
        Append data to existing files
        :param pair: Pair - used for filename
        :param data: List of Lists containing trade data,
                     column sequence as in DEFAULT_TRADES_COLUMNS
        """
        raise NotImplementedError()

    def _trades_load(self, pair: str, timerange: Optional[TimeRange] = None) -> TradeList:
        """
        Load a pair from file, either .json.gz or .json
        # TODO: respect timerange ...
        :param pair: Load trades for this pair
        :param timerange: Timerange to load trades for - currently not implemented
        :return: List of trades
        """
        raise NotImplementedError()

    @classmethod
    def _get_file_extension(cls):
        return "json.gz" if cls._use_zip else "json"
    
    def query_download_data(self, pair:str , timeframe: str, timerange: Optional[TimeRange], candle_type: CandleType):
        start = None
        end = None
        if timerange and timerange.starttype == 'date' and timerange.stoptype == 'date':
                start = int(timerange.startdt.timestamp() * 1000)
                end = int(timerange.stopdt.timestamp() * 1000)
        else:
            return (False, None, None, None, None)
        
        if 'db_history' not in self._extra or self._extra['db_history'] in ["", None]:
            logger.error(f"Could not load data because no db_history.")
            return (False, None, None, None, None)
        genName = GenerateTableName(pair, timeframe)
        if genName not in self.Classes:
            self.Classes[genName] = OhlcvClassCreate(pair, timeframe)
            OhlcvConnect(self.Classes[genName], self._extra['db_history'])

        dClass = self.Classes[genName]        
        dateList = sorted(dClass.load(['date']))
        if dateList is None or len(dateList) < 2:
            return (True, start, end, None, None)

        s = dateList[0]
        e = dateList[-1]

        #padding, without hole
        if end < s:
            end = s
        if start > e:
            start = e
        if start >= end:
            return (False, None, None, None, None)
        
        #whether is overlap
        if start >= s and end <= e:
            return (False, None, None, None, None)
        elif start < s and end > e:
            return (True, start, s, e, end)
        else:
            if start < s:
                return (True, start, s, None, None)
            else:
                return (True, e, end, None, None)

        

        
        
