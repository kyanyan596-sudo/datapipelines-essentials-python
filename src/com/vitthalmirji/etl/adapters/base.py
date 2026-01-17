from abc import ABC, abstractmethod
from src.com.vitthalmirji.etl.ITable import ITable

class BaseAdapter(ABC):
    @abstractmethod
    def read(self, source_config: dict) -> ITable:
        """从数据源读取数据"""
        pass
    
    @abstractmethod
    def write(self, data: ITable, destination_config: dict):
        """写入数据到目标"""
        pass
