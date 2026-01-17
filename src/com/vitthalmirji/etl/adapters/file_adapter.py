from typing import Dict
from pyspark.sql import DataFrame
from src.com.vitthalmirji.etl.ITable import ITable
from src.com.vitthalmirji.etl.adapters.base import BaseAdapter

class FileAdapter(BaseAdapter):
    def read(self, source_config: Dict) -> ITable:
        """读取文件系统数据源"""
        file_type = source_config.get("format", "parquet")
        path = source_config["path"]
        
        spark = SparkSession.getActiveSession()
        if file_type == "csv":
            return spark.read.csv(path, header=True, inferSchema=True)
        elif file_type == "json":
            return spark.read.json(path)
        elif file_type == "parquet":
            return spark.read.parquet(path)
        else:
            raise ValueError(f"Unsupported file format: {file_type}")

    def write(self, data: ITable, destination_config: Dict) -> None:
        """写入文件系统"""
        file_type = destination_config.get("format", "parquet")
        path = destination_config["path"]
        mode = destination_config.get("mode", "overwrite")
        
        if file_type == "csv":
            data.write.csv(path, mode=mode, header=True)
        elif file_type == "json":
            data.write.json(path, mode=mode)
        elif file_type == "parquet":
            data.write.parquet(path, mode=mode)
        else:
            raise ValueError(f"Unsupported file format: {file_type}")
