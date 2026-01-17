from src.com.vitthalmirji.etl.adapters.base import BaseAdapter
from src.com.vitthalmirji.etl.ITable import ITable
from src.com.vitthalmirji.utils.secrets import get_secret
from pyspark.sql import SparkSession

class JDBCAdapter(BaseAdapter):
    def read(self, source_config: dict) -> ITable:
        """
        从JDBC数据源读取数据
        Args:
            source_config: 包含JDBC连接参数的字典
                - url: JDBC连接URL
                - driver: JDBC驱动类名
                - dbtable: 要读取的表名或查询
                - user: 数据库用户名
                - password: 数据库密码或密钥名称
        """
        # 安全获取密码（如果是密钥名称）
        password = get_secret(source_config.get("password", ""))
        
        return SparkSession.getActiveSession().read \
            .format("jdbc") \
            .option("url", source_config["url"]) \
            .option("driver", source_config["driver"]) \
            .option("dbtable", source_config["dbtable"]) \
            .option("user", source_config["user"]) \
            .option("password", password) \
            .load()

    def write(self, data: ITable, destination_config: dict):
        """
        写入数据到JDBC目标
        Args:
            data: 要写入的ITable数据
            destination_config: 包含JDBC连接参数的字典
                - url: JDBC连接URL
                - driver: JDBC驱动类名
                - dbtable: 目标表名
                - user: 数据库用户名
                - password: 数据库密码或密钥名称
        """
        # 安全获取密码（如果是密钥名称）
        password = get_secret(destination_config.get("password", ""))
        
        data.write \
            .format("jdbc") \
            .option("url", destination_config["url"]) \
            .option("driver", destination_config["driver"]) \
            .option("dbtable", destination_config["dbtable"]) \
            .option("user", destination_config["user"]) \
            .option("password", password) \
            .save()
