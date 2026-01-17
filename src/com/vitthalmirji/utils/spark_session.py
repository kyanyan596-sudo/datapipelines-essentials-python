import os
from pyspark.sql import SparkSession

class SparkSessionManager:
    _instance = None
    
    @classmethod
    def get_or_create(cls, config: dict = None) -> SparkSession:
        """获取或创建SparkSession实例（单例模式）"""
        if cls._instance is None:
            builder = SparkSession.builder.appName(config.get("appName", "ETLPipeline"))
            
            # 设置主节点URL
            if "master" in config:
                builder.master(config["master"])
                
            # 添加Hadoop AWS包（S3支持）
            builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
            
            # 应用自定义配置
            for key, value in config.items():
                if key not in ["master", "appName"]:
                    builder.config(key, value)
            
            # Windows系统需要额外设置
            if os.name == 'nt':
                os.environ['HADOOP_HOME'] = "C:/hadoop"  # 假设Hadoop安装在C:/hadoop
                builder.config("spark.hadoop.fs.s3a.access.key", config.get("aws_access_key", ""))
                builder.config("spark.hadoop.fs.s3a.secret.key", config.get("aws_secret_key", ""))
            
            cls._instance = builder.getOrCreate()
        return cls._instance
    
    @classmethod
    def getActiveSession(cls) -> SparkSession:
        """获取当前活动的SparkSession"""
        if cls._instance is None:
            raise RuntimeError("SparkSession not initialized. Call get_or_create() first.")
        return cls._instance
    
    @classmethod
    def stop(cls) -> None:
        """停止SparkSession"""
        if cls._instance:
            cls._instance.stop()
            cls._instance = None
