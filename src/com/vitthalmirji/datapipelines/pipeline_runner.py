import yaml
from pyspark.sql import SparkSession
from com.vitthalmirji.utils.spark_session import get_spark_session
from com.vitthalmirji.utils.logging_util import setup_logger

def load_config(config_path: str) -> dict:
    """加载YAML配置文件"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def main():
    # 初始化日志和Spark会话
    logger = setup_logger("pipeline_runner")
    spark = get_spark_session(app_name="ETL-Pipeline-Runner")
    
    try:
        # 1. 加载数据源配置
        sources_config = load_config("src/config/sources.yaml")
        for source in sources_config['sources']:
            df = spark.read \
                .format(source['format']) \
                .options(**source.get('options', {})) \
                .load(source['path'])
            df.createOrReplaceTempView(source['name'])
            logger.info(f"Created temp view: {source['name']}")
        
        # 2. 加载转换管道配置
        pipeline_config = load_config("src/config/pipeline.yaml")
        
        # 3. 执行转换步骤
        for step in pipeline_config['steps']:
            if 'sql' in step:
                spark.sql(step['sql'])
                logger.info(f"Executed SQL: {step['sql'][:50]}...")
            elif 'dataframe_ops' in step:
                # 预留DataFrame操作接口
                pass
        
        # 4. 输出最终结果
        output_config = pipeline_config['output']
        result_df = spark.sql(output_config['source'])
        
        if output_config['type'] == 's3':
            result_df.write \
                .format(output_config['format']) \
                .mode(output_config.get('mode', 'overwrite')) \
                .save(output_config['path'])
        elif output_config['type'] == 'hive':
            result_df.write \
                .format("hive") \
                .mode(output_config.get('mode', 'overwrite')) \
                .saveAsTable(output_config['table_name'])
        
        logger.info("Pipeline execution completed successfully")
    
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
