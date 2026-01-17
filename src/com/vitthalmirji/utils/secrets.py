"""
安全密钥管理模块
提供统一的密钥访问接口，支持：
1. 环境变量获取
2. AWS Secrets Manager集成
3. 本地开发环境模拟
"""
import os
import boto3
from botocore.exceptions import ClientError
from typing import Union

class SecretManager:
    def __init__(self, use_aws: bool = False):
        """
        初始化密钥管理器
        
        :param use_aws: 是否使用AWS Secrets Manager
        """
        self.use_aws = use_aws
        if use_aws:
            self.client = boto3.client('secretsmanager')

    def get_secret(self, secret_name: str, env_fallback: str = None) -> Union[str, None]:
        """
        获取密钥值，优先从AWS Secrets Manager获取，失败时回退到环境变量
        
        :param secret_name: 密钥名称（AWS或环境变量名）
        :param env_fallback: 备选环境变量名（可选）
        :return: 密钥值或None
        """
        # 本地开发环境优先使用环境变量
        if not self.use_aws or os.getenv('ENV') == 'local':
            return os.getenv(secret_name) or (os.getenv(env_fallback) if env_fallback else None)
        
        try:
            response = self.client.get_secret_value(SecretId=secret_name)
            return response['SecretString']
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                print(f"Secret {secret_name} not found in AWS Secrets Manager")
            elif error_code == 'AccessDeniedException':
                print(f"Access denied to secret {secret_name}")
            return os.getenv(secret_name) if env_fallback is None else os.getenv(env_fallback)

# 单例模式实现
secret_manager = SecretManager(use_aws=os.getenv('USE_AWS_SECRETS', 'false').lower() == 'true')

if __name__ == '__main__':
    # 测试示例
    db_password = secret_manager.get_secret(
        secret_name="prod/db/password",
        env_fallback="DB_PASSWORD_DEV"
    )
    print(f"Database Password: {'*****' if db_password else 'Not Found'}")
