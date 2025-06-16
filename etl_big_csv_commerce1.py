import pandas as pd
from sqlalchemy import create_engine
import logging
import time

# 配置日志记录
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class ETLProcessor:
    def __init__(self, config):
        self.config = config
        self.engine = create_engine(
            f"mysql+pymysql://{config['db_user']}:{config['db_password']}"
            f"@{config['db_host']}:{config['db_port']}/{config['db_name']}",
            pool_size=10,
            max_overflow=20
        )

    def _process_chunk(self, chunk):
        """执行数据转换逻辑（示例）"""
        # 1. 清理无效数据
        chunk = chunk.dropna(subset=['user_id', 'order_id'])

        # 2. 类型转换
        chunk['amount'] = pd.to_numeric(chunk['amount'], errors='coerce')
        chunk['order_date'] = pd.to_datetime(chunk['order_date'], errors='coerce')

        # 3. 计算衍生字段
        chunk['price_category'] = pd.cut(chunk['amount'],
                                         bins=[0, 50, 100, 500, float('inf')],
                                         labels=['micro', 'small', 'medium', 'large'])

        # 4. 过滤异常值
        chunk = chunk[(chunk['amount'] > 0) & (chunk['amount'] < 100000)]

        return chunk

    def _batch_insert(self, chunk, table_name):
        """批量写入数据库"""
        try:
            chunk.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='append',
                index=False,
                method='multi',  # 启用批量插入
                chunksize=500  # 每批插入500条
            )
            return True
        except Exception as e:
            logging.error(f"数据库写入失败: {str(e)}")
            # 失败数据写入回退文件
            chunk.to_csv('error_records.csv', mode='a', header=False)
            return False

    def run_etl(self):

        total_rows = 0

        reader=pd.read_csv(self.config['input_file']
                           , chunksize=10000
                           ,parse_dates=['order_date']  #自动解析日期
                           ,iterator=True)

        for  i,chunk in enumerate(reader):
                total_rows += len(chunk)
                print('第',i,'批','处理数据量为:',total_rows)
                print(chunk.head(5))








if __name__ == "__main__":
    config = {
        'input_file': './data/big_csv_commerce.csv',
        'db_host': '192.168.254.128',
        'db_port': '3306',
        'db_name': 'test_Etl',
        'db_user': 'root',
        'db_password': 'root',
        'table_name': 'big_csv_commerce'
    }

    etl = ETLProcessor(config)
    etl.run_etl()