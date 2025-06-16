import pandas as pd
from sqlalchemy import create_engine

# 配置
CSV_FILE = './data/large_data.csv'
CHUNK_SIZE = 10000  # 每次读取1万行，可根据内存调整

MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root'
MYSQL_HOST = '192.168.254.128'
MYSQL_PORT = '3306'
MYSQL_DB = 'test_Etl'
MYSQL_TABLE = 'processed_large_data'

def transform_data(chunk_df):
    """
    对数据块进行转换或清洗
    """
    # 删除空行
    chunk_df = chunk_df.dropna()

    # 转换列名为小写
    chunk_df.columns = [col.strip().lower() for col in chunk_df.columns]

    # 示例：添加一列 price_with_tax
    if 'price' in chunk_df.columns:
        chunk_df['price_with_tax'] = chunk_df['price'] * 1.1

    return chunk_df

def load_to_mysql(df_chunk, engine, table_name):
    """
    将一个处理后的数据块写入MySQL
    """
    df_chunk.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',  # 追加模式
        index=False,
        method='multi'       # 批量插入
    )

def run_etl():
    engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}')

    chunk_count = 0
    for chunk in pd.read_csv(CSV_FILE, chunksize=CHUNK_SIZE):
        chunk_count += 1
        print(f"📦 正在处理第 {chunk_count} 块...")

        df_cleaned = transform_data(chunk)
        load_to_mysql(df_cleaned, engine, MYSQL_TABLE)

    print("✅ 所有数据块处理完毕！")

if __name__ == "__main__":
    run_etl()
