import csv
import random
import datetime
from faker import Faker
from tqdm import tqdm


'''
生成电商案例的大CSV文件进行ETL测试

'''

def generate_big_csv(filename, total_rows):
    fake = Faker('zh_CN')
    headers = ['order_id', 'user_id', 'amount', 'order_date', 'product_id']

    # 预生成基础数据集
    user_pool = [fake.random_int(1000, 9999) for _ in range(1000)]
    product_pool = [f"P-{fake.random_int(1000, 9999)}" for _ in range(500)]

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        # 分批生成避免内存溢出
        batch_size = 100_000
        batches = total_rows // batch_size

        for _ in tqdm(range(batches), desc="生成进度"):
            data = []
            for _ in range(batch_size):
                # 构造带异常值的记录
                row = [
                    fake.unique.random_number(digits=8),  # order_id
                    random.choice(user_pool + [None]),  # user_id（5%为空）
                    random.choice([
                        round(random.uniform(1, 150000), 2),  # 正常范围+超限值
                        "invalid",  # 错误格式
                        -abs(random.gauss(500, 300)),  # 负值
                        random.choice(["", "N/A"])  # 空值
                    ]),
                    fake.date_time_between(
                        start_date='-2y',
                        end_date='now'
                    ).strftime('%Y-%m-%d %H:%M:%S') if random.random() > 0.02 else "2023-13-32",  # 错误日期
                    random.choice(product_pool + [None])  # product_id
                ]
                data.append(row)

            writer.writerows(data)

    print(f"生成完成！文件路径：{filename}")


if __name__ == "__main__":
    generate_big_csv("big_csv_commerce.csv", total_rows=1000000)  # 生成百万行测试文件