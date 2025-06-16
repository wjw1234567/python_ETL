import pandas as pd
import numpy as np

# 行数
NUM_ROWS = 500000

# 生成数据
data = {
    'id': np.arange(1, NUM_ROWS + 1),
    'name': np.random.choice(['Apple', 'Banana', 'Cherry', 'Durian', 'Mango', 'Grape'], NUM_ROWS),
    'price': np.round(np.random.uniform(1, 100, size=NUM_ROWS), 2)  # 价格区间：1~100，保留2位小数
}

# 创建DataFrame
df = pd.DataFrame(data)

# 保存为CSV
df.to_csv('large_data.csv', index=False)

print("✅ 生成完成：large_data.csv (",NUM_ROWS,"}万行)")
