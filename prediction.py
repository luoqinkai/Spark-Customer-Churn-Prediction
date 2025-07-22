# --- 0. 环境自检与自动安装 ---
# 在执行任何操作前，先检查所有必需的库是否已安装。
import sys
import subprocess
import importlib


def check_and_install_libraries(required_libraries):
    """
    检查指定的Python库是否已安装，如果未安装则尝试使用pip自动安装。

    参数:
    required_libraries (dict): 一个字典，键是pip安装时的包名，值是import时使用的模块名。

    返回:
    bool: 如果所有库都已就绪，返回True，否则在尝试安装后返回False并退出程序。
    """
    missing_packages = []
    for package_name, import_name in required_libraries.items():
        try:
            importlib.import_module(import_name)
            print(f"[OK] 库 '{import_name}' 已安装。")
        except ImportError:
            print(f"[!] 库 '{import_name}' 未找到，准备安装 '{package_name}'...")
            missing_packages.append(package_name)

    if missing_packages:
        print("\n正在尝试自动安装缺失的库...")
        try:
            # 使用 sys.executable 确保pip命令在当前Python环境中执行
            subprocess.check_call([sys.executable, '-m', 'pip', 'install'] + missing_packages)
            print("\n所有缺失的库已成功安装！")
            print("请重新运行此脚本以加载新安装的库。")
        except subprocess.CalledProcessError:
            print("\n错误：自动安装失败。")
            print("请在你的终端中手动运行以下命令:")
            print(f"pip install {' '.join(missing_packages)}")

        # 退出程序，让用户重新运行以加载新库
        sys.exit(1)

    print("\n所有依赖库均已满足，开始执行主程序...\n")
    return True


# 定义项目需要的库 (pip包名 -> import名)
REQUIRED_LIBRARIES = {
    'pyspark': 'pyspark',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'scikit-learn': 'sklearn'
}

# 执行检查
check_and_install_libraries(REQUIRED_LIBRARIES)

# --- 1. 导入所有必要的库 ---
# Spark相关的库，用于分布式计算
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Imputer
from pyspark.ml.classification import LogisticRegression

# 数据处理和数值计算库
import pandas as pd
import numpy as np

# 模型评估相关的库
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score

# --- 2. 初始化Spark Session ---
# SparkSession是所有Spark操作的入口点
spark = SparkSession.builder \
    .appName("TelcoChurnPrediction") \
    .getOrCreate()

# --- 3. 数据模拟与加载 ---
# 为了项目的可复现性，我们在这里模拟生成一个结构与真实Telco Churn数据集类似的DataFrame。
# 在实际项目中，这一步通常是 spark.read.csv() 或 spark.read.parquet()。
n_customers = 7043
np.random.seed(42)  # 使用固定的随机种子保证每次生成的数据一致

# 模拟生成分类特征
contract = np.random.choice(['Month-to-month', 'One year', 'Two year'], size=n_customers, p=[0.55, 0.21, 0.24])
payment_method = np.random.choice(
    ['Electronic check', 'Mailed check', 'Bank transfer (automatic)', 'Credit card (automatic)'], n_customers)

# 模拟生成数值特征
tenure = np.random.randint(1, 73, n_customers)
monthly_charges = np.random.normal(64.76, 30.09, n_customers).clip(18.25, 118.75)
total_charges = (tenure * monthly_charges + np.random.normal(0, 200, n_customers)).clip(18.8)

# 模拟生成目标变量 (Churn)，并使其与部分特征相关
churn_prob = 1 / (1 + np.exp(-(-2.0 + (contract == 'Month-to-month') * 1.0 - tenure * 0.03 + monthly_charges * 0.01)))
churn_label = np.random.binomial(1, churn_prob, n_customers)
churn = np.array(['Yes' if c == 1 else 'No' for c in churn_label])

# 模拟生成缺失值
total_charges[np.random.choice(n_customers, 11, replace=False)] = np.nan

# 使用Pandas创建本地DataFrame，然后转换为Spark DataFrame
pd_df = pd.DataFrame({
    'Contract': contract, 'PaymentMethod': payment_method,
    'tenure': tenure, 'MonthlyCharges': monthly_charges, 'TotalCharges': total_charges,
    'Churn': churn
})
df = spark.createDataFrame(pd_df)

print("--- 原始数据样本 ---")
df.show(5)

# --- 4. 特征工程与数据预处理 ---
# a. 将目标变量 'Churn' 从字符串 (Yes/No) 转换为数值 (1/0)
df = df.withColumn("label", when(col("Churn") == "Yes", 1).otherwise(0))

# b. 识别需要处理的特征列
categorical_cols = ['Contract', 'PaymentMethod']
numeric_cols = ['tenure', 'MonthlyCharges', 'TotalCharges']

# c. 创建特征处理流水线 (Pipeline)
stages = []

# c1. 处理数值列的缺失值：使用中位数进行填充
imputer = Imputer(
    inputCols=["TotalCharges"],
    outputCols=["TotalCharges_imputed"]
).setStrategy("median")
stages.append(imputer)

# c2. 处理分类特征：先转换为数值索引，再进行独热编码
for col_name in categorical_cols:
    string_indexer = StringIndexer(inputCol=col_name, outputCol=col_name + "_index", handleInvalid='keep')
    encoder = OneHotEncoder(inputCols=[string_indexer.getOutputCol()], outputCols=[col_name + "_vec"])
    stages += [string_indexer, encoder]

# c3. 将所有处理好的特征合并成一个向量
assembler_inputs = [c + "_vec" for c in categorical_cols] + ['tenure', 'MonthlyCharges', 'TotalCharges_imputed']
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
stages.append(assembler)

# --- 5. 建立模型、训练与预测 ---
# a. 建立逻辑回归模型
lr = LogisticRegression(labelCol="label", featuresCol="features")
stages.append(lr)  # 将模型作为流水线的最后一步

# b. 将所有步骤整合进一个总的Pipeline
pipeline = Pipeline(stages=stages)

# c. 划分训练集和测试集 (80%用于训练, 20%用于测试)
(train_data, test_data) = df.randomSplit([0.8, 0.2], seed=42)

# d. 训练模型：Pipeline会依次执行所有定义的步骤
model = pipeline.fit(train_data)

# e. 在测试集上进行预测
predictions = model.transform(test_data)

# --- 6. 模型评估 ---
# 将Spark DataFrame转换为Pandas DataFrame以便使用scikit-learn进行评估
preds_and_labels = predictions.select("prediction", "label").toPandas()

# 计算混淆矩阵的四个值
labels = [0.0, 1.0]  # 确保矩阵是2x2
cm = confusion_matrix(preds_and_labels['label'], preds_and_labels['prediction'], labels=labels)
tn, fp, fn, tp = cm.ravel()

# 计算核心评估指标
# zero_division=0: 当分母为0时（例如模型从未预测某个类别），将结果设为0而不是报错
precision = precision_score(preds_and_labels['label'], preds_and_labels['prediction'], labels=labels, zero_division=0)
recall = recall_score(preds_and_labels['label'], preds_and_labels['prediction'], labels=labels, zero_division=0)
f1 = f1_score(preds_and_labels['label'], preds_and_labels['prediction'], labels=labels, zero_division=0)

print("\n--- 模型评估报告 ---")
print(f"True Positives (TP): {tp}")
print(f"True Negatives (TN): {tn}")
print(f"False Positives (FP): {fp}")
print(f"False Negatives (FN): {fn}")
print("-" * 25)
print(f"Precision: {precision:.2f}")
print(f"Recall:    {recall:.2f}")
print(f"F1 Score:  {f1:.2f}")

# --- 7. 导出评估结果，用于后续可视化 ---
report_data = {
    'Metric': ['TP', 'TN', 'FP', 'FN', 'Precision', 'Recall', 'F1 Score'],
    'Value': [int(tp), int(tn), int(fp), int(fn), precision, recall, f1]
}
report_df = pd.DataFrame(report_data)
# 将Pandas DataFrame转回Spark DataFrame以便使用Spark的写入功能
spark_report_df = spark.createDataFrame(report_df)
spark_report_df.coalesce(1).write.option("header", "true").mode("overwrite").csv("churn_model_report_professional")

print("\n模型评估报告已成功保存至 'churn_model_report_professional' 文件夹。")

# --- 8. 停止Spark Session，释放资源 ---
spark.stop()
