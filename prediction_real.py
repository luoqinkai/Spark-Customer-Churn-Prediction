# -*- coding: utf-8 -*-
"""
电信客户流失预测模型 (生产级脚本)

功能:
1.  环境自检与自动安装依赖库。
2.  从CSV文件加载真实的电信客户数据。
3.  进行健壮的数据清洗和预处理。
4.  使用Spark MLlib Pipeline构建、训练和评估逻辑回归模型。
5.  输出详细的模型评估报告。
"""

# --- 0. 环境自检与自动安装 ---
import sys
import subprocess
import importlib
import os

# 终极修复：在创建SparkSession之前，强制设置Python环境
# 这能确保Spark在任何环境下都能找到正确的Python解释器
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def check_and_install_libraries(required_libraries):
    """检查并安装所有必需的库。"""
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
            subprocess.check_call([sys.executable, '-m', 'pip', 'install'] + missing_packages)
            print("\n所有缺失的库已成功安装！请重新运行此脚本。")
        except subprocess.CalledProcessError:
            print(f"\n错误：自动安装失败。请手动运行: pip install {' '.join(missing_packages)}")
        sys.exit(1)
    print("\n所有依赖库均已满足，开始执行主程序...\n")

# 定义项目需要的库
REQUIRED_LIBRARIES = {
    'pyspark': 'pyspark',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'scikit-learn': 'sklearn'
}
check_and_install_libraries(REQUIRED_LIBRARIES)


# --- 1. 导入所有必要的库 ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Imputer
from pyspark.ml.classification import LogisticRegression
import pandas as pd
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score, accuracy_score


# --- 2. 初始化Spark Session ---
# 使用 .config("spark.driver.host", "127.0.0.1") 避免潜在的网络问题
spark = SparkSession.builder \
    .appName("TelcoChurnPrediction_RealData") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()


# --- 3. 数据加载与初步清洗 (使用真实数据) ---
# 定义数据路径，请确保数据集文件位于此路径
DATA_PATH = "WA_Fn-UseC_-Telco-Customer-Churn.csv"

try:
    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
    print(f"--- 成功从 '{DATA_PATH}' 加载数据 ---")
except Exception as e:
    print(f"错误: 无法在路径 '{DATA_PATH}' 找到或读取数据。")
    print("请确保您已下载 'WA_Fn-UseC_-Telco-Customer-Churn.csv' 文件，并将其放置在与脚本相同的目录中。")
    print(f"详细错误: {e}")
    spark.stop()
    sys.exit(1)

# 数据清洗: TotalCharges列可能包含空格，导致类型推断错误
# 我们需要手动将其转换成数值类型，空格会被转换成null
df = df.withColumn("TotalCharges", col("TotalCharges").cast(DoubleType()))

print("--- 原始数据样本 ---")
df.show(5, truncate=False)
df.printSchema()


# --- 4. 特征工程与数据预处理 ---
# a. 将目标变量 'Churn' 从字符串 (Yes/No) 转换为数值 (1/0)
df = df.withColumn("label", when(col("Churn") == "Yes", 1).otherwise(0))

# b. 识别需要处理的特征列
# 我们从原始数据中选择一些有代表性的特征
categorical_cols = ['gender', 'Partner', 'Dependents', 'PhoneService', 'MultipleLines',
                    'InternetService', 'OnlineSecurity', 'OnlineBackup', 'DeviceProtection',
                    'TechSupport', 'StreamingTV', 'StreamingMovies', 'Contract',
                    'PaperlessBilling', 'PaymentMethod']
numeric_cols = ['tenure', 'MonthlyCharges', 'TotalCharges']

# c. 创建特征处理流水线 (Pipeline)
stages = []

# c1. 处理数值列的缺失值：使用中位数进行填充
# TotalCharges列在清洗后可能存在null值
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
# 注意：我们使用填充后的 'TotalCharges_imputed'
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
(train_data, test_data) = df.randomSplit([0.8, 0.2], seed=1234)

# d. 训练模型
print("\n--- 开始训练模型 ---")
model = pipeline.fit(train_data)
print("--- 模型训练完成 ---")

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
accuracy = accuracy_score(preds_and_labels['label'], preds_and_labels['prediction'])
precision = precision_score(preds_and_labels['label'], preds_and_labels['prediction'], labels=labels, zero_division=0)
recall = recall_score(preds_and_labels['label'], preds_and_labels['prediction'], labels=labels, zero_division=0)
f1 = f1_score(preds_and_labels['label'], preds_and_labels['prediction'], labels=labels, zero_division=0)

print("\n--- 模型评估报告 ---")
print(f"True Positives (TP) : {tp}")
print(f"True Negatives (TN) : {tn}")
print(f"False Positives (FP): {fp}")
print(f"False Negatives (FN): {fn}")
print("-" * 25)
print(f"Accuracy : {accuracy:.2%}")
print(f"Precision: {precision:.2%}")
print(f"Recall   : {recall:.2%}")
print(f"F1 Score : {f1:.2%}")


# --- 7. 导出评估结果 ---
# 定义输出目录
output_dir = "churn_model_report"
report_data = {
    'Metric': ['Accuracy', 'Precision', 'Recall', 'F1 Score', 'TP', 'TN', 'FP', 'FN'],
    'Value': [accuracy, precision, recall, f1, int(tp), int(tn), int(fp), int(fn)]
}
report_df = pd.DataFrame(report_data)
spark_report_df = spark.createDataFrame(report_df)
spark_report_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_dir)

print(f"\n模型评估报告已成功保存至 '{output_dir}' 文件夹。")


# --- 8. 停止Spark Session，释放资源 ---
spark.stop()
print("\nSpark Session已成功停止。")
