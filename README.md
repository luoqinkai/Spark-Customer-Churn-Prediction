# **PySpark电信客户流失预测模型**

这是一个使用PySpark构建的端到端机器学习项目，旨在预测电信客户是否会流失。项目利用了Spark的分布式计算能力和MLlib库，实现了一个从数据加载、清洗、特征工程到模型训练和评估的完整流水线。

## **✨ 项目特性**

* **环境自检**: 脚本在运行前会自动检查并尝试安装所有必需的Python库。  
* **端到端流水线**: 使用Spark MLlib Pipeline，将数据处理和模型训练步骤串联起来，使代码更整洁、可复用性更高。  
* **真实数据驱动**: 使用Kaggle上经典的[电信客户流失数据集](https://www.kaggle.com/datasets/blastchar/telco-customer-churn)进行建模。  
* **健壮的预处理**: 包含对缺失值（Imputer）、分类特征（StringIndexer \+ OneHotEncoder）的完整处理流程。  
* **详细的评估**: 输出包括准确率、精确率、召回率、F1分数和混淆矩阵在内的完整评估报告。

## **🚀 如何开始**

### **1\. 环境准备**

* **Java**: 确保已安装 **Java 17** 或更高版本，并正确配置了JAVA\_HOME环境变量。  
* **Python**: 建议使用 **Python 3.9** 或更高版本。

### **2\. 项目设置**

1. **克隆或下载仓库**  
   git clone \[你的仓库URL\]  
   cd \[你的仓库目录\]

2. **创建并激活虚拟环境** (推荐)  
   \# Windows  
   python \-m venv .venv  
   .\\.venv\\Scripts\\activate

   \# macOS / Linux  
   python3 \-m venv .venv  
   source .venv/bin/activate

3. **下载数据集**  
   * 从[Kaggle](https://www.kaggle.com/datasets/blastchar/telco-customer-churn)下载数据集。  
   * 将下载的WA\_Fn-UseC\_-Telco-Customer-Churn.csv文件放置在项目的根目录下。

### **3\. 运行预测模型**

* 所有依赖库都会由脚本自动检查并提示安装。你只需直接运行主脚本：  
  python prediction\_final.py

* 程序运行成功后，你会在项目根目录下看到一个名为churn\_model\_report的文件夹，里面包含了模型评估结果的CSV文件。

## **📂 项目结构**

.  
├── prediction\_final.py             \# 主程序脚本  
├── WA\_Fn-UseC\_-Telco-Customer-Churn.csv  \# 数据集文件 (需自行下载)  
├── churn\_model\_report/             \# 输出目录，存放模型评估报告  
│   └── part-00000-....csv  
└── README.md                       \# 本说明文档

## **💡 未来改进方向**

当前项目实现了一个完整的基线模型（Baseline Model），但还有巨大的提升空间。以下是一些可以尝试的改进方向：

1. **超参数调优 (Hyperparameter Tuning)**  
   * 当前的逻辑回归模型使用的是默认参数。可以使用CrossValidator和ParamGridBuilder来自动搜索最佳的参数组合（如正则化参数regParam、弹性网络参数elasticNetParam），这将显著提升模型性能。  
2. **尝试更复杂的模型**  
   * 除了逻辑回归，可以尝试RandomForestClassifier（随机森林）或GBTClassifier（梯度提升树）。这两种模型通常能更好地捕捉特征间的复杂关系，获得更高的预测精度。  
3. **高级特征工程**  
   * **特征交叉**: 创建新的组合特征，例如Contract和InternetService的交叉特征，可能会发掘出更有价值的信息。可以使用VectorAssembler或自定义Transformer实现。  
   * **特征分箱**: 将连续的数值特征（如tenure）转换成离散的分类特征（如“新用户”、“中期用户”、“老用户”），有时能让线性模型更好地学习。可以使用Bucketizer。  
4. **不平衡数据处理**  
   * 客户流失数据通常是不平衡的（流失用户是少数）。这会导致模型倾向于预测“不流失”。可以尝试在训练前对少数类（流失用户）进行**过采样 (Oversampling)** 或对多数类进行**欠采样 (Undersampling)** 来平衡数据集。  
5. **配置与代码解耦**  
   * 将文件路径、模型参数、特征列名等配置信息提取到一个单独的配置文件中（如config.yaml或config.json），让主脚本更专注于逻辑，提高可维护性。  
6. **结果可视化**  
   * 增加使用matplotlib或seaborn库来绘制混淆矩阵热力图、ROC曲线等，使评估结果更直观。  
7. **模型可解释性 (SHAP)**  
   * 使用类似SHAP (SHapley Additive exPlanations) 的工具来解释模型的预测结果，理解哪些特征对客户流失的决策影响最大。