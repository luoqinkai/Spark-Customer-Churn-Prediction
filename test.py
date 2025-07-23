# immediate_fix.py
# 针对Python 3.12和PySpark 3.5.1兼容性问题的修复脚本

import os
import sys
import warnings

# 忽略一些不影响功能的警告
warnings.filterwarnings("ignore", message="Deprecated in 3.0.0")


def patch_python312_compatibility():
    """
    修复Python 3.12与PySpark的兼容性问题
    """
    # 1. 设置环境变量，禁用某些可能导致问题的Python 3.12特性

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # 2. 禁用Python worker重用（这是导致EOF错误的常见原因）
    os.environ['PYSPARK_PYTHON_WORKER_REUSE'] = 'false'

    # 3. 设置确定性的哈希种子
    os.environ['PYTHONHASHSEED'] = '0'

    # 4. 确保Python路径一致
    python_exe = sys.executable
    os.environ['PYSPARK_PYTHON'] = python_exe
    os.environ['PYSPARK_DRIVER_PYTHON'] = python_exe


def create_compatible_spark_session():
    """
    创建一个针对兼容性问题优化的Spark会话
    """
    from pyspark.sql import SparkSession

    # 构建一个配置了所有必要参数的Spark会话
    spark = SparkSession.builder \
        .appName("Python312CompatibleSpark") \
        .master("local[1]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.python.worker.memory", "1g") \
        .config("spark.python.worker.reuse", "false") \
        .config("spark.python.profile", "false") \
        .config("spark.sql.execution.arrow.enabled", "false") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.sql.execution.arrow.fallback.enabled", "true") \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.rpc.message.maxSize", "1024") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "1024m") \
        .getOrCreate()

    # 设置日志级别减少干扰
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def test_spark_functionality():
    """
    测试Spark的基本功能
    """
    print("=== 开始PySpark兼容性测试 ===\n")

    # 应用兼容性补丁
    patch_python312_compatibility()

    print("1. 创建Spark会话...")
    spark = create_compatible_spark_session()
    print("   ✓ Spark会话创建成功")

    try:
        # 测试1：最基础的range操作
        print("\n2. 测试range操作...")
        df_range = spark.range(5)
        count = df_range.count()
        print(f"   ✓ Range计数成功: {count}")

        # 测试2：创建简单DataFrame（不使用中文避免编码问题）
        print("\n3. 测试DataFrame创建...")
        data = [
            (1, "user1"),
            (2, "user2"),
            (3, "user3")
        ]
        columns = ["id", "name"]
        df = spark.createDataFrame(data, columns)
        print("   ✓ DataFrame创建成功")

        # 测试3：显示数据（这是您原来失败的操作）
        print("\n4. 测试show()操作...")
        df.show()
        print("   ✓ show()执行成功")

        # 测试4：简单的DataFrame操作
        print("\n5. 测试DataFrame转换...")
        df_filtered = df.filter(df.id > 1)
        filtered_count = df_filtered.count()
        print(f"   ✓ 过滤操作成功: {filtered_count} 行")

        # 测试5：收集数据到本地
        print("\n6. 测试collect()操作...")
        collected_data = df.collect()
        print(f"   ✓ 数据收集成功: {len(collected_data)} 行")

        print("\n=== 所有测试通过！PySpark环境正常工作 ===")

    except Exception as e:
        print(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()

        # 提供详细的调试信息
        print("\n=== 调试信息 ===")
        print(f"Python版本: {sys.version}")
        print(f"Python路径: {sys.executable}")

        try:
            import pyspark
            print(f"PySpark版本: {pyspark.__version__}")
        except:
            print("PySpark版本: 无法获取")

    finally:
        # 确保关闭Spark会话
        spark.stop()
        print("\n会话已关闭")


if __name__ == "__main__":
    test_spark_functionality()