#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
完整的PySpark环境检测脚本
检测所有依赖库的版本和兼容性问题
"""

import sys
import os
import subprocess
import json
from datetime import datetime


class SparkEnvironmentChecker:
    def __init__(self):
        self.results = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "system_info": {},
            "python_info": {},
            "java_info": {},
            "installed_packages": {},
            "compatibility_issues": [],
            "recommendations": []
        }

    def print_section(self, title):
        """打印分节标题"""
        print("\n" + "=" * 60)
        print(f" {title} ")
        print("=" * 60)

    def check_python_version(self):
        """检查Python版本"""
        self.print_section("Python版本检查")

        python_version = sys.version_info
        python_version_str = f"{python_version.major}.{python_version.minor}.{python_version.micro}"

        self.results["python_info"] = {
            "version": python_version_str,
            "version_info": str(sys.version),
            "executable": sys.executable,
            "prefix": sys.prefix
        }

        print(f"Python版本: {python_version_str}")
        print(f"Python路径: {sys.executable}")

        # 检查Python版本兼容性
        if python_version.major == 3 and python_version.minor == 12:
            print("")
            self.results["compatibility_issues"].append(
                ""
            )
            self.results["recommendations"].append(
                ""
            )
        elif python_version.major == 3 and python_version.minor < 9:
            print("⚠️  警告: Python版本过低，PySpark 4.0需要Python 3.9+")
            self.results["compatibility_issues"].append(
                f"Python {python_version_str}太旧，PySpark 4.0.0需要Python 3.9+"
            )

    def check_java_version(self):
        """检查Java版本"""
        self.print_section("Java环境检查")

        java_home = os.environ.get('JAVA_HOME', '未设置')
        print(f"JAVA_HOME: {java_home}")

        try:
            # 检查java命令
            # --- FIX START ---
            # 1. 移除了 stderr=subprocess.STDOUT 参数
            # 2. java -version 的输出在 stderr, 所以后面会从 result.stderr 读取
            result = subprocess.run(['java', '-version'],
                                    capture_output=True,
                                    text=True,
                                    check=False)  # 使用 check=False 避免在返回非0码时抛出异常

            # java -version 命令即使成功也会返回非0退出码，所以我们检查输出内容
            # 并且它的输出在 stderr
            java_output = result.stderr
            if "version" in java_output.lower():
                print(f"Java版本信息:\n{java_output}")

                # 解析Java版本
                if 'version "1.8' in java_output or 'version "8"' in java_output:
                    java_version = "8"
                elif 'version "11' in java_output:
                    java_version = "11"
                elif 'version "17' in java_output:
                    java_version = "17"
                else:
                    java_version = "unknown"

                self.results["java_info"] = {
                    "java_home": java_home,
                    "version": java_version,
                    "output": java_output
                }

                print(f"✓ Java {java_version}已安装")

            else:
                print("✗ Java未找到或无法执行")
                print(f"  - Stderr: {result.stderr}")
                print(f"  - Stdout: {result.stdout}")
                self.results["compatibility_issues"].append("Java未安装或无法执行")
                self.results["recommendations"].append("请安装Java 8、11或17")
            # --- FIX END ---

        except FileNotFoundError:
            print("✗ 无法执行java命令")
            self.results["compatibility_issues"].append("找不到java命令")
            self.results["recommendations"].append("请安装Java并设置JAVA_HOME环境变量")
        except Exception as e:
            print(f"✗ 检查Java时发生未知错误: {e}")

    def check_package_versions(self):
        """检查所有相关Python包的版本"""
        self.print_section("Python包版本检查")

        packages_to_check = {
            'pyspark': {
                'min_for_py312': '4.0.0',
                'pandas_min': {'3.5': '1.0.5', '4.0': '2.0.0'},
                'numpy_min': {'3.5': '1.15', '4.0': '1.21'},
                'pyarrow_min': {'3.5': '4.0.0', '4.0': '11.0.0'}
            },
            'pandas': None,
            'numpy': None,
            'scikit-learn': None,
            'pyarrow': None,
            'py4j': None
        }

        for package_name in packages_to_check:
            try:
                # Special case for scikit-learn import name
                import_name = 'sklearn' if package_name == 'scikit-learn' else package_name
                module = __import__(import_name)
                version = getattr(module, '__version__', 'unknown')
                print(f"✓ {package_name}: {version}")
                self.results["installed_packages"][package_name] = version

            except ImportError:
                print(f"✗ {package_name}: 未安装")
                self.results["installed_packages"][package_name] = "not installed"

        # 检查版本兼容性
        self._check_version_compatibility()

    def _check_version_compatibility(self):
        """检查各个包之间的版本兼容性"""
        print("\n版本兼容性分析:")

        pyspark_version = self.results["installed_packages"].get("pyspark", "not installed")
        pandas_version = self.results["installed_packages"].get("pandas", "not installed")
        numpy_version = self.results["installed_packages"].get("numpy", "not installed")
        python_version = sys.version_info

        if pyspark_version != "not installed":
            # 解析PySpark版本
            try:
                major_version = int(pyspark_version.split('.')[0])
                minor_version = int(pyspark_version.split('.')[1])

                # 检查Python 3.12兼容性
                if python_version.minor == 12 and major_version < 4:
                    print(f"❌ 严重问题: PySpark {pyspark_version}不支持Python 3.12!")
                    self.results["compatibility_issues"].append(
                        f"PySpark {pyspark_version}与Python 3.12不兼容"
                    )

                # 检查PySpark 4.0的依赖要求
                if major_version >= 4:
                    print(f"ℹ️  PySpark 4.0+需要:")
                    print("   - Pandas >= 2.0.0")
                    print("   - NumPy >= 1.21")
                    print("   - PyArrow >= 11.0.0")

                    # 检查Pandas版本
                    if pandas_version != "not installed":
                        pandas_major = int(pandas_version.split('.')[0])
                        if pandas_major < 2:
                            print(f"   ❌ Pandas {pandas_version} < 2.0.0")
                            self.results["compatibility_issues"].append(
                                f"PySpark 4.0需要Pandas >= 2.0.0，当前是{pandas_version}"
                            )

                    # 检查NumPy版本
                    if numpy_version != "not installed":
                        numpy_parts = numpy_version.split('.')
                        numpy_major = int(numpy_parts[0])
                        numpy_minor = int(numpy_parts[1])
                        if numpy_major == 1 and numpy_minor < 21:
                            print(f"   ❌ NumPy {numpy_version} < 1.21")
                            self.results["compatibility_issues"].append(
                                f"PySpark 4.0需要NumPy >= 1.21，当前是{numpy_version}"
                            )

                elif major_version == 3 and minor_version == 5:
                    print(f"ℹ️  PySpark 3.5.x支持:")
                    print("   - Python 3.8-3.11")
                    print("   - Pandas >= 1.0.5")
                    print("   - NumPy >= 1.15")

            except (ValueError, IndexError):
                print(f"⚠️  无法解析PySpark版本: {pyspark_version}")

    def test_spark_functionality(self):
        """测试Spark基本功能"""
        self.print_section("Spark功能测试")

        try:
            from pyspark.sql import SparkSession

            print("创建SparkSession...")
            spark = SparkSession.builder \
                .appName("EnvironmentTest") \
                .master("local[1]") \
                .getOrCreate()

            spark.sparkContext.setLogLevel("ERROR")

            # 测试1：基本range操作
            print("测试1: Range操作...")
            df = spark.range(5)
            count = df.count()
            print(f"✓ Range count: {count}")

            # 测试2：DataFrame创建
            print("测试2: DataFrame创建...")
            test_data = [(1, "A"), (2, "B"), (3, "C")]
            test_df = spark.createDataFrame(test_data, ["id", "value"])
            print("✓ DataFrame创建成功")

            # 测试3：Show操作（最容易出问题的）
            print("测试3: Show操作...")
            try:
                test_df.show()
                print("✓ Show操作成功")
                self.results["spark_test"] = "passed"
            except Exception as e:
                print(f"✗ Show操作失败: {str(e)}")
                self.results["spark_test"] = "failed"
                self.results["compatibility_issues"].append(
                    f"Spark show()操作失败: {str(e)}"
                )

            spark.stop()
            print("✓ SparkSession已停止")

        except Exception as e:
            print(f"✗ Spark测试失败: {str(e)}")
            self.results["spark_test"] = "failed"
            self.results["compatibility_issues"].append(f"Spark测试失败: {str(e)}")

    def generate_recommendations(self):
        """生成最终建议"""
        self.print_section("建议方案")

        python_version = sys.version_info

        if python_version.minor == 12:
            print("\n🚨 您正在使用Python 3.12，这与旧版PySpark不兼容。有两个解决方案：")
            print("\n方案1（推荐）：降级Python")
            print("  在您的项目中使用一个Python 3.11的环境。例如，使用conda:")
            print("  ```bash")
            print("  # 使用conda创建Python 3.11环境")
            print("  conda create -n pyspark311 python=3.11")
            print("  conda activate pyspark311")
            print("  pip install pyspark==3.5.1 pandas numpy scikit-learn")
            print("  ```")

            print("\n方案2：升级到PySpark 4.0")
            print("  ```bash")
            print("  pip uninstall pyspark -y")
            print("  pip install pyspark>=4.0.0")
            print("  # 注意：可能还需要升级其他依赖")
            print("  pip install --upgrade pandas numpy pyarrow")
            print("  ```")

        # 输出所有发现的问题
        if self.results["compatibility_issues"]:
            print("\n⚠️  发现的兼容性问题:")
            for issue in self.results["compatibility_issues"]:
                print(f"  - {issue}")

        # 输出建议
        if self.results["recommendations"]:
            print("\n💡 建议:")
            for rec in self.results["recommendations"]:
                print(f"  - {rec}")

    def save_report(self):
        """保存检测报告"""
        report_file = "spark_env_check_report.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        print(f"\n📄 详细报告已保存到: {report_file}")

    def run_all_checks(self):
        """运行所有检查"""
        print("🔍 开始PySpark环境全面检测...")
        print(f"检测时间: {self.results['timestamp']}")

        # 运行各项检查
        self.check_python_version()
        self.check_java_version()
        self.check_package_versions()
        self.test_spark_functionality()
        self.generate_recommendations()
        self.save_report()

        print("\n✅ 环境检测完成！")

        # 返回是否有严重问题
        return len(self.results["compatibility_issues"]) == 0


if __name__ == "__main__":
    checker = SparkEnvironmentChecker()
    success = checker.run_all_checks()

    if not success:
        print("\n❌ 发现兼容性问题，请按照建议修复后再运行您的代码")
        sys.exit(1)
    else:
        print("\n✅ 环境检测通过，可以运行PySpark代码")
        sys.exit(0)
