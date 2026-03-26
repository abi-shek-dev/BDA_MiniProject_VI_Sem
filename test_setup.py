import os
import subprocess

print("--- DIAGNOSTIC TEST ---")

# 1. Check JAVA_HOME
java_home = os.environ.get('JAVA_HOME')
print(f"1. JAVA_HOME set to: {java_home}")
if not java_home:
    print("   -> WARNING: JAVA_HOME is NOT set! PySpark needs this.")

# 2. Check Hadoop Files
winutils = os.path.exists("C:\\hadoop\\bin\\winutils.exe")
hadoop_dll = os.path.exists("C:\\hadoop\\bin\\hadoop.dll")
print(f"2. winutils.exe found: {winutils}")
print(f"3. hadoop.dll found: {hadoop_dll}")

print("-----------------------")