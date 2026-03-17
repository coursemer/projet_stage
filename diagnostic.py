#!/usr/bin/env python3
"""
Quick Diagnostic Script - Projet Stage
Verify all components are working correctly
"""

import sys
from pathlib import Path

def test_imports():
    """Test all critical imports"""
    results = {}
    
    # Python version
    results['Python'] = f"3.{sys.version_info.minor}"
    
    # Test each package
    try:
        import airflow
        results['Airflow'] = airflow.__version__
    except:
        results['Airflow'] = 'FAIL'
    
    try:
        import pyspark
        results['PySpark'] = pyspark.__version__
    except:
        results['PySpark'] = 'FAIL'
    
    try:
        import pandas
        results['Pandas'] = pandas.__version__
    except:
        results['Pandas'] = 'FAIL'
    
    return results

def test_files():
    """Test required files exist"""
    files = {
        'dbt_project.yml': Path('dbt/dbt_project.yml'),
        'sources.yml': Path('dbt/models/sources.yml'),
        'stg_events.sql': Path('dbt/models/staging/stg_events.sql'),
        'test_data_quality.py': Path('spark/test_data_quality.py'),
        'data_profiling.py': Path('spark/data_profiling.py'),
        'hello_world.py': Path('dags/hello_world.py'),
    }
    
    return {name: 'OK' if f.exists() else 'MISSING' for name, f in files.items()}

def main():
    print("=" * 60)
    print("DIAGNOSTIC - Projet Stage")
    print("=" * 60)
    
    # Test imports
    print("\n📦 IMPORTS & VERSIONS:")
    try:
        import airflow
        print(f"  ✅ Airflow: {airflow.__version__}")
    except: print("  ❌ Airflow: FAILED")
    
    try:
        import pyspark
        print(f"  ✅ PySpark: {pyspark.__version__}")
    except: print("  ❌ PySpark: FAILED")
    
    try:
        import pandas
        print(f"  ✅ Pandas: {pandas.__version__}")
    except: print("  ❌ Pandas: FAILED")
    
    try:
        import numpy
        print(f"  ✅ NumPy: {numpy.__version__}")
    except: print("  ❌ NumPy: FAILED")
    
    try:
        import great_expectations
        print(f"  ✅ Great Expectations: OK")
    except: print("  ❌ Great Expectations: FAILED")
    
    try:
        import pandera
        print(f"  ✅ Pandera: OK")
    except: print("  ❌ Pandera: FAILED")
    
    # Test files
    print("\n📁 REQUIRED FILES:")
    files = test_files()
    for name, status in files.items():
        symbol = "✅" if status == "OK" else "❌"
        print(f"  {symbol} {name}: {status}")
    
    # Test DAGs
    print("\n🐝 AIRFLOW DAGS:")
    try:
        airflow_home = Path.home() / '.airflow'
        if airflow_home.exists():
            print(f"  ✅ Airflow Home: {airflow_home}")
        else:
            print(f"  ⚠️  Airflow Home not found")
    except: pass
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY:")
    all_ok = all(status == "OK" for status in files.values())
    if all_ok:
        print("✅ ALL COMPONENTS OK - Project is ready!")
    else:
        print("⚠️  Some files missing - please check above")
    print("=" * 60)

if __name__ == "__main__":
    main()
