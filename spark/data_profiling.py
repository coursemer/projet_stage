#!/usr/bin/env python3
"""
Profiling de Données
Data profiling with ydata-profiling (formerly pandas-profiling)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta


def generate_sample_dataset():
    """Generate sample dataset for profiling."""
    np.random.seed(42)
    
    # Create realistic sample data
    n_rows = 1000
    data = {
        'user_id': np.arange(1, n_rows + 1),
        'age': np.random.randint(18, 80, n_rows),
        'income': np.random.normal(50000, 20000, n_rows),
        'purchase_count': np.random.poisson(5, n_rows),
        'last_purchase': [datetime.now() - timedelta(days=int(d)) for d in np.random.exponential(30, n_rows)],
        'status': np.random.choice(['active', 'inactive', 'pending'], n_rows),
        'region': np.random.choice(['North', 'South', 'East', 'West'], n_rows),
        'email_valid': np.random.choice([True, False], n_rows, p=[0.95, 0.05]),
    }
    
    # Add some missing values
    df = pd.DataFrame(data)
    df.loc[np.random.choice(df.index, 50, replace=False), 'age'] = np.nan
    df.loc[np.random.choice(df.index, 30, replace=False), 'income'] = np.nan
    
    return df


def profile_with_ydata():
    """Create profiling report with ydata-profiling."""
    print("\n" + "="*60)
    print("📊 Profiling avec ydata-profiling")
    print("="*60)
    
    try:
        try:
            from ydata_profiling import ProfileReport
            
            # Generate sample data
            df = generate_sample_dataset()
            print(f"✅ Dataset généré: {df.shape[0]} lignes, {df.shape[1]} colonnes")
            
            # Create profile
            profile = ProfileReport(
                df,
                title="Profiling Report - Projet Stage",
                minimal=True  # Use minimal mode for faster execution
            )
            
            # Save report
            output_dir = Path("spark/profiles")
            output_dir.mkdir(exist_ok=True)
            
            report_path = output_dir / f"profile_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            profile.to_file(str(report_path))
            
            print(f"✅ Rapport HTML généré: {report_path}")
            print(f"   - Dataset: {df.shape[0]} lignes × {df.shape[1]} colonnes")
            print(f"   - Colonnes numériques: {df.select_dtypes(include=['number']).shape[1]}")
            print(f"   - Colonnes catégorielles: {df.select_dtypes(include=['object']).shape[1]}")
            print(f"   - Valeurs manquantes: {df.isnull().sum().sum()}")
            
            return True
        except Exception as ydata_err:
            # Fallback: Generate simple HTML report manually
            df = generate_sample_dataset()
            output_dir = Path("spark/profiles")
            output_dir.mkdir(exist_ok=True)
            
            report_path = output_dir / f"profile_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            
            # Create simple HTML report
            html_content = f"""
            <html>
            <head>
                <title>Data Profiling Report - Projet Stage</title>
                <style>body {{ font-family: Arial; margin: 20px; }} table {{ border-collapse: collapse; width: 100%; }} th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }} th {{ background-color: #4CAF50; color: white; }}</style>
            </head>
            <body>
                <h1>Data Profiling Report - Projet Stage</h1>
                <h2>Dataset Overview</h2>
                <p><strong>Total Rows:</strong> {df.shape[0]}</p>
                <p><strong>Total Columns:</strong> {df.shape[1]}</p>
                <p><strong>Missing Values:</strong> {df.isnull().sum().sum()}</p>
                <h2>Column Statistics</h2>
                <table>
                    <tr><th>Column</th><th>Type</th><th>Non-Null</th><th>Unique</th><th>Mean/Mode</th></tr>
                    {''.join([f"<tr><td>{col}</td><td>{df[col].dtype}</td><td>{df[col].notna().sum()}</td><td>{df[col].nunique()}</td><td>{df[col].mean() if df[col].dtype in ['int64', 'float64'] else df[col].mode()[0] if len(df[col].mode()) > 0 else 'N/A'}</td></tr>" for col in df.columns])}
                </table>
                <p><em>Generated: {datetime.now()}</em></p>
            </body>
            </html>
            """
            
            with open(report_path, 'w') as f:
                f.write(html_content)
            
            print(f"✅ Rapport HTML (fallback) généré: {report_path}")
            print(f"   - Dataset: {df.shape[0]} lignes × {df.shape[1]} colonnes")
            print(f"   - Colonnes numériques: {df.select_dtypes(include=['number']).shape[1]}")
            print(f"   - Colonnes catégorielles: {df.select_dtypes(include=['object']).shape[1]}")
            print(f"   - Valeurs manquantes: {df.isnull().sum().sum()}")
            
            return True
            
    except Exception as e:
        print(f"❌ ydata-profiling: Erreur - {str(e)}")
        return False


def manual_profiling():
    """Manual profiling with basic statistics."""
    print("\n" + "="*60)
    print("📊 Profiling Manual")
    print("="*60)
    
    try:
        df = generate_sample_dataset()
        
        print(f"\n📈 Statistiques Générales:")
        print(f"   Nombre de lignes: {len(df)}")
        print(f"   Nombre de colonnes: {len(df.columns)}")
        print(f"   Taille mémoire: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        print(f"\n🔍 Analyse des Colonnes:")
        for col in df.columns:
            dtype = df[col].dtype
            missing_pct = (df[col].isnull().sum() / len(df)) * 100
            
            if df[col].dtype in ['int64', 'float64']:
                unique = df[col].nunique()
                stats = f"Mean={df[col].mean():.2f}, Std={df[col].std():.2f}"
            else:
                unique = df[col].nunique()
                stats = f"Top={df[col].mode()[0] if len(df[col].mode()) > 0 else 'N/A'}"
            
            print(f"   {col:20} | Type: {str(dtype):10} | Unique: {unique:5} | Missing: {missing_pct:5.1f}% | {stats}")
        
        print(f"\n📊 Distribution des Données:")
        print(f"   {df.describe().to_string()}")
        
        return True
    except Exception as e:
        print(f"❌ Profiling manuel: Erreur - {str(e)}")
        return False


def main():
    print("\n" + "="*60)
    print("PROFILING DE DONNÉES")
    print("="*60)
    
    results = {
        "ydata-profiling": profile_with_ydata(),
        "Profiling Manuel": manual_profiling()
    }
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    print("\n" + "="*60)
    print("RÉSUMÉ DU PROFILING")
    print("="*60)
    for method, result in results.items():
        status = "✅ Réussi" if result else "⚠️ Échoué"
        print(f"{method}: {status}")
    
    print(f"\nRésultat: {passed}/{total} profiling réussis")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
