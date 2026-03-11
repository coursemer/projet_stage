#!/usr/bin/env python3
"""
Script de test de connectivité pour Docker, Airflow et Spark
"""

import subprocess
import socket
import time
import sys
from pathlib import Path

class ConnectivityTest:
    def __init__(self):
        self.results = []
        
    def test_docker(self):
        """Vérifier Docker"""
        print("\n=== Test Docker ===")
        try:
            result = subprocess.run(['docker', '--version'], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                print(f"✓ Docker: {result.stdout.strip()}")
                self.results.append(True)
            else:
                print(f"✗ Docker: Erreur - {result.stderr}")
                self.results.append(False)
        except Exception as e:
            print(f"✗ Docker: {e}")
            self.results.append(False)

    def test_docker_compose(self):
        """Vérifier docker-compose"""
        print("\n=== Test docker-compose ===")
        try:
            result = subprocess.run(['docker-compose', '--version'], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                print(f"✓ docker-compose: {result.stdout.strip()}")
                self.results.append(True)
            else:
                print(f"✗ docker-compose: Erreur - {result.stderr}")
                self.results.append(False)
        except Exception as e:
            print(f"✗ docker-compose: {e}")
            self.results.append(False)

    def test_airflow_installed(self):
        """Vérifier Airflow"""
        print("\n=== Test Airflow ===")
        try:
            # Vérifier si airflow_env existe
            if not Path("airflow_env").exists():
                print("✗ Airflow: Environnement virtuel non trouvé")
                self.results.append(False)
                return
                
            result = subprocess.run(
                [f'{Path.cwd()}/airflow_env/bin/python', '-c', 'import airflow; print(airflow.__version__)'],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                print(f"✓ Airflow: Version {result.stdout.strip()}")
                self.results.append(True)
            else:
                print(f"✗ Airflow: {result.stderr}")
                self.results.append(False)
        except Exception as e:
            print(f"✗ Airflow: {e}")
            self.results.append(False)

    def test_spark(self):
        """Vérifier Spark via PySpark plutôt que spark-submit"""
        print("\n=== Test Spark ===")
        # tenter import pyspark et création d'une session
        try:
            import pyspark
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.master("local[*]").appName("conn_test").getOrCreate()
            version = spark.version
            spark.stop()
            print(f"✓ Spark (PySpark) disponible, version {version}")
            self.results.append(True)
            return
        except Exception as e:
            print(f"⚠️ PySpark local non disponible : {e}")
        # fallback : démarrer conteneur spark-master et tester à l'intérieur
        try:
            docker_cmd = [
                'docker-compose', 'run', '--rm', 'spark-master',
                'python', '-c',
                'from pyspark.sql import SparkSession; print(SparkSession.builder.appName("x").master("local[*]").getOrCreate().version)'
            ]
            result = subprocess.run(docker_cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                print(f"✓ Spark (docker) version {result.stdout.strip()}")
                self.results.append(True)
            else:
                print(f"✗ Spark docker: {result.stderr}")
                self.results.append(False)
        except Exception as e:
            print(f"✗ Spark docker: {e}")
            self.results.append(False)

    def test_port_availability(self, host='localhost', port=8080, service=''):
        """Vérifier disponibilité d'un port"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        try:
            result = sock.connect_ex((host, port))
            if result == 0:
                print(f"✓ Port {port} {service}: Accessible")
                return True
            else:
                print(f"⊘ Port {port} {service}: Non disponible (service pas encore démarré)")
                return False
        except Exception as e:
            print(f"✗ Port {port}: Erreur - {e}")
            return False
        finally:
            sock.close()

    def test_ports(self):
        """Vérifier les ports principaux"""
        print("\n=== Test Portabilité (services) ===")
        self.test_port_availability(port=8080, service="(Airflow)")
        self.test_port_availability(port=8081, service="(Spark Master)")
        self.test_port_availability(port=7077, service="(Spark RPC)")

    def run_all(self):
        """Exécuter tous les tests"""
        print("=" * 50)
        print("Tests de Connectivité - projet_stage")
        print("=" * 50)
        
        self.test_docker()
        self.test_docker_compose()
        self.test_airflow_installed()
        self.test_spark()
        self.test_ports()
        
        print("\n" + "=" * 50)
        passed = sum(self.results)
        total = len(self.results)
        print(f"Résultats: {passed}/{total} tests réussis")
        print("=" * 50)

if __name__ == '__main__':
    tester = ConnectivityTest()
    tester.run_all()
