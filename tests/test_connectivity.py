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
        """Vérifier Spark"""
        print("\n=== Test Spark ===")
        try:
            result = subprocess.run(['spark-submit', '--version'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                lines = result.stdout.split('\n')
                print(f"✓ Spark: Installé")
                for line in lines[:2]:
                    if line.strip():
                        print(f"  {line.strip()}")
                self.results.append(True)
            else:
                print(f"✗ Spark: {result.stderr}")
                self.results.append(False)
        except Exception as e:
            print(f"✗ Spark: {e}")
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
        self.test_port_availability(8080, "(Airflow)")
        self.test_port_availability(8081, "(Spark Master)")
        self.test_port_availability(7077, "(Spark RPC)")

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
