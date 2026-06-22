"""
Configuration pytest : ajoute le répertoire etl/ au chemin Python
pour permettre les imports depuis dags/.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
