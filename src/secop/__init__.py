"""Paquete ``secop``: lógica reutilizable y testeable del proyecto.

Todo lo que transforma datos vive aquí (no en los notebooks) por dos razones:
1. Se puede probar con pytest en local, en Codespaces o en GitHub Actions,
   sin necesidad de Databricks.
2. Los notebooks quedan cortos y se dedican a leer, llamar funciones y escribir.
"""

# Versión del código. Se registra en MLflow para saber con qué código se
# entrenó cada modelo. Se actualiza junto con la etiqueta de git (v0.1, v0.2...).
__version__ = "0.1.0"
