
import importlib.util
import unittest
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException



# Importação do script ETL


CAMINHO_SCRIPT_ETL = Path("1.ETL/script.py")

spec = importlib.util.spec_from_file_location(
    "script_etl",
    CAMINHO_SCRIPT_ETL,
)

script_etl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(script_etl)

deduplicar_clientes = script_etl.deduplicar_clientes



# Testes unitários


class TestDeduplicarClientes(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("teste_deduplicar_clientes")
            .getOrCreate()
        )

        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_happy_path_mantem_registro_mais_recente(self):
        """
        Happy path: quando um cliente possui várias versões,
        deve permanecer somente a mais recente.
        """
        dados = [
            (1, "CLIENTE ANTIGO", date(2024, 1, 10)),
            (1, "CLIENTE NOVO", date(2025, 2, 15)),
            (2, "OUTRO CLIENTE", date(2024, 5, 20)),
        ]

        colunas = [
            "cod_cliente",
            "nm_cliente",
            "dt_atualizacao",
        ]

        dataframe = self.spark.createDataFrame(
            dados,
            colunas,
        )

        resultado = deduplicar_clientes(dataframe)

        registros = {
            linha["cod_cliente"]: linha
            for linha in resultado.collect()
        }

        self.assertEqual(resultado.count(), 2)

        self.assertEqual(
            registros[1]["nm_cliente"],
            "CLIENTE NOVO",
        )

        self.assertEqual(
            registros[1]["dt_atualizacao"],
            date(2025, 2, 15),
        )

    def test_caso_borda_cliente_com_um_registro(self):
        """
        Caso de borda: um cliente com apenas um registro deve ser preservado.
        """
        dados = [
            (10, "CLIENTE ÚNICO", date(2025, 1, 1)),
        ]

        colunas = [
            "cod_cliente",
            "nm_cliente",
            "dt_atualizacao",
        ]

        dataframe = self.spark.createDataFrame(
            dados,
            colunas,
        )

        resultado = deduplicar_clientes(dataframe)

        registro = resultado.collect()[0]

        self.assertEqual(resultado.count(), 1)
        self.assertEqual(registro["cod_cliente"], 10)
        self.assertEqual(
            registro["nm_cliente"],
            "CLIENTE ÚNICO",
        )

    def test_erro_sem_coluna_dt_atualizacao(self):
        """
        Situação de erro: ausência da coluna dt_atualizacao deve causar erro.
        """
        dados = [
            (1, "CLIENTE SEM DATA"),
        ]

        colunas = [
            "cod_cliente",
            "nm_cliente",
        ]

        dataframe = self.spark.createDataFrame(
            dados,
            colunas,
        )

        with self.assertRaises(AnalysisException):
            deduplicar_clientes(dataframe).collect()


if __name__ == "__main__":
    unittest.main(verbosity=2)
