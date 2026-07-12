
import importlib.util
import unittest
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException



# Importação do script ETL e massa de dados


CAMINHO_SCRIPT_ETL = Path("1.ETL/script.py")

CAMINHO_DADOS_TESTE = Path("5.TesteUnitario/dados_teste_clientes.csv")


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



    """
    Lê a massa de dados criada especificamente para os testes unitários.
    """

    def carregar_base_teste(self):
        return (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("dateFormat", "yyyy-MM-dd")
            .csv(str(CAMINHO_DADOS_TESTE))
        )



    """
    Happy path: apenas a versão mais recente do cadastro deve ser mantida.
    """

    def test_happy_path_mantem_registros_mais_recentes(self):
        
        dataframe = self.carregar_base_teste()

        resultado = deduplicar_clientes(dataframe)

        registros = {
            linha["cod_cliente"]: linha
            for linha in resultado.collect()
        }

        self.assertEqual(
            resultado.count(),
            3,
        )

        self.assertEqual(
            registros[1]["nm_cliente"],
            "CLIENTE MAIS RECENTE",
        )

        self.assertEqual(
            registros[3]["nm_cliente"],
            "SEGUNDA VERSAO",
        )



    """
    Caso de borda: cliente com apenas uma versão deve ser preservado.
    """

    def test_caso_borda_cliente_com_um_registro(self):
        
        dataframe = self.carregar_base_teste()

        resultado = deduplicar_clientes(dataframe)

        cliente_unico = (
            resultado
            .filter(F.col("cod_cliente") == 2)
            .collect()
        )

        self.assertEqual(
            len(cliente_unico),
            1,
        )

        self.assertEqual(
            cliente_unico[0]["nm_cliente"],
            "CLIENTE UNICO",
        )



    """
    Caso extremo: após a deduplicação, o cod_cliente pode aparecer + 1x.
    """

    def test_resultado_sem_clientes_duplicados(self):
        
        dataframe = self.carregar_base_teste()

        resultado = deduplicar_clientes(dataframe)

        total_registros = resultado.count()

        total_clientes_distintos = (
            resultado
            .select("cod_cliente")
            .distinct()
            .count()
        )

        self.assertEqual(
            total_registros,
            total_clientes_distintos,
        )



    """
    Situação de erro: ausência da coluna dt_atualizacao deve causar erro.
    """

    def test_erro_sem_coluna_dt_atualizacao(self):
        
        dataframe = self.spark.createDataFrame(
            [
                (1, "CLIENTE SEM DATA"),
            ],
            [
                "cod_cliente",
                "nm_cliente",
            ],
        )

        with self.assertRaises(AnalysisException):
            deduplicar_clientes(dataframe).collect()


if __name__ == "__main__":
    unittest.main(
        argv=[""],
        exit=False,
        verbosity=2,
    )
