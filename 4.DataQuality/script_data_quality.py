
import argparse
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


"""
Regras de qualidade:

· Completude: campos obrigatórios não podem ser nulos.

· Unicidade: deve existir apenas um registro por cod_cliente.

· Validade: telefone, tipo de pessoa, datas, renda e idade devem respeitar as
regras.

· Consistência: nome deve estar em maiúsculas e datas precisam fazer sentido.

· Volume: a tabela não pode estar vazia.

· Schema: as colunas esperadas devem existir.

"""



# COnfigurações


CAMINHO_SILVER = (
    "Anexos/bucket-silver/tb_cliente/anomesdia=20260710"
)

CAMINHO_BRONZE = (
    "Anexos/bucket-bronze/tabela_cliente_landing/anomesdia=20260710"
)

CAMINHO_RELATORIO_SILVER = (
    "4.DataQuality/resultado_silver.txt"
)

CAMINHO_RELATORIO_BRONZE = (
    "4.DataQuality/resultado_bronze.txt"
)

PADRAO_TELEFONE = r"^\(\d{2}\)\d{5}-\d{4}$"

COLUNAS_OBRIGATORIAS = [
    "cod_cliente",
    "nm_cliente",
    "nm_pais_cliente",
    "nm_cidade_cliente",
    "nm_rua_cliente",
    "num_casa_cliente",
    "dt_nascimento_cliente",
    "dt_atualizacao",
    "tp_pessoa",
    "vl_renda",
]

COLUNAS_ESPERADAS = [
    "cod_cliente",
    "nm_cliente",
    "nm_pais_cliente",
    "nm_cidade_cliente",
    "nm_rua_cliente",
    "num_casa_cliente",
    "num_telefone_cliente",
    "dt_nascimento_cliente",
    "dt_atualizacao",
    "tp_pessoa",
    "vl_renda",
]



# Função de Data Quality


def executar_data_quality(dataframe, nome_camada, caminho_relatorio):

    resultados = []


# 1. Completude
# Campos obrigatórios não podem ser nulos


    for coluna in COLUNAS_OBRIGATORIAS:

        quantidade_nulos = (
            dataframe
            .filter(F.col(coluna).isNull())
            .count()
        )

        status = (
            "APROVADO"
            if quantidade_nulos == 0
            else "REPROVADO"
        )

        resultados.append(
            f"Completude - {coluna}: "
            f"{status} | Nulos encontrados: {quantidade_nulos}"
        )



# 2. Unicidade
# Deve existir apenas um registro por cod_cliente


    clientes_duplicados = (
        dataframe
        .groupBy("cod_cliente")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    status_unicidade = (
        "APROVADO"
        if clientes_duplicados == 0
        else "REPROVADO"
    )

    resultados.append(
        "Unicidade - cod_cliente: "
        f"{status_unicidade} | "
        f"Códigos duplicados encontrados: {clientes_duplicados}"
    )



# 3. Validade
# Telefone, tipo de pessoa, datas, renda e idade

# 3.1 Telefone:
# Pode ser nulo, mas, quando preenchido, deve seguir o padrão.

    telefones_invalidos = (
        dataframe
        .filter(
            F.col("num_telefone_cliente").isNotNull()
            & ~F.col("num_telefone_cliente").rlike(
                PADRAO_TELEFONE
            )
        )
        .count()
    )

    status_telefone = (
        "APROVADO"
        if telefones_invalidos == 0
        else "REPROVADO"
    )

    resultados.append(
        "Validade - telefone: "
        f"{status_telefone} | "
        f"Telefones inválidos encontrados: {telefones_invalidos}"
    )


# 3.2 Tipo de pessoa:
# Somente PF ou PJ são aceitos.

    tipos_pessoa_invalidos = (
        dataframe
        .filter(
            ~F.col("tp_pessoa").isin("PF", "PJ")
        )
        .count()
    )

    status_tipo_pessoa = (
        "APROVADO"
        if tipos_pessoa_invalidos == 0
        else "REPROVADO"
    )

    resultados.append(
        "Validade - tipo de pessoa: "
        f"{status_tipo_pessoa} | "
        f"Valores inválidos encontrados: {tipos_pessoa_invalidos}"
    )


# 3.3 Renda:
# Não pode ser negativa.

    rendas_invalidas = (
        dataframe
        .filter(F.col("vl_renda") < 0)
        .count()
    )

    status_renda = (
        "APROVADO"
        if rendas_invalidas == 0
        else "REPROVADO"
    )

    resultados.append(
        "Validade - renda: "
        f"{status_renda} | "
        f"Rendas negativas encontradas: {rendas_invalidas}"
    )


# 3.4 Data de nascimento:
# Não pode ser uma data futura.

    nascimentos_futuros = (
        dataframe
        .filter(
            F.col("dt_nascimento_cliente")
            > F.current_date()
        )
        .count()
    )

    status_nascimento = (
        "APROVADO"
        if nascimentos_futuros == 0
        else "REPROVADO"
    )

    resultados.append(
        "Validade - data de nascimento: "
        f"{status_nascimento} | "
        f"Datas futuras encontradas: {nascimentos_futuros}"
    )


# 3.5 Idade:
# Não pode ser negativa.

    df_com_idade = dataframe.withColumn(
        "idade",
        F.floor(
            F.months_between(
                F.current_date(),
                F.col("dt_nascimento_cliente"),
            ) / 12
        ),
    )

    idades_invalidas = (
        df_com_idade
        .filter(
            (F.col("idade") < 0)
        )
        .count()
    )

    status_idade = (
        "APROVADO"
        if idades_invalidas == 0
        else "REPROVADO"
    )

    resultados.append(
        "Validade - idade: "
        f"{status_idade} | "
        f"Idades fora da faixa de 0 a 120 anos: "
        f"{idades_invalidas}"
    )


# 4. Consistência
# Nome em maiúsculas e datas de atualização posteriores ao nascimento

# 4.1 Nome em letras maiúsculas

    nomes_fora_padrao = (
        dataframe
        .filter(
            F.col("nm_cliente")
            != F.upper(F.trim(F.col("nm_cliente")))
        )
        .count()
    )

    status_nome = (
        "APROVADO"
        if nomes_fora_padrao == 0
        else "REPROVADO"
    )

    resultados.append(
        "Consistência - nome em maiúsculas: "
        f"{status_nome} | "
        f"Nomes fora do padrão: {nomes_fora_padrao}"
    )


# 4.2 Data de atualização:
# Não deve ser anterior à data de nascimento.

    datas_inconsistentes = (
        dataframe
        .filter(
            F.col("dt_atualizacao")
            < F.col("dt_nascimento_cliente")
        )
        .count()
    )

    status_datas = (
        "APROVADO"
        if datas_inconsistentes == 0
        else "REPROVADO"
    )

    resultados.append(
        "Consistência - datas: "
        f"{status_datas} | "
        f"Atualizações anteriores ao nascimento: "
        f"{datas_inconsistentes}"
    )



# 5. Volume
# O parquet a ser ingestado não pode estar vazio


    total_registros = dataframe.count()

    status_volume = (
        "APROVADO"
        if total_registros > 0
        else "REPROVADO"
    )

    resultados.append(
        "Volume - tabela não vazia: "
        f"{status_volume} | "
        f"Total de registros: {total_registros}"
    )



# 6. Schema
# Valida se as colunas obrigatórias existem


    colunas_ausentes = []

    for coluna in COLUNAS_ESPERADAS:
        if coluna not in dataframe.columns:
            colunas_ausentes.append(coluna)

    status_schema = (
        "APROVADO"
        if len(colunas_ausentes) == 0
        else "REPROVADO"
    )

    if len(colunas_ausentes) == 0:
        detalhe_schema = "Nenhuma coluna ausente"
    else:
        detalhe_schema = ", ".join(colunas_ausentes)

    resultados.append(
        "Schema - colunas esperadas: "
        f"{status_schema} | "
        f"{detalhe_schema}"
    )



# Gerando os relatórios


    quantidade_reprovacoes = sum(
        1
        for resultado in resultados
        if "REPROVADO" in resultado
    )

    status_geral = (
        "APROVADO"
        if quantidade_reprovacoes == 0
        else "REPROVADO"
    )

    relatorio = f"""
RELATÓRIO DE QUALIDADE DE DADOS - CAMADA {nome_camada.upper()}
======================================================================

As validações foram executadas na seguinte ordem:

1. Completude
2. Unicidade
3. Validade
4. Consistência
5. Volume
6. Schema

RESULTADOS
======================================================================

"""

    relatorio += "\n".join(resultados)

    relatorio += f"""

======================================================================

STATUS GERAL: {status_geral}

Quantidade de regras reprovadas: {quantidade_reprovacoes}

"""

    with open(
        caminho_relatorio,
        "w",
        encoding="utf-8",
    ) as arquivo:
        arquivo.write(relatorio)

    print(relatorio)



# Iniciando validação


spark = (
    SparkSession.builder
    .appName("data_quality_clientes")
    .getOrCreate()
)


df_bronze = spark.read.parquet(CAMINHO_BRONZE)

df_silver = spark.read.parquet(CAMINHO_SILVER)



# Execução do Data Quality da Bronze


executar_data_quality(
    dataframe=df_bronze,
    nome_camada="Bronze",
    caminho_relatorio=CAMINHO_RELATORIO_BRONZE,
)



# Execução do Data Quality da Silver


executar_data_quality(
    dataframe=df_silver,
    nome_camada="Silver",
    caminho_relatorio=CAMINHO_RELATORIO_SILVER,
)


spark.stop()
