##autor: Otavio Augusto
##data: 2023-07-02

# Importação de bibliotecas
import pandas as pd
import os

# Definir o caminho para a pasta que contém os arquivos CSV
pasta = 'base_geral'  # Caminho para a pasta que contém os arquivos CSV
pasta_saida = 'base_geral_ano'  # Pasta de saída para os arquivos CSV por ano

# Criar a pasta de saída, se ainda não existir
os.makedirs(pasta_saida, exist_ok=True)
arquivos_csv = os.listdir(pasta)  # Lista de arquivos CSV na pasta
dataframes = []


# Iterar sobre cada arquivo CSV na pasta
for arquivo_csv in arquivos_csv:
    caminho_arquivo = os.path.join(pasta, arquivo_csv)
    df = pd.read_csv(caminho_arquivo, dtype={'COD_SIGTAP_SOL': str}, low_memory=False)
    df = df.iloc[1:]  # Remover a primeira linha (cabeçalho)
    dataframes.append(df)


# Concatenar os dataframes em um único dataframe
df_concatenado = pd.concat(dataframes, ignore_index=True)


# Agrupar os dados e somar as quantidades
df_agrupado = df_concatenado.groupby(['ANO_SOL', 'IBGE_UNIDADE', 'IBGE_USUARIO', 'COD_SIGTAP_SOL'], as_index=False).sum()


# Remover a coluna 'MES_SOL'
df_final = df_agrupado.drop('MES_SOL', axis=1)


# Iterar sobre cada ano e salvar um arquivo CSV correspondente
for ano in df_final['ANO_SOL'].unique():
    df_ano = df_final[df_final['ANO_SOL'] == ano]
    caminho_saida = os.path.join(pasta_saida, f'{ano}.csv')
    df_ano.to_csv(caminho_saida, index=False)
    print(f'Arquivo CSV {ano}.csv salvo com sucesso.')

print('Arquivos CSV por ano foram salvos com sucesso.')
