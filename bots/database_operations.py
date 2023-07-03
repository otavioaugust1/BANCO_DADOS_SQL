##autor: Otavio Augusto
##data: 2023-06-29


# Importação de bibliotecas
import pandas as pd     # Biblioteca para manipulação de dados
import os               # Biblioteca para manipulação de arquivos e pastas
import shutil           # Biblioteca para manipulação de arquivos e pastas
import psycopg2         # Biblioteca para conexão com o banco de dados PostgreSQL


# Função para extrair dados de um único dia
def extract_data_for_date(date, db_host, db_port, db_name, db_user, db_password):
    # Conexão com o banco de dados PostgreSQL
    conn = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
    cursor = conn.cursor()

    # Leitura do arquivo SQL
    with open(r'C:\Users\otavi\GitHub\SQL_SISREG\sisreg_producao.sql', 'r') as sql_file:
        query = sql_file.read()

    # Formatando a consulta SQL com a data
    query = query.format(date.strftime("%Y-%m-%d"))

    # Print da mensagem de extração de dados para o dia atual
    print(f"Extraindo dados do dia {date.strftime('%Y-%m-%d')}... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Execução da consulta SQL
    cursor.execute(query)
    results = cursor.fetchall()

    # Criação do DataFrame do Pandas para o dia atual
    df = pd.DataFrame(results, columns=["ANO_SOL", "MES_SOL", "IBGE_UNIDADE", "IBGE_USUARIO", "COD_SIGTAP_SOL", "STATUS_SOL", "Quantidade Solicitada"])

    # Fechamento da conexão com o banco de dados
    cursor.close()
    conn.close()

    # Definir o caminho de saída para o mês atual
    output_folder = os.path.join("base", f"{date.year}-{date.strftime('%m')}")
    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder, f"{date.year}-{date.strftime('%m-%d')}_dados_brutos.csv")

    # Salvar os dados em um arquivo CSV
    df.to_csv(output_path, sep=',', index=False)

    return output_folder


# Função para transformar os dados
def transform_data(input_folder, output_folder):
    for foldername in os.listdir(input_folder):
        folder_path = os.path.join(input_folder, foldername)
        if os.path.isdir(folder_path):
            df_concatenated = pd.DataFrame()  # DataFrame para concatenar os arquivos do ANO-MES
            for filename in os.listdir(folder_path):
                if filename.endswith("_dados_brutos.csv"):
                    file_path = os.path.join(folder_path, filename)
                    df = pd.read_csv(file_path)

                    # Realizar o tratamento dos dados
                    df["Quantidade Solicitada"] = df["Quantidade Solicitada"].astype(int)
                    df["Quantidade Aprovada Confirmada"] = 0
                    df["Quantidade Aprovada Não Confirmada"] = 0
                    df["Quantidade Aprovada Paciente Faltou"] = 0
                    df.loc[df["STATUS_SOL"] == "AGENDAMENTO / CONFIRMADO / EXECUTANTE", "Quantidade Aprovada Confirmada"] = df["Quantidade Solicitada"]
                    df.loc[df["STATUS_SOL"] == "AGENDAMENTO / PENDENTE CONFIRMAÇÃO / EXECUTANTE", "Quantidade Aprovada Não Confirmada"] = df["Quantidade Solicitada"]
                    df.loc[df["STATUS_SOL"] == "AGENDAMENTO / FALTA / EXECUTANTE", "Quantidade Aprovada Paciente Faltou"] = df["Quantidade Solicitada"]
                    df = df[["ANO_SOL", "MES_SOL", "IBGE_UNIDADE", "IBGE_USUARIO", "COD_SIGTAP_SOL", "Quantidade Solicitada", "Quantidade Aprovada Confirmada", "Quantidade Aprovada Não Confirmada", "Quantidade Aprovada Paciente Faltou"]]
                    df = df.groupby(["ANO_SOL", "MES_SOL", "IBGE_UNIDADE", "IBGE_USUARIO", "COD_SIGTAP_SOL"], as_index=False).sum()
                    df["MES_SOL"] = df["MES_SOL"].apply(lambda x: str(x).zfill(2))
                    df["COD_SIGTAP_SOL"] = df["COD_SIGTAP_SOL"].apply(lambda x: str(x).zfill(10))
                    df["IBGE_USUARIO"] = df["IBGE_USUARIO"].astype(int)

                    # Concatenar o DataFrame tratado com os anteriores
                    df_concatenated = pd.concat([df_concatenated, df])

            # Verificar se há dados para tratar
            if not df_concatenated.empty:
                # Definir o caminho de saída para o arquivo tratado
                output_filename = f"{foldername}_dados_tratados.csv"
                output_path = os.path.join(output_folder, output_filename)

                # Salvar o DataFrame tratado como arquivo CSV
                df_concatenated.to_csv(output_path, sep=',', index=False)

            # Remover a pasta correspondente ao mês após a geração do arquivo tratado
            shutil.rmtree(folder_path)


# Criação de pasta para armazenar os arquivos de saída
pasta_saida = 'base_geral/'  # Pasta onde serão gravados os arquivos de saída

for pasta in [pasta_saida]:
    try:
        os.makedirs(pasta, exist_ok=True)
    except OSError as e:
        print(f'Falha ao criar a pasta {pasta}. Motivo: {e}')

print(f"Criando Pasta (base) com sucesso!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

