##autor: Otavio Augusto
##data: 2023-06-29


# Importação de bibliotecas
import pandas as pd         # Biblioteca para manipulação de dados
import os                   # Biblioteca para manipulação de arquivos e pastas
import shutil               # Biblioteca para manipulação de arquivos e pastas
import concurrent.futures   # Biblioteca para processamento paralelo
from bots.database_operations import extract_data_for_date, transform_data

# Criação de pasta para armazenar os arquivos de saída
pasta_saida = 'base_geral/'  # Pasta onde serão gravados os arquivos de saída

for pasta in [pasta_saida]:
    try:
        os.makedirs(pasta, exist_ok=True)
    except OSError as e:
        print(f'Falha ao criar a pasta {pasta}. Motivo: {e}')

print(f"Criando Pasta (base) com sucesso!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Dados para o banco de dados PostgreSQL
db_host = ''          # Alterar para o IP do servidor de banco de dados
db_port = ''          # Alterar para a porta do servidor de banco de dados
db_name = ''          # Alterar para o nome do banco de dados
db_user = ''          # Alterar para o usuário do banco de dados
db_password = ''      # Alterar para a senha do usuário do banco de dados

# Extrair dados do banco de dados
start_date = pd.Timestamp("2013-01-01")     # Alterar para a data inicial
end_date = pd.Timestamp("2013-01-01")       # Alterar para a data final

# Lista de datas a serem processadas
dates_to_process = pd.date_range(start=start_date, end=end_date, freq='D')

# Lista para armazenar as pastas de saída
output_folders = []

# Função para processar uma data (extração e transformação)
def process_date(date):
    # Extrair dados para a data atual
    output_folder = extract_data_for_date(date, db_host, db_port, db_name, db_user, db_password)

    # Armazenar a pasta de saída para a data atual
    output_folders.append(output_folder)


# Utilizar ThreadPoolExecutor para extrair os dados das datas em paralelo
with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(process_date, dates_to_process)

# Aguardar todas as tarefas de extração serem concluídas
print("Extração de dados concluída!")

# Transformar os dados das pastas extraídas
transform_data("base", pasta_saida)

# Remover a pasta base
shutil.rmtree("base")

print(f"Extração e transformação de dados concluídas!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
