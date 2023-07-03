# Projeto de Extração e Transformação de Dados

Este projeto consiste em um script em Python para extrair e transformar dados de um banco de dados PostgreSQL. O objetivo é extrair os dados brutos de solicitações realizadas em um determinado período de tempo e transformá-los em um formato mais adequado para análise.

## Funcionalidades

- Conexão com um banco de dados PostgreSQL para extrair os dados de solicitações.
- Extração dos dados em arquivos CSV separados por data.
- Transformação dos dados, incluindo tratamento de colunas, agregação e formatação.
- Concatenação dos dados transformados por mês.
- Geração de arquivos CSV contendo os dados tratados para cada mês.

## Requisitos

- Python 3.7 ou superior
- Bibliotecas Python: pandas, psycopg2, shutil, concurrent.futures

## Configuração

1. Certifique-se de ter Python instalado no seu sistema.
2. Instale as bibliotecas necessárias executando o seguinte comando:


```` python main.py ````

3. Faça o download do projeto para o seu diretório de trabalho.

## Uso

1. Configure as variáveis de conexão com o banco de dados PostgreSQL no script:
- `db_host`: endereço do host do banco de dados
- `db_port`: porta do banco de dados
- `db_name`: nome do banco de dados
- `db_user`: usuário do banco de dados
- `db_password`: senha do usuário do banco de dados
2. Defina o período de tempo desejado para a extração e transformação de dados no script:
- `start_date`: data inicial no formato "YYYY-MM-DD"
- `end_date`: data final no formato "YYYY-MM-DD"
3. Execute o script Python:

```` pip install psycopg2 pandas ````

4. O script irá extrair os dados brutos para cada dia no período especificado e, em seguida, transformá-los em arquivos tratados por mês.
5. Os arquivos tratados serão salvos na pasta "base_geral" no diretório de trabalho.

## Autor

Otavio Augusto - <https://github.com/otavioaugust1>

## Licença

Este projeto está licenciado sob a Licença MIT. Consulte o arquivo LICENSE para obter mais informações.
