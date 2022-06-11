import pandas as pd
# from sqlalchemy import create_engine
import sqlalchemy

def connect_to_postgresql() -> sqlalchemy.engine.base.Engine:
    
    engine = sqlalchemy.create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    # 'postgresql://user:password@hostname:port/database_name'
    engine.connect()
    
    print("PostgreSQL conectado com sucesso! \n")
    return engine
    
def load_dataset(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df   

def get_sql_schema(df: pd.DataFrame, engine: sqlalchemy.engine.base.Engine) -> str:
    return pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine)

def create_tables_postgresql(df: pd.DataFrame, engine: sqlalchemy.engine.base.Engine) -> None:
    df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
    #if_exists='replace: se já existir database com esse nome, irá substituir
    
    print("Tabelas criadas com sucesso! \n")

def insert_data_postgresql(df: pd.DataFrame, engine: sqlalchemy.engine.base.Engine) -> None:
    df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
    #if_exists='append: se dados já existir, adiciona ele sem substituir 
    
    print("Dados inseridos com sucesso! \n")
    
def main():
    
    #conectando com postgreSQL
    engine = connect_to_postgresql()
    
    path = r"dataset\yellow_tripdata_2021-01.parquet"
    df = load_dataset(path)
    print(df.head(2))
    
    # criar o comando sql para criar a tabela no banco de dados
    # utilizando as colunas como variáveis
    # 'engine' é passado para gerar o comando em postgreSQL
    sql_schema = get_sql_schema(df, engine)
    print(sql_schema)
    
    #criando tabelas no postgreSQL
    # create_tables_postgresql(df=df, engine=engine)
    
    
    # inserindo dados nas tabelas
    insert_data_postgresql(df=df, engine=engine)
    
    
if __name__ == "__main__":
    main()