from de_redshift_tips import reshift_functions
from boto3 import client as aws_client

if __name__ == "__main__":

    # lista = [
    #     {'column_name': 'coluna1', 'ordinal_position': 5},
    #     {'column_name': 'coluna2', 'ordinal_position': 3},
    #     {'column_name': 'coluna3', 'ordinal_position': 2},
    #     {'column_name': 'coluna4', 'ordinal_position': 4},
    #     {'column_name': 'coluna5', 'ordinal_position': 1},
    #     {'column_name': 'coluna6', 'ordinal_position': 1}
    # ]

    # newList = s3_file_transformations.order_columns_by_schema(lista, 'ordinal_position')

    # print(newList)

 
    


    aws_access_key_id='ASIAS34UIATUMVOFDSMX'
    aws_secret_access_key='54VGeTPDImQeBMPoq69Acyqro+XVCRlIBh00HcH9'
    aws_session_token='IQoJb3JpZ2luX2VjEAYaCXNhLWVhc3QtMSJHMEUCIQC7MsrkssKXOLCc9oZYexAX0bfm5X1hGnPcei4a3q5+hwIgEljY4M+AmSO38S13MRSe4c/mkd8ZQZPsggSB2hVCREMqrQMI7///////////ARACGgwxOTczNDI1Mjg3NDQiDKPVLU+fX2DU7Np8PyqBAzXpqjVJS3Z9U4SdQw29E9+JwGeD4JoKRmomZh+aAWiu2Bx/PywbSu1y+jFrrStQIv56OHZOsGbx9RYRHfxf05ZZsUeh6yr+PvjExBBVYJ6mO+Xl0r9PidJtLhTKQcnJgc+wF5V9jC1mm803H8Vfj9Vq638ZeaAid1HU8NF9RChki4JRPlgC8w4qTEaKConcSYhKeAgW0sOelscBKKYxcemUU6irUjFH7K1PbFeRe83hfDD+7k80+ZWMPWbBR/lp/COyTmeKymmbcfSMKxiHZ4ETzyFTLBnzmAQUGaH0gUFfilcFHcviTd+9RWkpNcwvS5TvgTxxm8LqarmX2GBWN3JDPy9TR1Mm+1nkKCzH/LYNk73y2hEBkaPUT2J8bKjjx+kaR+yjKUzaZfJE0fx46pCuQRt3Poz5G1FF8CSzvOdoRxxmVWNJZwpZuhLuB2/rYylnWNxm0I8Qt75GxJvBUSJaABx7F0jptodid/ZPY4/oaY5piAPq/0Hoy1yyV4KQHRowrcDloQY6pgF+ItGcrf1QPS1Crm1N4i/8o4MWHrd/e529EI0+0JN8bNFybM6Hvrh2V5+lVWeRjG4Y6gLBB8uX8LhQWGJM/pYMIegnrEo3nQawlQlNLx+QjSheaj4OuZMuoiM9j4r0rnxZ/ZoCDeCd5EpjAQEsMMyqmOCEMvtwlPnogFNk9uR/EfhLJhQsRuQHOqFUL4PgKOWg5wsO6lzo8SQbFHtV3+PpjufAGeDY'
        
    
    s3_client = aws_client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
    )

    glue_client = aws_client(
        "glue",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        region_name='sa-east-1'
    )

    conn_options = reshift_functions.get_connection_glue(glue_client, 'vivo_postgres_production_pricing_clj')
    jdbc_url = f"""jdbc:postgresql://{conn_options['hostname']}:{conn_options['port']}/"""
    print(type(jdbc_url))
    print(jdbc_url)





