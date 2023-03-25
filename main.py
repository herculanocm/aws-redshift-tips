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
    looger = reshift_functions.get_logger()
 
    looger.info('iniciando')
    aws_access_key_id='ASIAS34UIATUKKIMXT4L'
    aws_secret_access_key='ttoDwLWQLk+RwEq/W0PM/RSiI/QHTRcxdQGBwJi+'
    aws_session_token='IQoJb3JpZ2luX2VjECkaCXNhLWVhc3QtMSJHMEUCIHhfSTrIpHmw02tMu389b8eUZAsT92yuO4vQ/pw64HeyAiEA0Jpn8OaYpS4UmeJGUPykhWzgxgOlI1Cu5K9ziExuv3gqsQMI8v//////////ARACGgwxOTczNDI1Mjg3NDQiDNSPWpJghUxG6mTSByqFA/ywG5qWTBq9i4X1ElTeMahfTTnR/hoefmbaAWaVD5GrFYUu7rCaMsdh7iySLRG+21ux2rbzZKKQ3BbuCRLHxKQ0Lb6eRxjV1n+wnXEIJOVJ9uvI0DrhgRfezAZZwBau7++KFzwymu2b3J14dYQjdoUJn9S0+ObHTiNK9K3CO9NIFaAUMFi19PDLViwXheLXnyXncGpQ/yTDMRo1oftKhcsUaqYrBR8sUYWtrjqcbEv2BHD6M1PjVWpR7FoX4cgIMEJIBgbiuGnGA6OHRSh4k0dI82dzMDoctCpUFPnGO1lQLDsITPe+aXwJEiJUrEmg4rxDQfVeOkRSkLXBrTgNwsuSEcAgYAu7qwgtLqWb4N9KfyabWWGSP4ioA0ZukYCu+DIQ+xhPXEwFrkfKGg2ruCi9a3W3dMQu7KKUuKz5cdZ+BdkJ++eBt7lz4eJEnrDUXP1sr3p0L1lXxerQNeoSWi158qcAHdJ9kiqnhG/T43SjKQBQ8c0fi1QC5KkcW7l3N1vf0+J+MK7P/KAGOqYBxx6eJWmkSY6HVuaNGw58/cW5THHKeacZ4L5Uqsz1K6oeCvfc+Ry/sOhd5bGkF+Zy6oJ/06jLIQtxuGjUESeBsvweOg1tpfL00KpfTQMohByY1Q+vAKp9Cmqr10mhhel2QX7KaGeWULR51CxKslL1mZqNWMuf+4j8mE9DhB7Qr7Yu8fgF/G+JPmFigygIT6qMQA88f3tX9ziuMPJtygLpnB90aZ7Nmw=='
    
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

    result = reshift_functions.redshift_exec_query_with_commit(glue_client, 'asgard-redshift-production-trusted-owner','select 1')
    print(type(result))
    print(result)

