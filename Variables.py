url = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69'
api_key = '579b464db66ec23bdd0000015329a80a58574d2b7b0a8424ba7e3941'
format = 'json'
limit = 4000
offset=0


database_info = {
    'host' :'airqualityindex.cgyf89pncfsp.us-east-1.rds.amazonaws.com',
    'user' : 'admin',
    'password':'Bhupesh123',
    'databaseName' :"airqualityindex",
    'table' :"airquality",
    'port': 3306
}

local_database = {
    'host' :'localhost',
    'user' : 'root',
    'password':'root',
    'databaseName' :"airqualityindex",
    'table' :"airquality",
    'port': 3306
}

# AWS credentials
access_key = "AKIA5YUAV4T2HCRRW33P"
secret_key = "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"
s3_output_location = 's3a://bhupeshbucket123/export/'

aws_variables ={
    'access_key': "AKIA5YUAV4T2HCRRW33P",
    'secret_key': "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",
    'spark': {
        'spark_output_location': 's3a://bhupeshbucket123/export/'
    },
    'athena':{
        's3_data_location':'s3://bhupeshbucket123/export/',
        's3_query_output': 's3://bhupeshbucket123/query_result/',
        'region_name':'eu-north-1'
    }
}