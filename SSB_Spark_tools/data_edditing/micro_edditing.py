import time
import datetime
from pyspark.sql import SparkSession 
from pyspark.sql.types import *
import pyspark.sql.functions as F

def micro_edditing():
    user = input('Oppgi bruker identen')
    data_path = input('Oppgi filsti for det datasettet du ønsker og edditere')
    previous_log = input('Dersom dette er fortsettelse av edditering oppgi Log position, dersom dette er første edditering skriv: ny')

    if previous_log == '':
        print('Kjøring uten ytterlige edditeringer')
    elif previous_log == 'ny':
        print('Ingen log oppgit, initierer ny log')
        log = {}
        df_edditor =[]
        auto_edditor = {}
    else: 
        log = spark.read.namespace(previous_log)
        auto_edditor = spark.read.namespace(previous_log)

    inn_data = spark.read.namespace(data_path)
    print('Under følger ett utsnitt av dattasettet du har hentet opp for editering. \nVennligst påse at dette er korrekt data du ønsker og editere')
    inn_data.show()
    changed_df= inn_data

    
    col = input('Edditerings Kolonne')
    ident_col = input('Identifikasjons Kolonne')
    row_identifier = input('Identifikasjons Verdi')
    reason =input('Edditerings Årsak')
    new_value = input('Ny Verdi')

    if reason != '':
        original = (inn_data.select(col)
                    .where(F.col(ident_col)==row_identifier)
                    .collect()
                   )
        timestamp = (datetime.datetime
                     .fromtimestamp(time.time())
                     .strftime('%Y-%m-%d %H:%M:%S')
                    )
        log[user+timestamp] = [data_path, 
                               ident_col, 
                               row_identifier, 
                               col, original, 
                               new_value, 
                               reason
                              ]
        df_edditor.append([data_path, 
                           ident_col, 
                           row_identifier,
                           col, 
                           new_value
                          ])

    else:
        print('Oppgi informasjon i skjema og trykk på enter for å registrere edditeringen')
        
    log_list = log[user+timestamp]
    print('----------------------------------------')
    print('Bruker: ', user)
    print('Datasett endret: ', log_list[0])
    print('\nEndring er foretatt for ', log_list[1],':',log_list[2])
    print('Kolonne: ', log_list[3] ,'er endret')
    print('fra: ', log_list[4], '\ntil: ', log_list[5])
    print('\nBegrunnelse for denne endringen er oppgit som: ', log_list[6])
    print('\nEndringen er utført: ', timestamp)
    print('----------------------------------------')
    
    confirm = input('Ønsker du å bekrefte denne edditeringen? [y/n]')
    if confirm =='y':
        for eddit in df_edditor:
        if eddit[0] in auto_edditor:
            auto_edditor[eddit[0]].append(eddit[1:]) 
        else:
            auto_edditor[eddit[0]] = [eddit[1:]]
        
        print(len(df_edditor),' Edditeringer er lagret')
        df_edditor =[]
    else:
        break
        
    for k,v in log.items():
    print('Bruker: ', k[0:3])
    print('Datasett endret: ', v[0])
    print('\nEndring er foretatt for ', v[1],':',v[2])
    print('Kolonne: ', v[3] ,'er endret')
    print('fra: ', v[4], '\ntil: ', v[5])
    print('\nBegrunnelse for denne endringen er oppgit som: ', v[6])
    print('\nEndringen er utført: ', k[3:])
    print('----------------------------------------')

        
if len(auto_edditor)>0:
    log= spark.read.namespace(previous_log)
    auto_edditor= spark.read.namespace(previous_log)
    for datasett, eddits in auto_edditor.items():
        eddited_df = spark.read.namespace(datasett)
        for eddit in eddits:
            eddited_df = (eddited_df.withColumn(eddit[2],
                                                F.when(F.col(eddit[0])==eddit[1],
                                                       F.lit(eddit[3]))
                                                .otherwise(F.col(eddit[2])))
        print('Lagret edditeringer for ',datasett,' til LDS')
    elif len(auto_edditor)==0:
        print('No eddits registered or performed')
    else:
        auto_edditor = spark.read.namespace(previous_log)
        for datasett, eddits in auto_edditor.items():
            eddited_df = spark.read.namespace(datasett)
        for eddit in eddits:
            eddited_df = (eddited_df.withColumn(eddit[2],
                                                F.when(F.col(eddit[0])==eddit[1], 
                                                       F.lit(eddit[3]))
                                                .otherwise(F.col(eddit[2]))))
        print('Lagret edditeringer for ',datasett,' til LDS')
    