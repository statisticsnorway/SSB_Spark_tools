import pytest
#from ssb_sparktools.processing import processing as stproc
import os
import sys
sys.path.append(os.path.abspath("/home/jovyan/SSB_Spark_tools/ssb_sparktools/processing/"))

from pyspark.sql import SparkSession
from pyspark import SparkContext


spark = SparkSession.builder.getOrCreate() 

test_data = spark.read.parquet("pytest/testdata/unpack_test_data.parquet").drop('identifikator',
                                                                         'gjelderPeriode',
                                                                         'hendelsetidspunkt',
                                                                         'sekvensnummer',
                                                                         'hendelsetype',
                                                                         'personidentifikator',
                                                                         'inntektsaar',
                                                                         'skjermet',
                                                                         'registreringstidspunkt')


test_dict={}

test_dict = stproc.unpack_parquet(test_data, levels=1)

def test_unpack_parquet_return_dict():
    assert len(test_dict)!=0
    
def test_unpack_parquet_column_rettention():
    assert len(test_dict.keys())==len(test_data.columns)
