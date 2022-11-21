import os
import sys

import pytest


# Legger undermapper med scripts til systempath, slik at vi kan importere biblioteker fra disse mappene.#
sys.path.append(os.path.abspath(os.getcwd() + "/ssb_sparktools/processing/"))
sys.path.append(os.path.abspath(os.getcwd() + "/ssb_sparktools/editing/"))
sys.path.append(os.path.abspath(os.getcwd() + "/ssb_sparktools/quality/"))

from datetime import datetime

import pyspark.sql.functions as F
from controls import *
from editing import *
from processing import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from quality import *


# Definerer Spark-session#
spark = SparkSession.builder.getOrCreate()

#### LAGER TESTDATA ####
# Flat data #
testdata_schema = StructType(
    [
        StructField("identifikator", StringType(), False),
        StructField("numbvar", LongType(), True),
        StructField("boolvar", BooleanType(), True),
        StructField("varenummer", StringType(), True),
    ]
)
testdata_raw = [
    ("id1", 1, True, "#001"),
    ("id2", 2, False, "#002"),
    ("id3", None, False, "#003"),
    ("id4", 4, None, "#004"),
    ("id5", 4, True, "#005"),
]
testdata = spark.createDataFrame(testdata_raw, testdata_schema)

testdata_kodeliste_schema = StructType(
    [
        StructField("varenummer", StringType(), True),
        StructField("frukt", StringType(), True),
    ]
)
testdata_kodeliste_raw = [
    ("#001", "Eple"),
    ("#002", "Banan"),
    ("#003", "Kiwi"),
    ("#004", "Appelsin"),
    ("#006", "Druer"),
]
testdata_kodeliste = spark.createDataFrame(
    testdata_kodeliste_raw, testdata_kodeliste_schema
)

# Hierarkisk data #
hierarki_schema = StructType(
    [
        StructField("persid", StringType(), False),
        StructField("dato", StringType(), False),
        StructField(
            "arbeidsgiver",
            ArrayType(
                StructType(
                    [
                        StructField("navn", StringType(), True),
                        StructField("adresse", StringType(), True),
                        StructField(
                            "ansatte",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("navn", StringType(), True),
                                        StructField("adresse", StringType(), True),
                                    ]
                                )
                            ),
                        ),
                    ]
                )
            ),
        ),
        StructField(
            "utdanning",
            ArrayType(
                StructType(
                    [
                        StructField("utdanningsinstitusjon", StringType(), True),
                        StructField("adresse", StringType(), True),
                        StructField(
                            "utdanning",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("fag", StringType(), True),
                                        StructField("eksamensdato", StringType(), True),
                                        StructField(
                                            "annet",
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            "Hovedlinje",
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            "karakter",
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                )
                                            ),
                                        ),
                                    ]
                                )
                            ),
                        ),
                    ]
                )
            ),
        ),
    ]
)
hierarkidata_raw = [
    (
        "#ID1",
        "01Jan2020",
        [
            (
                "Industri AS",
                "Jernveien 24",
                [("Per", "Storgata 3"), ("Kari", "Toppen 2")],
            )
        ],
        [
            (
                "Mek Skole",
                "Mek veien 1",
                [
                    ("Mekaniskefag", "21Jun2013", [("Plateteknikk", "B")]),
                    ("Byggingeniør", "11Jun2018", [("Stålkonstruksjon", "B")]),
                ],
            )
        ],
    ),
    (
        "#ID2",
        "02Mar2020",
        [
            (
                "Lommerusk AS",
                "Sliteveien 23",
                [("Espen", "Ukjent"), ("Ronny", "Kaiegata 2")],
            )
        ],
        [
            (
                "Harde Skole",
                "Kjeppveien 10",
                [("Grunnskole", "19Jun2014", [("Ingen", "C")])],
            )
        ],
    ),
    (
        "#ID3",
        "15Feb2020",
        [("Papir AS", "Papirveien 24", [("Ole", "Storgata 3"), ("Siri", "Toppen 3")])],
        [
            (
                "Skogen Skole",
                "Treveien 5",
                [
                    ("Papirfag", "21Jun2014", [("Papp", "D")]),
                    ("Papiringeniør", "11Jun2012", [("Papirrull", "A")]),
                ],
            )
        ],
    ),
]

hierarki_testdata = spark.createDataFrame(hierarkidata_raw, hierarki_schema)

dfpath_1 = ["arbeidsgiver", "ansatte"]
dfpath_2 = ["utdanning", "utdanning"]
dfpaths = [dfpath_1, dfpath_2]

getDF = getHFrames(hierarki_testdata, keepvar=["persid"], pathlists=dfpath_1)
getFrames = getHFrames(hierarki_testdata, keepvar=["persid"], pathlists=dfpaths)

orderdataschema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("farge", StringType(), True),
        StructField("enheter", IntegerType(), True),
    ]
)
ordereddata = [
    ("#001", "Blå", 10),
    ("#002", "Blå", 20),
    ("#003", "Blå", 15),
    ("#004", "Blå", None),
    ("#005", "Rød", 20),
    ("#006", "Rød", 22),
    ("#007", "Rød", 10),
    ("#008", "Gul", 20),
    ("#009", "Gul", None),
]
ordereddata_df = spark.createDataFrame(ordereddata, orderdataschema)

#### TESTER FUNKSJONER I SPARK TOOLS' UNDERMAPPER ####
#### SPARK TOOLS PROCESSING ####
# cross_sectional #
REFDATO = "2020-03-01 00:00:00"
referansedato = datetime.strptime(REFDATO, "%Y-%m-%d %H:%M:%S")

dato_hendelsedata = hierarki_testdata.withColumn(
    "dato", F.to_timestamp("dato", "ddMMMyyyy")
)
tverrsnitt = cross_sectional(dato_hendelsedata, "dato", ["persid"])
tverrsnitt_co = cross_sectional(
    dato_hendelsedata, "dato", ["persid"], coDate=referansedato
)


def test_cross_sectional_all():
    assert tverrsnitt.count() == 3


def test_cross_sectional_co():
    assert tverrsnitt_co.count() == 2


# getHFrames #
def test_getHFrames_isdf():
    assert isinstance(getDF, DataFrame)


def test_getHFrames_dfcols():
    assert all(
        elem in list(getDF.columns)
        for elem in ["persid", "ansatte_id", "navn", "adresse"]
    )


def test_getHFrames_dfdicts():
    assert isinstance(
        getHFrames(hierarki_testdata, keepvar=["persid"], pathlists=dfpaths), dict
    )


def test_getHFrames_elements():
    assert all(
        elem in list(getFrames.keys())
        for elem in ["utdanning_utdanning", "arbeidsgiver_ansatte"]
    )


# orderedgroup #

# Newer pyspark versions sort the partitions in Window.partitionBy() in ascending order.
# The tests below are updated to reflect this.
def test_orderedgroup_standard():
    assert [
        row[0] for row in orderedgroup(ordereddata_df, "farge", "enheter").collect()
    ] == ["#002", "#003", "#001", "#004", "#008", "#009", "#006", "#005", "#007"]


def test_orderedgroup_nullfirst():
    assert [
        row[0]
        for row in orderedgroup(
            ordereddata_df, "farge", "enheter", null_last=False
        ).collect()
    ] == ["#004", "#002", "#003", "#001", "#009", "#008", "#006", "#005", "#007"]


def test_orderedgroup_asc():
    assert [
        row[0]
        for row in orderedgroup(ordereddata_df, "farge", "enheter", asc=True).collect()
    ] == ["#001", "#003", "#002", "#004", "#008", "#009", "#007", "#005", "#006"]


# unpack_parquet #
test_dict = {}

test_dict = unpack_parquet(hierarki_testdata, spark_session=spark)
test_dict_less = unpack_parquet(hierarki_testdata, levels=1, spark_session=spark)


def test_unpack_parquet_return_dict():
    assert len(test_dict) != 0


def test_unpack_parquet_nodfs():
    assert len(test_dict.keys()) == 5


def test_unpack_parquet_nodfs_levels():
    assert len(test_dict_less.keys()) == 2


def test_unpack_parquet_child():
    assert (
        all(
            dfname in test_dict.keys()
            for dfname in [
                "utdanning",
                "utdanning_utdanning-child",
                "utdanning_utdanning-child_annet",
            ]
        )
        == True
    )


####  SPARK TOOLS EDITING  #####

# listcode_lookup
lookup_result = listcode_lookup(
    testdata,
    "varenummer",
    testdata_kodeliste,
    ["varenummer", "frukt"],
    spark_session=spark,
)


def test_listcode_lookup():
    assert [row["frukt"] for row in lookup_result.select("frukt").collect()][:5] == [
        "Eple",
        "Banan",
        "Kiwi",
        "Appelsin",
        None,
    ]


# missing_correction_bool
test_corrected_bool, missing_boolcount = missing_correction_bool(
    testdata, df_name="testdf", spark_session=spark
)


def test_bool_nocorrection():
    assert missing_boolcount.iat[0, 3] == 1


def test_bool_distribution():
    assert [row["boolvar"] for row in test_corrected_bool.select("boolvar").collect()][
        :4
    ] == [True, False, False, False]


# missing_correction_number
test_corrected_number, missing_numbcount = missing_correction_number(
    testdata, df_name="testdf", spark_session=spark
)


def test_numb_nocorrection():
    assert missing_numbcount.iat[0, 3] == 1


def test_numb_distribution():
    assert [
        row["numbvar"] for row in test_corrected_number.select("numbvar").collect()
    ][:4] == [1, 2, 0, 4]


# spark_missing_correction_bool
test_sparkbooldata_korrigert, spark_missing_boolcount = spark_missing_correction_bool(
    testdata, spark_session=spark
)


def test_sparkbool_nocorrection():
    assert spark_missing_boolcount.collect()[0][2] == 1


def test_sparkbool_distribution():
    assert [
        row["boolvar"]
        for row in test_sparkbooldata_korrigert.select("boolvar").collect()
    ][:4] == [True, False, False, False]


# spark_missing_correction_number
test_numbdata_korrigert, missing_numbcount = spark_missing_correction_number(
    testdata, spark_session=spark
)


def test_numb_nocorrection():
    assert missing_numbcount.collect()[0][2] == 1


def test_numb_distribution():
    assert [
        row["numbvar"] for row in test_numbdata_korrigert.select("numbvar").collect()
    ][:4] == [1, 2, 0, 4]


#### SPARK TOOLS CONTROLS ####

# listcode_check
kodeliste = testdata_kodeliste.select("varenummer")
sjekk, sjekkdf = listcode_check(testdata, "varenummer", kodeliste)
listcode_testdata = sjekkdf.groupBy("i_kodeliste").count()


def test_listcode_check():
    assert (listcode_testdata.collect()[0][1], listcode_testdata.collect()[1][1]) == (
        4,
        1,
    )


# compare_dimdf
def test_compare_dimdf_identical():
    assert compare_dimdf(testdata, testdata) == True


def test_compare_dimdf_necolumns():
    assert compare_dimdf(testdata, testdata.drop("numbvar")) == False


def test_compare_dimdf_nerows():
    assert (
        compare_dimdf(testdata, testdata.filter(F.col("identifikator") != "id4"))
        == False
    )


def test_compare_dimdf_necolumnsrows():
    assert (
        compare_dimdf(
            testdata, testdata.drop("numbvar").filter(F.col("identifikator") != "id4")
        )
        == False
    )


# compare_columns
def test_compare_columns_identical():
    assert compare_dimdf(testdata, testdata) == True


def test_compare_columns_necolumns():
    assert compare_dimdf(testdata, testdata.drop("numbvar")) == False


# compare_df
def test_compare_df_identical():
    assert compare_df(testdata, testdata) == True


def test_compare_df_ne():
    assert (
        compare_df(
            testdata,
            testdata.withColumn(
                "numbvar",
                F.when(F.col("identifikator") == "id3", F.lit(100)).otherwise(
                    F.col("numbvar")
                ),
            ),
        )
        == False
    )


####  SPARK TOOLS QUALITY  #####
test_missing_spark = spark_qual_missing(testdata, spark_session=spark)
test_missing_pd = missing_df(testdata, spark_session=spark)

# missing_df
def test_missing_df():
    assert test_missing_pd["missing"].count() > 0


def test_missing_variable():
    list(test_missing_pd.index[:4]) == [
        "identifikator",
        "numbvar",
        "boolvar",
        "varenummer",
    ]


def test_missing_missingvalues():
    list(test_missing_pd["missing"])[:4] == [0.0, 1.0, 1.0, 0.0]


def test_missing_shareoftot():
    list(test_missing_pd["shareoftotal"])[:4] == [0.0, 0.2, 0.2, 0.0]


# spark_qual_missing
def test_spark_qual_missing():
    assert test_missing_spark.count() > 0


def test_spark_qual_missing_variable():
    assert [row["variable"] for row in test_missing_spark.select("variable").collect()][
        :4
    ] == ["identifikator", "numbvar", "boolvar", "varenummer"]


def test_spark_qual_missing_missingvalues():
    assert [
        row["obs_missing"] for row in test_missing_spark.select("obs_missing").collect()
    ][:4] == [0, 1, 1, 0]


def test_spark_qual_missing_datatype():
    assert [
        row["datatype"] for row in test_missing_spark.select("datatype").collect()
    ] == ["StringType", "LongType", "BooleanType", "StringType"]


def test_spark_qual_missing_percentage():
    assert [
        row["percentage_missing"]
        for row in test_missing_spark.select("percentage_missing").collect()
    ] == [0.0, 20.0, 20.0, 0.0]
