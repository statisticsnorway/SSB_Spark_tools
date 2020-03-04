import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark import Row

def skriv_log(navn, ds, lagring):
    global logKlar, pssteg
    tmpLog = logKlar.filter(F.col('log').isin(navn) == False)
    newRow = spark.createDataFrame([(navn, pssteg, ds, lagring)], schema)
    logKlar = tmpLog.union(newRow)


def print_log(navn):
    logtekst = logKlar.filter(F.col('log') == navn).collect()[0]
    print ("%html")
    print(logtekst['logdata'])


def listut_logger():
    # Last inn logdatasett
    a = list(logKlar.select("log").toPandas()['log'])
    print("%html <h3>Logger registrert i logdatasett: </h3><ul>")
    for key in a:
        print("<li>", key, "</li>")
    print("</ul>")


def formatAvledetLog(skatteobjekt, ikkenye):
    tmp_log = "<h4>Avledninger fra skatteobjekt <strong>{}</strong> </h4>".format(skatteobjekt)
    nye = False
    for key in so_dict[skatteobjekt].columns:
        if key not in ikkenye:
            nye = True
            tmp_log = tmp_log + "<li>{}</li>".format(key)
    tmp_log = tmp_log + "</ul>"
    if not nye:
        return ''
    else:
        return tmp_log


def logkobling_avledning(innds, resultatds):
    koblelog = "<h4>{}</h4>".format(resultatds)
    koblelog = koblelog + "<strong>Datasett som kobles:</strong><ul>"
    for ds in innds:
        koblelog = koblelog + "<li>{} (ant. observasjoner: {})</li>".format(ds, so_dict[ds].count())
    koblelog = koblelog + "</ul><strong>Resultat: {} med {} observasjoner</strong><ul>".format(resultatds, so_dict[
        resultatds].count())
    for ds in innds:
        koblelog = koblelog + "<li> Fra {} ble det koblet {} observasjoner</li>".format(ds, so_dict[ds].filter(
            so_dict[ds].personidentifikator.isin(so_dict[resultatds]['personidentifikator'])).count())
    koblelog = koblelog + "</ul>"
    return koblelog