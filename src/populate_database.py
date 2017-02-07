
from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.sql import SQLContext

import os, sys
from load_config import load_config
from hydrogen import Hydrogen

def main():
    #Loading config file from json
    conf_file = load_config(sys.argv[1])

    vsId = conf_file["virtualScreeningData"]["vsId"]
    hydrogen_fileName = os.path.join(conf_file["drugdesignAnalysisFiles"]["rootPathAnalysis"], conf_file["drugdesignAnalysisFiles"]["hydrogenAllRes"])
    buried_area_fileName = os.path.join(conf_file["drugdesignAnalysisFiles"]["rootPathAnalysis"], conf_file["drugdesignAnalysisFiles"]["hydrogenAllRes"])
    keyspace = conf_file["cassandraDB"]["keyspace"]
    nodeIP = conf_file["cassandraDB"]["nodeIP"]

    sc = SparkContext()
    sql = SQLContext(sc)

    # Creating Hydrogen object
    hydrogen = Hydrogen()
    hydrogen.set_cassandra_client(nodeIP, keyspace)

    #Reading hydrogen all residue file and converting it as dataframe
    df_hydrogen_all_res = sql.createDataFrame( hydrogen.load_file_all_residue_hbonds(sc, hydrogen_fileName) )
    df_hydrogen_all_res.createOrReplaceTempView("hydrogen_all_resFILE")


    #Saving hydrogen_all_res Cassandra Table
    hydrogen.save_hydrogen_all_res_table(sql, df_hydrogen_all_res)

    #Getting hydrogenAllRes Cassandra Table
    hydrogenAllRes = hydrogen.get_hydrogen_all_res_table(sql)
    hydrogenAllRes.show()

    #Closing database connection
    hydrogen.close_connection()

main()
