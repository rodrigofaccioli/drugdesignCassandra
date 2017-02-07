
from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.sql import SQLContext

import os, sys
from loadConfig import load_config
from hydrogen import Hydrogen
from buriedArea import BuriedArea

def main():
    #Loading config file from json
    conf_file = load_config(sys.argv[1])

    vsId = conf_file["virtualScreeningData"]["vsId"]
    separator_filename_mode = conf_file["virtualScreeningData"]["separatorFilenameMode"]
    separator_receptor = conf_file["virtualScreeningData"]["separatorReceptor"]
    drugdesignCassandra_source_path = conf_file["drugdesignCassandra"]["sourcePath"]
    hydrogen_fileName = os.path.join(conf_file["drugdesignAnalysisFiles"]["rootPathAnalysis"], conf_file["drugdesignAnalysisFiles"]["hydrogenAllRes"])
    buried_area_fileName = os.path.join(conf_file["drugdesignAnalysisFiles"]["rootPathAnalysis"], conf_file["drugdesignAnalysisFiles"]["buriedAreasAllRes"])
    keyspace = conf_file["cassandraDB"]["keyspace"]
    nodeIP = conf_file["cassandraDB"]["nodeIP"]

    sc = SparkContext()
    sql = SQLContext(sc)

    sc.addPyFile(os.path.join(drugdesignCassandra_source_path,"commonFunctions.py"))
    sc.addPyFile(os.path.join(drugdesignCassandra_source_path,"connection.py"))
    sc.addPyFile(os.path.join(drugdesignCassandra_source_path,"commonDatabase.py"))
    sc.addPyFile(os.path.join(drugdesignCassandra_source_path,"hydrogen.py"))
    sc.addPyFile(os.path.join(drugdesignCassandra_source_path,"buriedArea.py"))

    # Creating Hydrogen object
    hydrogen = Hydrogen()
    hydrogen.set_cassandra_client(nodeIP, keyspace)

    #Reading hydrogen all residue file and converting it as dataframe
    df_hydrogen_all_res = sql.createDataFrame( hydrogen.load_file_all_residue_hbonds(sc, hydrogen_fileName) )
    df_hydrogen_all_res.createOrReplaceTempView("hydrogen_all_resFILE")

    #Saving hydrogen_all_res Cassandra Table
    hydrogen.save_hydrogen_all_res_table(sql, df_hydrogen_all_res)

    #Getting hydrogenAllRes Cassandra Table
    #hydrogenAllRes = hydrogen.get_hydrogen_all_res_table(sql)
    #hydrogenAllRes.createOrReplaceTempView("hydrogen_all_res")

    #Creating Histogram of hydrogen_all_res based on receptor_molecule
    hydrogen.save_histogram_hydrogen_all_res_receptor_molecule(separator_filename_mode, sql, df_hydrogen_all_res)

    #Creating Histogram of hydrogen_all_res based on molecule
    hydrogen.save_histogram_hydrogen_all_res_molecule(separator_receptor, separator_filename_mode, sql, df_hydrogen_all_res)

    #Closing database connection
    hydrogen.close_connection()
#############################################################################################

    # Creating Buried Area object
    buried_area = BuriedArea()
    buried_area.set_cassandra_client(nodeIP, keyspace)

    #Reading hydrogen all residue file and converting it as dataframe
    df_buried_area_all_res = sql.createDataFrame( buried_area.load_file_all_residue_buried_area(sc, buried_area_fileName) )
    df_buried_area_all_res.createOrReplaceTempView("buried_area_all_resFILE")

    #Saving buried_area_all_res Cassandra Table
    buried_area.save_buried_area_all_res_table(sql, df_buried_area_all_res)

    #Creating Histogram of hydrogen_all_res based on receptor_molecule
    buried_area.save_histogram_buried_area_all_res_receptor_molecule(separator_filename_mode, sql, df_buried_area_all_res)

    #Creating Histogram of buried_area_all_res based on molecule
    buried_area.save_histogram_buried_area_all_res_molecule(separator_receptor, separator_filename_mode, sql, df_buried_area_all_res)

    #Closing database connection
    buried_area.close_connection()

main()
