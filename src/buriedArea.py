from pyspark.sql import SQLContext, Row
from commonDatabase import CommonDatabase

from commonFunctions import remover_separator_filename_mode, get_molecule_name_from_filename_mode


class BuriedArea(CommonDatabase):

    """
    	load file that contains the residue list for Buried Area
    	sc - spark context
    	path_file_buried_area - path filename of all residues hbond
    """
    def load_file_all_residue_buried_area(self, sc, path_file_buried_area):
    	all_residue	= sc.textFile(path_file_buried_area)
    	header = all_residue.first() #extract header
    	#Spliting file by \t
    	all_residue_split = all_residue.filter(lambda x:x !=header).map(lambda line: line.split("\t"))
    	all_residue_split = all_residue_split.map(lambda p: Row( residue=str(p[0]), buried_area_residue=float(p[1]), residue_sasa_buried=float(p[2]), pose=str(p[3]) ))
    	return all_residue_split

    def save_buried_area_all_res_table(self, sqlctx, df):
    	table = self.client.get_keyspace() + "." + "buried_area_all_res"
    	buried_area_all_res = sqlctx.sql("select residue, buried_area_residue, residue_sasa_buried, pose from buried_area_all_resFILE")
    	buried_area_all_resRDD = buried_area_all_res.rdd.map(lambda p: Row( residue=str(p[0]), buried_area_residue=float(p[1]), residue_sasa_buried=float(p[2]), pose=str(p[3]) ))
    	for row in buried_area_all_resRDD.collect():
    		sql_query = """
    			INSERT INTO @$$$$$$$$$$$$@
    			(pose, buried_area_residue, residue, residue_sasa_buried)
    			VALUES (?, ?, ?, ?);
    		"""
    		sql_query = sql_query.replace("@$$$$$$$$$$$$@", table)
    		bound_statement = self.client.session.prepare(sql_query)
    		self.client.session.execute( bound_statement.bind((
    			row.pose,
    			row.buried_area_residue,
    			row.residue,
    			row.residue_sasa_buried
    			))
    		)


    def save_histogram_buried_area_all_res_receptor_molecule(self, separator_filename_mode, sqlctx, df):
		table = self.client.get_keyspace() + "." + "buried_area_all_res_histogram_recep_mol"
		buried_area_all_res = sqlctx.sql("select residue, buried_area_residue, residue_sasa_buried, pose from buried_area_all_resFILE")
		buried_area_all_resRDD = buried_area_all_res.rdd.map(lambda p: Row(recep_mol=remover_separator_filename_mode(separator_filename_mode, str(p[3]) ), residue=str(p[0]), buried_area_residue=float(p[1]), residue_sasa_buried=float(p[2]), pose=str(p[3]) ) )
		buried_area_all_res = sqlctx.createDataFrame(buried_area_all_resRDD)
		buried_area_all_res.createOrReplaceTempView("buried_area_all_res_histogram_recep_mol")
		buried_area_all_res = sqlctx.sql("select recep_mol, count(recep_mol) as number from buried_area_all_res_histogram_recep_mol group by recep_mol")
		for row in buried_area_all_res.collect():
			sql_query = """
				INSERT INTO @$$$$$$$$$$$$@
				(recep_mol, number)
				VALUES (?, ?);
			"""
			sql_query = sql_query.replace("@$$$$$$$$$$$$@", table)
			bound_statement = self.client.session.prepare(sql_query)
			self.client.session.execute( bound_statement.bind((
				row.recep_mol,
				row.number
				))
			)
