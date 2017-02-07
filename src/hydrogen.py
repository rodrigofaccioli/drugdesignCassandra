from pyspark.sql import SQLContext, Row
from commonDatabase import CommonDatabase


class Hydrogen(CommonDatabase):

	"""
		load file that contains the residue list for Hydrogen Bond
		sc - spark context
		path_file_hydrogen_bond - path filename of all residues hbond
	"""
	def load_file_all_residue_hbonds(self, sc, path_file_hydrogen_bond):
		all_residue	= sc.textFile(path_file_hydrogen_bond)
		header = all_residue.first() #extract header

		#Spliting file by \t
		all_residue_split = all_residue.filter(lambda x:x !=header).map(lambda line: line.split("\t"))
		all_residue_split = all_residue_split.map(lambda p: Row( ligand_atom=str(p[0]), accept_or_donate=str(p[1]), receptor_residue=str(p[2]), receptor_atom=str(p[3]), distance=float(p[4]), angle=float(p[5]), pose=str(p[6]) ))
		return all_residue_split

	def get_hydrogen_all_res_table(self, sql):
		return sql.read.format("org.apache.spark.sql.cassandra").options(keyspace=self.client.get_keyspace(), table="hydrogen_all_res").load()

	def save_hydrogen_all_res_table(self, sqlctx, df):
		table = self.client.get_keyspace() + "." + "hydrogen_all_res"
		hydrogen_all_res = sqlctx.sql("select ligand_atom, accept_or_donate, receptor_residue, receptor_atom, distance, angle, pose from hydrogen_all_resFILE")
		hydrogen_all_resRDD = hydrogen_all_res.rdd.map(lambda p: Row( ligand_atom=str(p[0]), accept_or_donate=str(p[1]), receptor_residue=str(p[2]), receptor_atom=str(p[3]), distance=float(p[4]), angle=float(p[5]), pose=str(p[6]) ))
		for row in hydrogen_all_resRDD.collect():
			sql_query = """
				INSERT INTO @$$$$$$$$$$$$@
				(pose, accept_or_donate, angle, distance, ligand_atom, receptor_atom, receptor_residue)
				VALUES (?, ?, ?, ?, ?, ?, ?);
			"""
			sql_query = sql_query.replace("@$$$$$$$$$$$$@", table)
			bound_statement = self.client.session.prepare(sql_query)
			self.client.session.execute( bound_statement.bind((
				row.pose,
				row.accept_or_donate,
				row.angle,
				row.distance,
				row.ligand_atom,
				row.receptor_atom,
				row.receptor_residue
				))
			)
