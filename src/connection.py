from cassandra.cluster import Cluster

"""
  Function to create a connection object
"""
def create_connection(nodeIP, keyspaceName):        
    client = Connection()
    client.connect([nodeIP])
    client.set_keyspace(keyspaceName)
    return client


class Connection:
	session = None
	keyspace = ""

	def connect(self, nodes):
		cluster = Cluster(nodes)
		metadata = cluster.metadata
		self.session = cluster.connect()

	def get_session():
		return self.session

	def set_keyspace(self, keyspace):
		self.keyspace = keyspace

	def get_keyspace(self):
		return self.keyspace

	def close(self):
		self.session.cluster.shutdown()
		self.session.shutdown()

	def query_schema(self, table):
		table_keyspace = self.keyspace+"."+table
		query = " SELECT * FROM "+ table_keyspace +";"
		result_set = self.session.execute(query)
		return result_set
