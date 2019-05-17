class Request:
	
	def __init__(self, queue_name, timestamp):
		self.queue_name = queue_name
		self.timestamp = timestamp

	# we need to implement this method otherwise we will get this error:
	# TypeError: '<' not supported between instances of 'BasicProperties'
	def __eq__(self, other):
		return self.timestamp == other.timestamp

	def __lt__(self, other):
		return self.timestamp < other.timestamp