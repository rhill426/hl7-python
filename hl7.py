#! /usr/bin/python

#-------------------------------------------------------------------------------#
# This function takes the message as a string and creates "msg" variable 		#
# in the form of a Python dictionary with HL7 fields as keys and field values   #
# as the values.  Repeating fields and segments are nested python lists         #
#-------------------------------------------------------------------------------#
def parse(raw):
	"""Turns message into Dictionary"""
	# This will be the returned parsed message dictionary
	msg = {}

	# Metadata
	structure = ""		# Since dictionary loses our order, we maintain in structure string
	segList = []		# List of message segments
	
	# Getting encoding characters from MSH-1 & MSH-2
	fld = raw[3:4]
	com = raw[4:5]
	rep = raw[5:6]
	esc = raw[6:7]
	sub = raw[7:8]

	# Finding the newline or return character
	if "\n" in raw:
		ret = "\n"
	else:
		ret = "\r"

	# Splitting Segments at the return character
	segments = raw.split(ret)

	# Starting a repeating segment list
	repSegList = []
	
	# Looping over the segments
	for segment in segments:
		# Getting segment name
		seg = segment[0:3]

		if seg == '':
			continue

		# Adding segment name to segment list
		if seg in segList:
			# Checking for repeating segments
			repSegList.append(msg[seg])
			msg[seg] = {}
		else:
			# Creating segment dictionary
			msg[seg] = {}
			# Adding unique segment to list
			segList.append(seg)
			# Setting this back to false
			repSegList = []
		
		# Trimming segment name off
		segment = segment[4:]

		# Adding segment to structure string
		structure += seg + '|'
		
		# Splitting into fields and assigning to msg dictionary
		fields = segment.split(fld)

		# Looping over fields
		if seg == 'MSH':
			msg[seg]['MSH.1'] = fld # Hard Coding MSH-1
			fldCount = 2 			# We've already set MSH_1 so we start at 2
		else:
			fldCount = 1

		# Process non-repeating segments
		for field in fields:
			# This is the current key name field
			currFld = seg+"."+str(fldCount)

			# If field is repeating we loop over repetitions
			if rep in field and currFld != 'MSH.2':
				field_list = []		# Starting a field list for the repetitions

				repetitions = field.split(rep)

				for repetition in repetitions:
					msg[seg][currFld] = {}

					# Looping over components
					if com in repetition and currFld != 'MSH.2':
						comCount = 1
						components = repetition.split(com)

						for component in components:
							currCom = seg+"."+str(fldCount)+"."+str(comCount)

							msg[seg][currFld][currCom] = {}

							# Looping over sub-components
							if sub in component:
								subCount = 1
								subcomponents = component.split(sub)
								for subcomponent in subcomponents:
									currSub = seg+"."+str(fldCount)+"."+str(comCount)+"."+str(subCount)
									msg[seg][currFld][currCom][currSub] = {}
									msg[seg][currFld][currCom][currSub] = subcomponent

									subCount += 1 	# Incrementing Sub-Componenet Count
							else:
								msg[seg][currFld][currCom] = component

							comCount += 1 	# Incrementing Component Count
					else:
						msg[seg][currFld] = repetition
						
					# Appending field to list
					field_list.append(msg[seg][currFld])

				# Setting the field to a list format	
				msg[seg][currFld] = field_list	

			else:
				msg[seg][currFld] = {}

				# Looping over components
				if com in field and currFld != 'MSH.2':
					comCount = 1
					components = field.split(com)

					for component in components:
						currCom = seg+"."+str(fldCount)+"."+str(comCount)

						msg[seg][currFld][currCom] = {}

						# Looping over sub-components
						if sub in component:
							subCount = 1
							subcomponents = component.split(sub)
							for subcomponent in subcomponents:
								currSub = seg+"."+str(fldCount)+"."+str(comCount)+"."+str(subCount)
								msg[seg][currFld][currCom][currSub] = {}
								msg[seg][currFld][currCom][currSub] = subcomponent

								subCount += 1 	# Incrementing Sub-Componenet Count
						else:
							msg[seg][currFld][currCom] = component

						comCount += 1 	# Incrementing Component Count
				else:
					msg[seg][currFld] = field

			index = fields.index(field)
			fields[index] = currFld

			fldCount += 1 	# Incrementing Field Count Variable	

		fieldStr = fld.join(fields)

		# Adding return character to structure string
		structure += fieldStr + ret


	if repSegList:
		repSegList.append(msg[seg])
		msg[seg] = repSegList

	# Adding structure string to dictionary
	msg['structure'] = structure

	# Adding a copy of the original message
	msg['raw'] = raw

	# Returning dictionary
	return msg

#-------------------------------------------------------------------------------#
# Function takes the python dictionary from the "parse" function and turns it   #
# back into a string in the formatted HL7 			                         	#
#-------------------------------------------------------------------------------#
def sethl7(msg):
	"""Combining Dictionary into HL7 message"""
	from re import compile, match

	# Setting some regex patterns
	fieldRegEx = compile('[A-Z0-9]{3}.([0-9]+)')
	comRegEx = compile('[A-Z0-9]{3}.[0-9]+.([0-9]+)')
	subRegEx = compile('[A-Z0-9]{3}.[0-9]+.[0-9]+.([0-9]+)')
	def order(d,regex):
		"""Function takes dictionary of hl7 field names and orders them"""
		ordered = {}
		for k in d:
			n = match(regex,k).group(1)
			ordered[int(n)] = k
		l = []
		for k in sorted(ordered):
			l.append(ordered[k])
		return l

	# This is the message we will build
	outMsg = ''
	
	# Getting encoding characters
	fld = msg['MSH']['MSH.1']
	com = msg['MSH']['MSH.2'][0:1]
	rep = msg['MSH']['MSH.2'][1:2]
	esc = msg['MSH']['MSH.2'][2:3]
	sub = msg['MSH']['MSH.2'][3:4]

	# Finding the newline or return character
	if "\n" in msg['structure']:
		ret = "\n"
	else:
		ret = "\r"

	repsegs = []	# list of repeating segments so we don't go over them twice

	segments = msg['structure'].split(ret)

	for seg in segments:
		# Skipping blanks
		if seg[0:3] == '':
			continue

		# Splitting segment into fields
		fields = seg.split('|')

		if isinstance(msg[fields[0]],list):
			# This is a repeating segment

			# Repeating segment iteration
			t = 0

			if seg in repsegs:
				continue	# We don't want to repeat this
			else:
				repsegs.append(seg)

			for r in msg[fields[0]]:
				# Adding segment name to beginning of string
				outMsg += seg[0:3]
				
				# Field iterator
				i = 1
				while i < len(fields):
					if isinstance(msg[fields[0]][t][fields[i]],list):
						# If field is a list/repeating field, we keep parsing
						repetitions = []
						x = 0
						for repetition in msg[fields[0]][t][fields[i]]:
							repList = []
							if isinstance(repetition,dict):
								# If it is a dictionary then we keep parsing the sub-components
								for c in order(repetition,comRegEx):
									if isinstance(msg[fields[0]][t][fields[i]][x][c],dict):
										# Component contains sub-component
										subList = []
										for s in order(msg[fields[0]][t][fields[i]][x][c],subRegEx):
											subList.append(msg[fields[0]][t][fields[i]][x][c][s])
										repList.append(sub.join(subList))
									else:
										# Appending to field repitition list
										repList.append(msg[fields[0]][t][fields[i]][x][c])
							else:
								# No subfields in repetition
								repList.append(msg[fields[0]][t][fields[i]][x])
							x += 1
							repList = com.join(repList)
							repetitions.append(repList)

						# Adding the repeating field string to the out message with the repetition character
						outMsg += fld + rep.join(repetitions)

					else:
						# Non repeating field
						if isinstance(msg[fields[0]][t][fields[i]],dict):
							# Contains components
							comList = []
							for c in order(msg[fields[0]][t][fields[i]],comRegEx):
								if isinstance(msg[fields[0]][t][fields[i]][c],dict):
									# Contains sub-components
									subList = []
									for s in order(msg[fields[0]][t][fields[i]][c],subRegEx):
										subList.append(msg[fields[0]][t][fields[i]][c][s])
									comList.append(sub.join(subList))
								else:
									comList.append(msg[fields[0]][t][fields[i]][c])
							outMsg += fld + com.join(comList)
						else:
							# Field without components or sub-components
							outMsg += fld + str(msg[fields[0]][t][fields[i]])

					# Incrementing count
					i += 1

				# Incrementing segment list count
				t += 1

				# Adding return character back on
				outMsg += ret

		else:
			# Non repeating segment
			
			# Adding segment name to beginning of string
			outMsg += seg[0:3]

			# field iterator
			i = 1
			while i < len(fields):
				if isinstance(msg[fields[0]][fields[i]],list):
					# If field is a list/repeating field, we keep parsing
					repetitions = []
					x = 0
					for repetition in msg[fields[0]][fields[i]]:
						repList = []
						if isinstance(repetition,dict):
							# If it is a dictionary then we keep parsing the sub-components
							for c in order(repetition,comRegEx):
								if isinstance(msg[fields[0]][fields[i]][x][c],dict):
									# Component contains sub-component
									subList = []
									for s in order(msg[fields[0]][fields[i]][x][c],subRegEx):
										subList.append(msg[fields[0]][fields[i]][x][c][s])
									repList.append(sub.join(subList))
								else:
									# Appending to field repitition list
									repList.append(msg[fields[0]][fields[i]][x][c])
						else:
							# No subfields in repetition
							repList.append(msg[fields[0]][fields[i]][x])
						x += 1
						repList = com.join(repList)
						repetitions.append(repList)

					# Adding the repeating field string to the out message with the repetition character
					outMsg += fld + rep.join(repetitions)

				else:
					# Non repeating field
					if isinstance(msg[fields[0]][fields[i]],dict):
						# Contains components
						comList = []
						for c in order(msg[fields[0]][fields[i]],comRegEx):
							if isinstance(msg[fields[0]][fields[i]][c],dict):
								# Contains sub-components
								subList = []
								for s in order(msg[fields[0]][fields[i]][c],subRegEx):
									subList.append(msg[fields[0]][fields[i]][c][s])
								comList.append(sub.join(subList))
							else:
								comList.append(msg[fields[0]][fields[i]][c])
						outMsg += fld + com.join(comList)
					else:
						# Field without components or sub-components
						outMsg += fld + str(msg[fields[0]][fields[i]])

				# Incrementing count
				i += 1

			# Adding return character back on
			outMsg += ret

	# Finished message
	return outMsg

#-------------------------------------------------------------------------------#
# Function listens on a port on the local machine and returns the data received #
# Format: tcplisten([Port Number])                                             	#
# ***Note: When sending to this port the sender must specify the hostname       #
# "localhost" or 127.0.0.1 will not work on the local machine                   #
#-------------------------------------------------------------------------------#
def tcplisten(port):
	"""Listens on port and returns the data"""
	import socket
	# Creating Socket
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	s.settimeout(.01)

	# Binding to address and port
	host = socket.gethostname() # Localhostname
	s.bind((host, port))

	# Listening
	s.listen(1)

	while True:
		# Connecting to client
		try:
			conn, addr = s.accept()
		except socket.timeout:
			pass
		# if addr:
		# 	# This is the remote IP and port
		# 	ip = addr[0]
		# 	port = addr[1]
		try:
			data = conn.recv(65536)
		except:
			continue
		if data:	
			# Stripping Vertical Tab and File Separators
			data = data.replace(b'\x0b', b'') # Vertical Tab
			data = data.replace(b'\x1c', b'') # File Separator
			data = data.decode('utf-8')       # Converting from byte to string

			# ACK or NACK back
			ACK = ack(data,'AA')
			conn.send(ACK)

			# This should be the received HL7 message
			yield data
			

#-------------------------------------------------------------------------------#
# Function generates ACK message from the original message with AA,AE,AR   		#
# Format: ack(msg,status)    				    								#
#-------------------------------------------------------------------------------#
def ack(raw,status):
	"""Creates AA,AE or AR ACK message and returns it to sender"""
	# First we parse the message
	ACK = ""
		
	# Get the field separator from MSH-1
	fld = raw[3:4]

	# Finding the newline or return character
	if "\n" in raw:
		ret = "\n"
	else:
		ret = "\r"

	# Splitting segments
	segments = raw.split(ret)
	# Splitting MSH fields
	fields = segments[0].split(fld)
	# Combining MSH segment with MSA segment
	# MSA|AA or AE or AR|MSH-10 value
	ACK = segments[0] + ret + "MSA" + fld + status + fld + fields[9] + ret
        
	# Wraps message and sends outbound
	SB = '\x0b'  # <SB>, vertical tab
	EB = '\x1c'  # <EB>, file separator
	CR = '\x0d'  # <CR>, \r
	FF = '\x0c'  # <FF>, new page form feed
		
	# wrap in MLLP message container
	data = SB + ACK + EB + CR
	data = bytes(data, "utf-8")

	return data

#-------------------------------------------------------------------------------#
# Function sends to a remote or local Server and port message                   #
# Format: tcpsend([Host Name], [Port Number], [HL7 message string])            	#
#-------------------------------------------------------------------------------#
def tcpsend(host,port,message):
	"""Sends data to outbound TCP connection"""
	import socket
	# Initializes and connects socket
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((host, port))

	# Wraps message and sends outbound
	SB = '\x0b'  # <SB>, vertical tab
	EB = '\x1c'  # <EB>, file separator
	CR = '\x0d'  # <CR>, \r
	FF = '\x0c'  # <FF>, new page form feed
	
	# Wrap in MLLP message container
	data = SB + message + EB + CR
	data = bytes(data, "utf-8")
	
	# Sending message
	s.send(data)

	# Storing the ACK
	RECV_BUFFER = 4096
	ACK = s.recv(RECV_BUFFER)
	ACK = ACK.replace(b"\x0b", b"") # Vertical Tab
	ACK = ACK.replace(b"\x1c", b"") # File Separator

	return ACK.decode()
	
	# Closing Socket
	s.close()

#-------------------------------------------------------------------------------#
# This function orders the randomly ordered dictionary entries so that 			#
# They are processed in HL7 field/component,sub-component order 				#
# Takes arguments of the unordered dictionary and regex pattern depending on 	#
# Whether it is a field, component, sub-component and returns a list of the 	#
# dictionary keys in order 														#
#-------------------------------------------------------------------------------#
