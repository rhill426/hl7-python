#! /usr/bin/python

#*******************************************************************************#
# HL7 library file to rapidly parse, receive, and send HL7 data 				#
# Developed in Python 3.4 														#
# Sept. 9th, 2014																#
#*******************************************************************************#

import socket
from re import compile, match

#-------------------------------------------------------------------------------#
# This function takes the message as a string and creates "msg" variable 		#
# in the form of a Python dictionary with HL7 fields as keys and field values   #
# as the values.  Repeating fields and segments are nested python lists         #
#-------------------------------------------------------------------------------#
def parse(raw):
	"""Turns message into Python Dictionary"""
	if raw == '':
		return False

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
	raw = raw.replace('\n','\r')
	ret = "\r"

	# Splitting Segments at the return character
	segments = raw.split(ret)
	
	# Looping over the segments
	for segment in segments:
		# Getting segment name
		seg = segment[0:3]

		if seg == '':
			continue

		# Adding segment name to segment list
		repSegList = []
		if seg in segList:
			# # Checking for repeating segments
			index = segList.index(seg)
			if isinstance(msg[segList[index]],list):
				repSegList = msg[segList[index]]
				msg[seg] = {}
			else:
				repSegList.append(msg[segList[index]])
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
			msg[seg]['MSH.1'] = fld
			fldCount = 2 			# We've already set MSH_1 so we start at 2
		else:
			fldCount = 1

		# Process segments
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
				elif sub in field and currFld != 'MSH.2':
					# Processing subfields w/o components
					comCount = 1
					subCount = 1
					currCom = seg+"."+str(fldCount)+"."+str(comCount)
					msg[seg][currFld][currCom] = {}
					subcomponents = field.split(sub)
					for subcomponent in subcomponents:
						currSub = seg+"."+str(fldCount)+"."+str(comCount)+"."+str(subCount)
						msg[seg][currFld][currCom][currSub] = {}
						msg[seg][currFld][currCom][currSub] = subcomponent

						subCount += 1 	# Incrementing Sub-Componenet Count
				else:
					msg[seg][currFld] = field

			index = fields.index(field)
			fields[index] = currFld

			fldCount += 1 	# Incrementing Field Count Variable	

		fieldStr = fld.join(fields)
		structureStr = '|'.join(fields)

		# Adding return character to structure string
		structure += structureStr + ret

		if repSegList:
			repSegList.append(msg[seg])
			msg[seg] = repSegList

	# Adding structure string to dictionary
	msg['structure'] = structure

	# Adding a copy of the original message
	msg['raw'] = raw

	# Returning a list of segments
	msg['segments'] = segList
	
	# Useful for scripting to kill or error message
	msg['status'] = ''

	# Returning short-cuts to useful fields
	msg['msg_date'] = msg['MSH']['MSH.7']
	msg['msg_type'] = msg['MSH']['MSH.9']['MSH.9.1']
	msg['msg_event'] = msg['MSH']['MSH.9']['MSH.9.2']
	msg['msg_id'] = msg['MSH']['MSH.10']
	msg['msg_version'] = msg['MSH']['MSH.12']

	# Returning dictionary
	return msg

#-------------------------------------------------------------------------------#
# Function takes the python dictionary from the "parse" function and turns it   #
# back into a string in the formatted HL7 			                         	#
#-------------------------------------------------------------------------------#
def toString(msg):
	"""Combining Dictionary into HL7 message"""
	if msg == '':
		return False

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
	ret = "\r"

	segList = []	# list of repeating segments so we don't go over them twice

	seg_dict = {}	# Keeps count of segments in a dictionary

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

			if seg in segList:
				t = int(seg_dict[seg])
				t += 1
				seg_dict[seg] = t
			else:
				seg_dict[seg] = t
				segList.append(seg)

			# Adding segment name to beginning of string
			outMsg += seg[0:3]
			
			# Field iterator
			i = 1
			while i < len(fields):
				if isinstance(msg[fields[0]][seg_dict[seg]][fields[i]],list):
					# If field is a list/repeating field, we keep parsing
					repetitions = []
					x = 0
					for repetition in msg[fields[0]][seg_dict[seg]][fields[i]]:
						repList = []
						if isinstance(repetition,dict):
							# If it is a dictionary then we keep parsing the sub-components
							for c in order(repetition,comRegEx):
								if isinstance(msg[fields[0]][seg_dict[seg]][fields[i]][x][c],dict):
									# Component contains sub-component
									subList = []
									for s in order(msg[fields[0]][seg_dict[seg]][fields[i]][x][c],subRegEx):
										subList.append(msg[fields[0]][seg_dict[seg]][fields[i]][x][c][s])
									repList.append(sub.join(subList))
								else:
									# Appending to field repitition list
									repList.append(msg[fields[0]][seg_dict[seg]][fields[i]][x][c])
						else:
							# No subfields in repetition
							repList.append(msg[fields[0]][seg_dict[seg]][fields[i]][x])
						x += 1
						repList = com.join(repList)
						repetitions.append(repList)

					# Adding the repeating field string to the out message with the repetition character
					outMsg += fld + rep.join(repetitions)

				else:
					# Non repeating field
					if isinstance(msg[fields[0]][seg_dict[seg]][fields[i]],dict):
						# Contains components
						comList = []
						for c in order(msg[fields[0]][seg_dict[seg]][fields[i]],comRegEx):
							if isinstance(msg[fields[0]][seg_dict[seg]][fields[i]][c],dict):
								# Contains sub-components
								subList = []
								for s in order(msg[fields[0]][seg_dict[seg]][fields[i]][c],subRegEx):
									subList.append(msg[fields[0]][seg_dict[seg]][fields[i]][c][s])
								comList.append(sub.join(subList))
							else:
								comList.append(msg[fields[0]][seg_dict[seg]][fields[i]][c])
						outMsg += fld + com.join(comList)
					else:
						# Field without components or sub-components
						outMsg += fld + str(msg[fields[0]][seg_dict[seg]][fields[i]])

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
# Utility functions for scripting Python Dictionary of HL7 fieldS 				#
#-------------------------------------------------------------------------------#
def rep(dict):
	"""Takes a dictionary/HL7 entry and returns boolian if its repeatable"""
	if isinstance(dict,list):
		return True
	else:
		return False

#-------------------------------------------------------------------------------#
# Class for inbound TCP functions												#
# ***Note: When sending to this port the sender must specify the hostname or IP #
# "localhost" or 127.0.0.1 will not work on the local machine                   #
#-------------------------------------------------------------------------------#
class listener:
	largeMsg = []	# A variable to hold large messages
	ackFlag = True

	def __init__(self,port):
		# Initializes connection object
		self.port = port

	def start(self):
		# Initializes and creates socket
		ib = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		ib.settimeout(.01)

		# Binding to address and port
		host = socket.gethostname() # Localhostname
		ib.bind((host,self.port))

		# Starts listener
		ib.listen(1)

		self.ib = ib

		def startListener():
			while True:
				# Connecting to client
				addr = False
				try:
					conn, addr = ib.accept()
				except socket.timeout:
					pass
				if addr:
					# This is the remote IP and port
					self.address = addr
					self.conn = conn
				try:
					data = conn.recv(65536)
				except:
					continue
				if data:	
					if b'\x1c' not in data:
						# Waiting on the end of the message in case its a long message
						listener.largeMsg.append(data)  	# Converting from byte to string
						continue							# Keep listening for rest of message
					if listener.largeMsg:
						listener.largeMsg.append(data)		# Add the last section
						data = b''.join(listener.largeMsg)
						listener.largeMsg = []

					# Stripping Vertical Tab and File Separators
					data = data.replace(b'\x0b', b'') # Vertical Tab
					data = data.replace(b'\x1c', b'') # File Separator
					data = data.decode('utf-8')       # Converting from byte to string

					# ACK or NACK back
					if listener.ackFlag:
						ACK = self.ack(data,'AA')

					# This should be the received HL7 message
					yield data

		self.generator = startListener()

	def ack(self,raw,status):
		"""Creates AA,AE or AR ACK message and returns it to sender"""
		# First we parse the message
		ACK = ""
			
		# Get the field separator from MSH-1
		fld = raw[3:4]
		com = raw[4:5]

		# Finding the newline or return character
		if "\n" in raw:
			ret = "\n"
		else:
			ret = "\r"

		# Splitting segments
		segments = raw.split(ret)
		# Splitting MSH fields
		fields = segments[0].split(fld)
		i = 0
		MSH = ""
		while i < 12:
			if i == 8:
				# Changing MSH-9-1
				coms = fields[i].split(com)
				coms[0] = 'ACK'
				fields[i] = com.join(coms) 
			MSH += fields[i] + fld
			i += 1
		MSH = MSH[0:len(MSH) - 1]# Trimming last field character
		# Combining MSH segment with MSA segment
		# MSA|AA or AE or AR|MSH-10 value
		ACK = MSH + ret + "MSA" + fld + status + fld + fields[9] + ret
	        
		# Wraps message and sends outbound
		SB = '\x0b'  # <SB>, vertical tab
		EB = '\x1c'  # <EB>, file separator
		CR = '\x0d'  # <CR>, \r
		FF = '\x0c'  # <FF>, new page form feed
			
		# wrap in MLLP message container
		data = SB + ACK + EB + CR
		data = bytes(data, "utf-8")

		# Sending ACK back on same connection
		self.conn.send(data)

		# Returning ACK to use if they do it directly
		return data

	def stop(self):
		# Stops the listener
		try:
			self.ib.close()
			status = True
		except:
			status = False

		return status

	def getMsg(self):
		# Getting message from listener
		return next(self.generator)

	def remoteAddress(self):
		# Prints remote address that is connected
		if self.address:
			print(self.address)

	def sendAck(self,boolian):
		if not boolian:
			listener.ackFlag = False
		else:
			listener.ackFlag = True

#-------------------------------------------------------------------------------#
# Class for outbound TCP functions						                 		#
#-------------------------------------------------------------------------------#
class sender:
	ackFlag = True
	def __init__(self,host,port):
		# Initializes connection object
		self.host = host
		self.port = port
		
		# Initializes and creates socket
		cnxn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.cnxn = cnxn

	def start(self):
		# Connects to remote host
		try:
			self.cnxn.connect((self.host, self.port))
			status = True
		except:
			status = False

		return status

	def stop(self):
		# Stops the connection
		try:
			self.cnxn.close()
			status = True
		except:
			status = False

		return status

	def send(self,message):
		# Sends data to outbound TCP connection

		# Wraps message and sends outbound
		SB = '\x0b'  # <SB>, vertical tab
		EB = '\x1c'  # <EB>, file separator
		CR = '\x0d'  # <CR>, \r
		FF = '\x0c'  # <FF>, new page form feed
		
		# Wrap in MLLP message container and converts to bytes
		MLLP = SB + message + EB + CR
		msg = bytes(MLLP, "utf-8")
		
		# Sending message
		try:
			self.cnxn.send(msg)
		except:
			return False

		if sender.ackFlag:
			# Storing the ACK
			RECV_BUFFER = 4096
			ACK = self.cnxn.recv(RECV_BUFFER)
			ACK = ACK.replace(b"\x0b", b"") # Vertical Tab
			ACK = ACK.replace(b"\x1c", b"") # File Separator

			# Returning ACK string
			return ACK.decode()

	def expectAck(self,boolian):
		if not boolian:
			sender.ackFlag = False
		else:
			sender.ackFlag = True