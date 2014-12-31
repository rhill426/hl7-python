#hl7-python
==========

Python HL7 library to edit, parse and send HL7 v2.x messages

Drop file into python 3.x library directory

##Usage:
```
import hl7

ib = hl7.listener(9999)
ob = hl7.sender('localhost',10000)

ib_status = ib.start()
ob_status = ob.start()

while True:
	raw = ib.getMsg()
	ib.remoteAddress()

	msg = hl7.parse(raw)

	msg['MSH']['MSH.3'] = 'TEST'

	out = hl7.toString(msg)

	ack = ob.send(out)
```
