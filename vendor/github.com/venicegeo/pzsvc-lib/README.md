# pzsvc-lib
A Go library designed to make it easier for external services to interact with Piazza.

Pulled out from the pzsvc-exec/pzsvc library on 14 July, 2016, to reflect the fact
that it's now a generic service library, rather than something purely intended to support pzsvc-exec.  Worth noting that it is in no way complete, or necessarily intended to be so.  The functions here were added as they were needed.  If you need a function that is not here, and you think it belong here, we are happy to consider pull requests, and will at least listen to requests of the other variety.  Split up into files as follows:

core.go: generic functions useful for many different kinds of Pz interactions, primarily focused around making http calls and interpreting the results.  If you're interacting with Pz using pzsvc-lib, you will have functions from this file in your call stack.

file.go: Functions useful for interacting with files - uploading them, downloading them, deploying them to geoserver, and so forth.

model.go: Useful structs.  Modeled off of the structs used inside of Pz itself (which are thus reflected in its JSON inputs and outputs).

service.go: functions about services - mostly managing service registrations, at this point, although this is also where functions about executing services go.

utils.go: small utility functions that don't inherently have anything to do with Pz or http calls at all