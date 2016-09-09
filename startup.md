# Startup procedures

## Harvesting Planet Labs
Call http://localhost:8080/planet with the following parameters:
* dropIndex=true (optional but this is the way to start fresh if you have existing data already)
* PL_API_KEY=...
* pzGateway=http://pz-gateway.stage.geointservices.io
* Provide auth information for the Piazza Gateway in the header - you must authenticate for this process to work.

## Testing Discovery
Call http://localhost:8080/discover with one or more of the following:
* bbox = x1,y1,x2,y2
* acquiredDate (RFC 3339)
* cloudCover (0 to 100)


## Setting up reccurring harvests
