# Startup procedures
replace localhost:8080 to wherever this application is deployed (e.g., http://pzsvc-image-catalog.stage.geointservices.io/)

## Harvesting Planet Labs
Call http://localhost:8080/planet with the following parameters:
* dropIndex=true (optional but this is the way to start fresh if you have existing data already)
* PL_API_KEY=...
* pzGateway=http://pz-gateway.stage.geointservices.io
* Provide auth information for the Piazza Gateway in the header - you must authenticate for this process to work.
* event=true - only recommended for subsequent harvests (to support product lines). Don't use this flag for initial harvests because it will create hundreds of thousands of unnecessary events

## Testing Discovery
Call http://localhost:8080/discover with one or more of the following:
* bbox = x1,y1,x2,y2
* acquiredDate (RFC 3339)
* cloudCover (0 to 100)

## Subsequent harvests
Use the same endpoint as the initial harvest
* event=true (optional) (this causes the catalog to post a Piazza event each time a new scene is harvested. This is not recommended for the initial harvest, but may be done in subsequent harvests when the number of harvested scenes is lower)
  
## Setting up recurring harvests
Call the harvest operation with one additional parameter:
* recurring=true

When this is done, the image catalog will set up the following:
* service 
* event type
* event (with cron of something like every 24h)
* trigger to call the service when the event fires

When this is working right, the following will occur:
* harvest operations kicked off in image catalog (see PCF logs for evidence)
* events fired in Piazza (event name is something like `beachfront:harvest:new-image-harvested:0`) for each newly harvested scene 

## Finding the right Event Type ID
There is no way to search events by Event Type Name at this time. You need to resolve to an Event Type ID. Once you get this ID, you can call the `/event` endpoint on the gateway with `?eventTypeId=...`
* Call http://localhost:8080/eventTypeID
* pzGateway=http://pz-gateway.stage.geointservices.io

