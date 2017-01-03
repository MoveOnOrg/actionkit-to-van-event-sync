# ActionKit to VAN Event Sync

This code syncs ActionKit events into NGPVAN events. This is intended as an example of how you might use https://github.com/MoveOnOrg/ngpvan_api, not as a ready-to-use tool. You're welcome to try to use the code, but you're likely to need something slightly different, because this was built around some specific assumptions for MoveOn's needs.

Specifically, this assumes two tables in a PostgreSQL database (we use Amazon Redshift):

1. actionkit.events_event: a live copy of ActionKit's events table
2. ngpvan.events: a custom table this code populates with NGPVAN events

There are two scripts included in this process:

1. event_van_export.py exports all events from NGPVAN into the ngpvan.events table, so the two systems can be compared in a single query.
2. event.py compares the two systems and adds any new events to NGPVAN.

Both scripts run as cron tasks, however often you want to sync. Note that VAN typically charges per API request, so you'll want to be careful how often you do this. Also note this is a one-way sync. It assumes synced events are initially added in ActionKit, and does not copy events from VAN back to ActionKit.

Both scripts print to the command line extensive details on what they are doing.

## How MoveOn used this

During the 2016 election, MoveOn organized a massive voter canvass across several key states. We did "barnstorming" events to recruit local leads, and then "canvass" events where we knocked on doors. In ActionKit, we created separate "barnstorm" and "canvass" campaigns, added all the events, emailed invitations to local members, and took RSVPs.

This code then synced those events to the appropriate VAN state instance, where we invited local members by phone, and entered verbal RSVPs, which were then synced back to ActionKit by separate scripts not included here. In VAN, "barnstorm" and "canvass" were the names of event types, and VAN's "shortName" field was set to the ActionKit event ID, to identify the same event between systems.
