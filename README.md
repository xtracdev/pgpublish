# pgpublish

Publish events from Postgres to an SNS topic


## TODO

* Get working assuming single publisher
* Add a publisher lock table
    Note we want to allow multiple event stores to be able to write to
    the table, but we want exclusive publishers to ensure each event is 
    published once.

Dependencies:

* go get github.com/aws/aws-sdk-go/...