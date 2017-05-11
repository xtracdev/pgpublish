# pgpublish

Publish events from Postgres to an SNS topic

[![CircleCI](https://circleci.com/gh/xtracdev/pgpublish.svg?style=svg)](https://circleci.com/gh/xtracdev/pgpublish)

This project complements the [pgeventstore](https://github.com/xtracdev/pgeventstore) 
project to take events from the publish table and send them to an AWS SNS
topic. Once published to SNS, events are removed from the publish table.

## Dependencies

<pre>
go get github.com/xtracdev/pgconn
go get github.com/xtracdev/pgeventstore
go get github.com/xtracdev/goes
go get github.com/Sirupsen/logrus
go get github.com/gucumber/gucumber
go get github.com/stretchr/testify/assert
go get github.com/lib/pq
go get github.com/aws/aws-sdk-go/...
go get github.com/armon/go-metrics
</pre>

## Contributing

To contribute, you must certify you agree with the [Developer Certificate of Origin](http://developercertificate.org/)
by signing your commits via `git -s`. To create a signature, configure your user name and email address in git.
Sign with your real name, do not use pseudonyms or submit anonymous commits.


In terms of workflow:

0. For significant changes or improvement, create an issue before commencing work.
1. Fork the respository, and create a branch for your edits.
2. Add tests that cover your changes, unit tests for smaller changes, acceptance test
for more significant functionality.
3. Run gofmt on each file you change before committing your changes.
4. Run golint on each file you change before committing your changes.
5. Make sure all the tests pass before committing your changes.
6. Commit your changes and issue a pull request.
## License

(c) 2017 Fidelity Investments
Licensed under the Apache License, Version 2.0
