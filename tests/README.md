# Tests

## Test suites

* unit: Tests which interact with the methods of a single
  class/module.  They mock anything underneath that touches the
  filesystem or database, and any non-trivial dependencies of the
  class.  See
  [Sandi Metz' advice for unit testing](https://www.youtube.com/watch?v=URSWYvyc42M)
  for the philosphy used in this suite.

* component: Tests which interact with multiple classes/modules, even
  those classes whose public methods may not be considered part of the
  Records Mover public API.  They generally use mocks extremely
  sparingly--generally only mocking things which do I/O (e.g.,
  touching the filesystem, network or database).

* integration: Tests which interact with the API at the highest level,
  or use the CLI.  These tests can touch databases, the network, etc,
  in controlled ways (e..g, using Docker or specific test accounts in
  cloud databases).

  Since a lot of Records Mover testing aims to ensure consistent
  behavior against different databases with different formats of
  data, this is an important suite than can reveal complex interaction
  issues.  Records Mover doesn't comply with a traditional test
  pyramid in that sense, but uses extensive parallelism in CircleCI to
  minimize test time.  Despite the provocative title,
  [Unit testing is overrated](https://tyrrrz.me/blog/unit-testing-is-overrated)
  describes the trade-offs and rationale to this approach.

  Adding new tests to this suite should be done sparingly, especially
  as many of the tests make sense to perform against multiple database
  types and data input types - `n*m` adds up quick, even in the cloud!
  Consider adding tests first at the unit level for detailed class
  behavior, or at the component level if the interaction can be
  reproduced there.
