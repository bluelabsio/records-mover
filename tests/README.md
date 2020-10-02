# Tests

## Test suites

* unit: Tests which interact with the methods of a single
  class/module, and use mocks to assert that side-effects occur -
  either on arguments passed in, or on dependencies of the
  class/module.  These tests mock anything underneath that touches the
  filesystem or database, and any non-trivial dependencies of the
  class.  See
  [Sandi Metz' advice for unit testing](https://www.youtube.com/watch?v=URSWYvyc42M)
  for the philosphy used in this suite.

* component: Tests which minimize their use of mocks to the following
  situations:

  * Mocks representing 'this argument/dependency won't be used'.
  * Mocks used as unique values to verify inputs get mapped to certain outputs.
  * Mocks used to avoid I/O - e.g., touching the filesystem, network or database

  These tests may interact with multiple classes/modules, including
  those classes whose public methods may not be considered part of the
  Records Mover public API.

  The idea with this suite is that it can be used to test at more
  natural levels of interaction than at the class/module level.

  Since much of the integration test suite is per-database, this is
  also a good place to test larger system behavior that isn't
  database-specific to minimize test time/cost.

  Note that tests in the 'unit' suite make small scale (within the
  same class) refactors very safe, they are fragile when refactors
  among different classes are made.

  If you are performing a refactor among different classes and want
  automated test support, adding a component test at a higher level to
  the changed classes is a good option.  At that point, you can safely
  remove the failing unit tests.

  If adding a component test isn't an easy option, it may be worth a
  little time to see if there's a natural higher level interface that
  can be added that would change that situation (not guanteed, but
  worth a look!).

* integration: Tests which interact with the API at the highest level,
  or use the CLI.  These tests can touch databases, the network, etc,
  in controlled ways (e..g, using Docker or specific test accounts in
  cloud databases).

  Since Records Mover testing aims to ensure consistent behavior
  against different databases with different formats of data, this is
  an important suite than can reveal complex interaction issues.
  Records Mover doesn't comply with a traditional test pyramid in that
  sense, but uses extensive parallelism in CircleCI to minimize test
  time.  Despite the provocative title,
  [Unit testing is overrated](https://tyrrrz.me/blog/unit-testing-is-overrated)
  describes the trade-offs and rationale to this approach.

  That said, adding new tests to this suite should be done sparingly,
  especially as many of the tests make sense to perform against
  multiple database types (`n`), data input types (`d`), and
  sources/targets (`s`/`t`) - right now our suite is `O(n^2)` for the
  database-to-database tests, `O(d*n)` for the database import tests,
  and `O(s*t)` for the CLI "any2any" tests.

  All of these values are expected to rise in the future, so this all
  adds up quick, even in the cloud!

  Consider adding tests first at the unit level for detailed class
  behavior, or ideally at the component level if the behavior can
  exercised with minimal use of mocking.
