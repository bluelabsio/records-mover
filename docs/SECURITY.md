# Security model

Records Mover, as with any data tool, is capable of be used to process
sensitive information.  Any time you choose to do this, it's important
to understand the security model of all of the pieces.

Key notes:

* If you are using MySQL, please read below and understand before proceeding.

## Input trust concerns

Records Mover has not yet been carefully inspected for use in
environments where it runs at an elevated level of trust compared to
its inputs (e.g., running Records Mover server-side in a web
application).

Inputs to think about:

* CSV/Parquet/Avro/etc files
* Databases/other systems from which data is being pulled
* Configuration and secrets management systems being used, including
  by db-facts

Some general things to audit before running in mixed trust
environments:

* Records Mover code
* Underlying library code (e.g., Pandas)
* Services/servers to which data is being pushed

Some specifics to consider:

* CSV hints are complex.  Date/time format hints in particular are
  complex and involve format strings.  Some areas of the code
  (including Pandas import and export code) process those dynamically,
  by converting between the format in the Records Spec to formats
  being used by other systems.
* Please PR and add more as you spot specific areas of concern.

## Authenticating TO external servers/services

Depending on what you ask it to do, Records Mover uses external
servers/services like databases and object stores.

These typically have a mode of authentication, whether it be
username/password, some form of OAuth, or another key-based system.
Records Mover of course relies on the server/service you place your
data on being configured to allow only authorized use.

## Local memory is trusted

While connecting to external servers/services, the credentials for
those servers/services will often be read into memory.

In addition, for performance reasons, Records Mover attempts to
instruct servers/services to share data directly with each other
whenever possible as opposed to loading it into memory.  However, that
options is often not feasible due to format differences or the need to
determine the format to begin with.

As a result, Records Mover must bring data into memory of the machine
upon which it is run and it must be considered trusted.

## Protection of credentials/secrets

The credentials themselves that you use are needed by Records Mover in
order to log into those services.  Records Mover has a dependency
called [db-facts](https://github.com/bluelabsio/db-facts) which can be
configured to use the secrets service of your choice to store these
secrets, but it is your responsibility to configure db-facts and store
the credential in a suitably secure way for your purposes.

## Encryption of communication

Records Mover relies upon Python standard libraries (e.g., sqlalchemy
and boto3) to connect to servers/services.  We recommend ensuring that
servers/services only allow connections via TLS and that Records Mover
is configured to only make connections via TLS.

PRs which help users configure this securely by default are very
welcome.

## Authentication OF servers/services

If you are concerned that your network may be compromised, and that
you may be tricked into providing credentials or sensitive information
to a monster-in-the-middle (MITM) attack, you should ensure that your
servers/services are configured with a known Certificate Authority
(CA), and that you provide CA certificate in the connection made.

PRs to make this configuration easier to use with Records Mover are
very welcome.

## Server/service-specific concerns

### MySQL

In order to use bulk CSV loading with MySQL, Records Mover enables on
the client side the "local_infile" option. Please see
[MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/load-data-local-security.html)
to read about the security aspects of this.

As noted above We are assuming that the server is known and trusted,
and that the network connection to it can be trusted.

If the server is trusted but the network is not, we recommend you
investigate the following:

* Enabling SSL on your MySQL server.
* Using the 'REQUIRE SSL' clause in the 'GRANT ALL
  PRIVILEGES' statement used to create users.


PRs to make this configuration easier to use with Records Mover are
very welcome.
