# Security model

Records mover, as with any data tool, is capable of be used to process
sensitive information.  Any time you choose to do this, it's important
to understand the security model of all of the pieces.

Key notes:

* If you are using MySQL, please read below and understand before proceeding.

## Authenticating TO external servers/services

Depending on what you ask it to do, records mover uses external
servers/services like databases and object stores.

These typically have a mode of authentication, whether it be
username/password, some form of OAuth, or another key-based system.
Records mover of course relies on the server/service you place your
data on being configured to allow only authorized use.

## Local memory is trusted

While connecting to external servers/services, the credentials for
those servers/services will often be read into memory.

In addition, for performance reasons, records mover attempts to
instruct servers/services to share data directly with each other
whenever possible as opposed to loading it into memory.  However, that
options is often not feasible due to format differences or the need to
determine the format to begin with.

As a result, records mover must bring data into memory of the machine
upon which it is run and it must be considered trusted.

## Protection of credentials/secrets

The credentials themselves that you use are needed by records mover in
order to log into those services.  Records mover has a dependency
called [db-facts](https://github.com/bluelabsio/db-facts) which can be
configured to use the secrets service of your choice to store these
secrets, but it is your responsibility to configure db-facts and store
the credential in a suitably secure way for your purposes.

## Encryption of communication

Records mover relies upon Python standard libraries (e.g., sqlalchemy
and boto3) to connect to servers/services.  We recommend ensuring that
servers/services only allow connections via TLS and that records mover
is configured to only make connections via TLS.

PRs which help users configure this securely by default are very
welcome.

## Authentication OF servers/services

If you are concerned that your network may be compromised, and that
you may be tricked into providing credentials or sensitive information
to a monster-in-the-middle (MITM) attack, you should ensure that your
servers/services are configured with a known Certificate Authority
(CA), and that you provide CA certificate in the connection made.

PRs to make this configuration easier to use with records mover are
very welcome.

## Server/service-specific concerns

### MySQL

In order to use bulk CSV loading with MySQL, records mover enables on
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
* Using pymysql's syntax to point to the correct CA cert for
  the MySQL server and require validation.

PRs to make this configuration easier to use with records mover are
very welcome.
