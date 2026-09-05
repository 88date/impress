<div align="center">

[![impress logo](https://raw.githubusercontent.com/metarhia/Metarhia/master/Logos/impress-header.png)](https://github.com/metarhia/impress)
[![ci Status](https://github.com/metarhia/impress/workflows/Testing%20CI/badge.svg)](https://github.com/metarhia/impress/actions?query=workflow%3A%22Testing+CI%22+branch%3Amaster)
[![snyk](https://snyk.io/test/github/metarhia/impress/badge.svg)](https://snyk.io/test/github/metarhia/impress)
[![npm downloads/month](https://img.shields.io/npm/dm/impress.svg)](https://www.npmjs.com/package/impress)
[![npm downloads](https://img.shields.io/npm/dt/impress.svg)](https://www.npmjs.com/package/impress)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/metarhia/impress/blob/master/LICENSE)

</div>

**Enterprise** application server for **[Node.js](http://nodejs.org)**: secure,
lightweight, interactive, and scalable.

## Description

**First** Node.js server scaled with **multithreading** and extra thin workload
**isolation**. Optimized for **high-intensive** data exchange, rapid development,
and **clean architecture**. Provides everything you need out of the box for
**reliable** and **efficient backend**, network communication with web and mobile
clients, protocol-agnostic **API**, run-time type validation, real-time and
in-memory data processing, and **reliable stateful** services.

**Weak sides**: not a good choice for content publishing including blogs and
online stores, server-side rendering, serving static content and stateless
services.

**Strong sides**: security and architecture for enterprise-level applications,
long-lived connections over websocket to minimize overhead for cryptographic
handshake, no third-party dependencies.

## Quick start

- See project template: [metarhia/Example](https://github.com/metarhia/Example)
- Start server with `node server.js`
- See [documentation and specifications](https://github.com/metarhia/Contracts)

API endpoint example: `application/api/example.1/citiesByCountry.js`

```js
async ({ countryId }) => {
  const fields = ['cityId', 'name'];
  const where = { countryId };
  const data = await db.select('City', fields, where);
  return { result: 'success', data };
};
```

You can call it from client-side:

```js
const res = await metacom.api.example.citiesByCountry({ countryId: 3 });
```

## NATS RPC worker roles

With `config.server.nats.enabled`, every application worker opens its own NATS
connection. All workers can make outgoing RPC calls directly through NATS.

Only workers with `kind: 'server'` subscribe to API methods whose `transports`
include `'nats'`, answer discovery requests, and announce their service catalog.
Regular workers and balancers act as RPC clients. This rule also applies after
reconnection and when API files are reloaded.

Incoming NATS RPC handlers execute in the receiving server worker. If there are
multiple server workers, their subscriptions share a queue group for each
method. Configure at least one `server.ports` entry on an application that must
serve NATS RPC. Applications without server workers can call remote providers.

API methods remain available for local calls in every application worker.
One worker per application fetches the remote service catalog and listens for
discovery changes: the first server worker, or a regular worker if there are no
server workers, or the balancer if it is the only worker. The master caches the
catalog and distributes versioned snapshots to all application workers.

Workers wait for their first catalog within `server.timeouts.start`. Late and
restarted workers receive the cached snapshot. The discovery worker resumes
updates after its automatic restart; cached catalogs remain available during
that restart. Documentation uses the local catalog without a new NATS query.

Service calls use the highest known service version by default. To select a
specific version, pass it in the second argument:

```js
await service.profile.get({ id });
await service.profile.get({ id }, { version: 1 });
```

An explicit version applies to both local and remote calls. If that version or
method is unavailable, the call fails without falling back to another version.

## Application events

Declare events in `application/events`, declare their handlers in
`application/subscribers`, and publish through `events.emit(name, data)`.
See [event delivery, configuration, and migration](EVENTS.md).

RPC services expose actions only. Event publication and subscriptions use
the dedicated events API.

## Metarhia and impress application server way

- Applied code needs to be simple and secure, so we use sandboxing with v8
  isolated contexts, worker threads and javascript closures;
- Domain code should be separated from system code; so we use DDD, layered
  (onion) architecture, DI, SOLID and GRASP principles, contract-based approach;
- Impress supports stateful applications with RPC and client-session sticky to
  servers; microservices, centralized or distributed architecture;
- No I/O is faster even than async I/O, so we hold state in memory, share it
  among multiple threads and use lazy I/O for persistent storage;
- We use just internal trusted dependencies, no third-party npm packages;
  total Metarhia technology stack size is less than 2mb.

## Features

- **API auto-routing** calls to `endpoint` for rapid API development (no need to add routes manually)
- **API concurrency**: request execution timeout and execution queue with both timeout and size limitations
- **Schemas** for API contract, data structures validation, and domain models
- **Application server** supports different API styles: RPC over AJAX and over Websocket, REST, and web hooks
- **Multiple protocols** support: HTTP, HTTPS, WS, WSS
- **Auto loader** with `start` hooks, namespace generation for code and dependencies
- **Live reload** of code through filesystem watch
- **Graceful shutdown** with `stop` hooks
- **Minimal dependencies** and reduced code size
- **Layered architecture**: api, domain logic, data access layer, and system code layer (hidden)
- **Code sandboxing** for enhanced security and execution context isolation
- **Code protection**: reference pollution prevention, prototype pollution prevention
- **Multi-threading** for CPU utilization and execution isolation
- **Load balancing** for simple scaling with redirection to multiple ports
- **Caching**: in-memory caching for APIs and static files
- **Configuration**: environment-specific application settings
- **Database access** layer compatible with PostgreSQL with SQL-injection protection
- **Persistent sessions** with authentication, groups, and anonymous sessions
- **Buffered logging** (lazy write) with log rotation (keep logs N days) and console interface
- **Testing**: integrated node.js native test runner and table-test support
- **Inter-process** communication and shared memory used for state management
- **File utilities**: upload, download, support for partial content and streaming
- **Task Management**: BullMQ queues and PostgreSQL scheduling with pg-boss

## TODO list

Those features will be implemented in nearest future (3-6 months):

- Server health monitoring
- Database migrations
- State synchronization mechanism with transactions and subscription
- Multi-tenancy support

## Requirements

- Node.js v22.13 or later
- Linux (tested on Fedora v36-42, Ubuntu v20-25, CentOS v8-9, RHEL v8-10)
- PostgreSQL v12-17 (recommended v15+)
- OpenSSL v3.0+ (optional, for https & wss)
- [certbot](https://github.com/certbot/certbot) (recommended but optional)

## License & Contributors

Copyright (c) 2012-2025 Metarhia contributors.
See github for full [contributors list](https://github.com/metarhia/impress/graphs/contributors).
Impress Application Server is [MIT licensed](./LICENSE).
Project coordinator: &lt;timur.shemsedinov@gmail.com&gt;
