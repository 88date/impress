'use strict';

const getExample = (examples) => {
  if (!Array.isArray(examples) || examples.length === 0) return null;
  return examples[0].parameters;
};

const describeProcedure = (procedure) => {
  const exp = procedure.exports;
  return {
    origin: typeof procedure.method === 'function' ? 'local' : 'remote',
    caption: exp.caption,
    description: exp.description,
    protocols: exp.protocols,
    transports: procedure.transports,
    roles: exp.roles,
    access: exp.access,
    parameters: exp.parameters,
    deprecated: exp.deprecated,
    returns: exp.returns,
    errors: exp.errors,
    example: getExample(exp.examples),
  };
};

const describeCollection = (collection, isAvailable) => {
  const units = {};
  for (const [unitName, unit] of Object.entries(collection)) {
    const versions = {};
    for (const [version, procedures] of Object.entries(unit)) {
      if (version === 'default') continue;
      const methods = {};
      for (const [methodName, procedure] of Object.entries(procedures)) {
        if (!isAvailable(procedure, unitName, version, methodName)) continue;
        methods[methodName] = describeProcedure(procedure);
      }
      if (Object.keys(methods).length > 0) versions[version] = methods;
    }
    if (Object.keys(versions).length > 0) units[unitName] = versions;
  }
  return units;
};

const describeApi = (collection) =>
  describeCollection(
    collection,
    (procedure) => typeof procedure.method === 'function',
  );

const describeServices = (collection, discovered = null) =>
  describeCollection(collection, (broker, serviceName, version, actionName) => {
    const isService = typeof broker.method === 'function' || broker.discovered;
    if (!isService || !discovered) return isService;
    const actions = discovered.get(serviceName);
    const key = `${version}.${actionName}`;
    return actions?.has(key) || false;
  });

const describeSchemas = (definitions) => {
  const schemas = {};
  for (const [name, definition] of definitions) {
    if (name.startsWith('.')) continue;
    schemas[name] = definition;
  }
  return schemas;
};

const describeMetadata = (source) => {
  if (Array.isArray(source)) return source.map(describeMetadata);
  if (source === null || typeof source !== 'object') return source;
  const metadata = {};
  for (const [name, value] of Object.entries(source)) {
    if (typeof value === 'function') continue;
    metadata[name] = describeMetadata(value);
  }
  return metadata;
};

const describeQueues = (configs, handlers) => {
  const queues = {};
  const queueNames = new Set([
    ...Object.keys(configs),
    ...Object.keys(handlers),
  ]);
  for (const queueName of queueNames) {
    const config = configs[queueName] || {};
    const workers = handlers[queueName] || {};
    queues[queueName] = describeMetadata({ ...config, workers });
  }
  return queues;
};

const describeEvents = (local = [], discovered = null) => {
  const events = new Map();
  for (const [name, event] of discovered || []) {
    events.set(name, {
      ...event,
      transports: event.transports || ['nats'],
      origin: 'remote',
    });
  }
  for (const event of local) {
    events.set(event.name, { ...event, origin: 'local' });
  }
  return Array.from(events.values()).sort((first, second) =>
    first.name.localeCompare(second.name),
  );
};

module.exports = {
  describeApi,
  describeServices,
  describeSchemas,
  describeQueues,
  describeEvents,
};
