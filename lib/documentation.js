'use strict';

const getExample = (examples) => {
  if (!Array.isArray(examples) || examples.length === 0) return null;
  return examples[0].parameters;
};

const describeProcedure = (procedure) => {
  const exp = procedure.exports;
  return {
    caption: exp.caption,
    description: exp.description,
    protocols: exp.protocols,
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
        if (!isAvailable(procedure)) continue;
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

const describeServices = (collection) =>
  describeCollection(
    collection,
    (broker) => typeof broker.method === 'function' || broker.discovered,
  );

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

module.exports = {
  describeApi,
  describeServices,
  describeSchemas,
  describeQueues,
};
