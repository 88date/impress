'use strict';

const quoteIdentifier = (value) => '"' + value.replaceAll('"', '""') + '"';

const serializePayload = (payload) => {
  const message = 'Outbox payload must be JSON-serializable';
  let serialized;
  try {
    serialized = JSON.stringify(payload);
  } catch (cause) {
    throw new Error(message, { cause });
  }
  if (serialized === undefined) throw new Error(message);
  return serialized;
};

class Outbox {
  constructor({ database, schema = 'public', table = 'outbox' }) {
    const target = `${quoteIdentifier(schema)}.${quoteIdentifier(table)}`;
    this.database = database;
    this.sql =
      `INSERT INTO ${target} ` +
      '("id", "name", "subject", "createdAt", "payload") ' +
      'VALUES ($1, $2, $3, $4, $5::text::jsonb)';
  }

  async write({ id, name, subject, createdAt, payload }, transaction = null) {
    const database = transaction ?? this.database;
    const serialized = serializePayload(payload);
    const values = [id, name, subject, createdAt, serialized];
    await database.query(this.sql, values);
  }
}

module.exports = { Outbox };
