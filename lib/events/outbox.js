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
    const insert =
      `INSERT INTO ${target} ` +
      '("id", "name", "subject", "createdAt", "payload") ';
    this.database = database;
    this.sql = insert + 'VALUES ($1, $2, $3, $4, $5::text::jsonb)';
    this.bulkSql =
      insert +
      'SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], ' +
      '$4::timestamptz[], $5::jsonb[])';
  }

  async write({ id, name, subject, createdAt, payload }, transaction = null) {
    const database = transaction ?? this.database;
    const serialized = serializePayload(payload);
    const values = [id, name, subject, createdAt, serialized];
    await database.query(this.sql, values);
  }

  async writeBulk(records, transaction = null) {
    const database = transaction ?? this.database;
    const ids = records.map(({ id }) => id);
    const values = [
      ids,
      records.map(({ name }) => name),
      records.map(({ subject }) => subject),
      records.map(({ createdAt }) => createdAt),
      records.map(({ payload }) => serializePayload(payload)),
    ];
    await database.query(this.bulkSql, values);
    return ids;
  }
}

module.exports = { Outbox };
