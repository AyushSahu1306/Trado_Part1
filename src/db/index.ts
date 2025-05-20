import { Pool } from "pg";
import { config } from "../config";

// Define a type for batch items
export interface BatchItem {
  topic: string;
  ltp: number;
  indexName?: string;
  type?: string;
  strike?: number;
}

// Initialize database connection pool
let pool: Pool;
let dataBatch: BatchItem[] = [];
let batchTimer: NodeJS.Timeout | null = null;

// Cache topic IDs to avoid repeated lookups
const topicCache = new Map<string, number>();

export function createPool(): Pool {
  return new Pool({
    host: config.db.host,
    port: config.db.port,
    user: config.db.user,
    password: config.db.password,
    database: config.db.database,
  });
}

export async function initialize(dbPool: Pool) {
  pool = dbPool;
  console.log("Database initialized");

  // TODO: Preload topic cache from database

  try {
    const res = await pool.query("SELECT topic_id, topic_name, index_name, type, strike FROM topics");
    for (const row of res.rows) {
      const key = `${row.topic_name}|${row.index_name || ""}|${row.type || ""}|${row.strike || ""}`;
      topicCache.set(key, row.topic_id);
    }
    console.log(`Loaded ${topicCache.size} topics into cache`);
  } catch (error) {
    console.error("Error preloading topic cache:", error);
  }

}

export async function getTopicId(
  topicName: string,
  indexName?: string,
  type?: string,
  strike?: number
): Promise<number> {
  // TODO: Implement this function
  // 1. Check if topic exists in cache
  // 2. If not in cache, check if it exists in database
  // 3. If not in database, insert it
  // 4. Return topic_id

  const cacheKey = `${topicName}|${indexName || ""}|${type || ""}|${strike || ""}`;

  // 1. Check if topic exists in cache
  if (topicCache.has(cacheKey)) {
    return topicCache.get(cacheKey)!;
  }

  // 2. If not in cache, check if it exists in database
  try {
    const res = await pool.query(
      "SELECT topic_id FROM topics WHERE topic_name = $1 AND index_name = $2 AND type = $3 AND strike = $4",
      [topicName, indexName || null, type || null, strike || null]
    );

    if (res.rows.length > 0) {
      const topicId = res.rows[0].topic_id;
      topicCache.set(cacheKey, topicId);
      return topicId;
    }

    // 3. If not in database, insert it
    const insertRes = await pool.query(
      "INSERT INTO topics (topic_name, index_name, type, strike) VALUES ($1, $2, $3, $4) RETURNING topic_id",
      [topicName, indexName || null, type || null, strike || null]
    );

    const topicId = insertRes.rows[0].topic_id;
    topicCache.set(cacheKey, topicId);
    return topicId;
  } catch (error) {
    console.error("Error getting topic ID:", error);
    throw error;
  }


  return 0; // Placeholder
}

export function saveToDatabase(
  topic: string,
  ltp: number,
  indexName?: string,
  type?: string,
  strike?: number
) {
  // TODO: Implement this function
  // 1. Add item to batch
  // 2. If batch timer is not running, start it
  // 3. If batch size reaches threshold, flush batch

  const formattedLtp = Number(ltp.toFixed(2));
  if (formattedLtp > 99999999.99) {
    console.warn(`LTP ${formattedLtp} exceeds NUMERIC(10,2) limit for topic ${topic}, skipping`);
    return;
  }

  // 1. Add item to batch
  dataBatch.push({ topic, ltp: formattedLtp, indexName, type, strike });

  // 2. If batch timer is not running, start it
  if (!batchTimer) {
    batchTimer = setTimeout(async () => {
      await flushBatch();
    }, config.app.batchInterval);
  }

  // 3. If batch size reaches threshold, flush batch
  if (dataBatch.length >= config.app.batchSize) {
    clearTimeout(batchTimer!);
    batchTimer = null;
    flushBatch();
  }

  console.log(`Saving to database: ${topic}, LTP: ${ltp}`);
}

export async function flushBatch() {
  // TODO: Implement this function
  // 1. Clear timer
  // 2. If batch is empty, return
  // 3. Process batch items (get topic IDs)
  // 4. Insert data in a transaction
  // 5. Reset batch

  // 1. Clear timer
  if (batchTimer) {
    clearTimeout(batchTimer);
    batchTimer = null;
  }

  // 2. If batch is empty, return
  if (dataBatch.length === 0) {
    return;
  }

  // 3. Process batch items (get topic IDs)
  const batchToInsert: { topicId: number; ltp: number; receivedAt: Date }[] = [];
  for (const item of dataBatch) {
    try {
      const topicId = await getTopicId(item.topic, item.indexName, item.type, item.strike);
      batchToInsert.push({ topicId, ltp: item.ltp, receivedAt: new Date() });
    } catch (error) {
      console.error(`Error processing item for topic ${item.topic}:`, error);
    }
  }

  // 4. Insert data in a transaction
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const queryText =
      "INSERT INTO ltp_data (topic_id, ltp, received_at) VALUES ($1, $2, $3)";
    for (const item of batchToInsert) {
      await client.query(queryText, [item.topicId, item.ltp, item.receivedAt]);
    }
    await client.query("COMMIT");
    console.log(`Inserted ${batchToInsert.length} items into ltp_data`);
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error inserting batch:", error);
  } finally {
    client.release();
  }

  // 5. Reset batch
  dataBatch = [];

  console.log("Flushing batch to database");
}

export async function cleanupDatabase() {
  // Flush any remaining items in the batch
  if (dataBatch.length > 0) {
    await flushBatch();
  }

  // Close the database pool
  if (pool) {
    await pool.end();
  }

  console.log("Database cleanup completed");
}
