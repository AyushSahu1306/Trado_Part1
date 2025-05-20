import mqtt from "mqtt";
import * as marketdata from "../proto/market_data_pb";
import * as subscriptionManager from "./subscriptionManager";
import * as db from "../db";
import * as utils from "../utils";
import { config, INDICES } from "../config";

// Store LTP values for indices
const indexLtpMap = new Map<string, number>();
const atmStrikeMap = new Map<string, number>();

export function processMessage(
  topic: string,
  message: Buffer,
  client: mqtt.MqttClient
) {
  try {
    // 1. Parse the message
    let decoded: any = null;
    let ltpValues: number[] = [];

    // Try decoding as MarketData
    try {
      decoded = marketdata.marketdata.MarketData.decode(
        new Uint8Array(message)
      );
      if (decoded && typeof decoded.ltp === "number") {
        ltpValues.push(decoded.ltp);
      }
    } catch (err) {
      // Try decoding as MarketDataBatch
      try {
        decoded = marketdata.marketdata.MarketDataBatch.decode(
          new Uint8Array(message)
        );
        if (decoded && Array.isArray(decoded.data)) {
          ltpValues = decoded.data
            .map((d: any) => d.ltp)
            .filter((v: any) => typeof v === "number");
        }
      } catch (batchErr) {
        // Try decoding as JSON
        try {
          decoded = JSON.parse(message.toString());
          if (decoded && typeof decoded.ltp === "number") {
            ltpValues.push(decoded.ltp);
          }
        } catch (jsonErr) {
          console.error(
            "Failed to decode message as protobuf or JSON for topic:",
            topic
          );
        }
      }
    }

    // 2. Process each LTP value
    for (const ltp of ltpValues) {
      // 3. Determine topic metadata
      let indexName: string | undefined;
      let type: string | undefined;
      let strike: number | undefined;

      const isIndexTopic = topic.startsWith(`${config.app.indexPrefix}/`);
      if (isIndexTopic) {
        // Extract index name (e.g., "index/NIFTY" -> "NIFTY")
        indexName = topic.split("/")[1];
        if (INDICES.includes(indexName)) {
          indexLtpMap.set(indexName, ltp);

          // 4. If it's the first message for this index, calculate ATM and subscribe to options
          if (subscriptionManager.isFirstIndexMessage.get(indexName)) {
            const atmStrike = utils.getAtmStrike(indexName, ltp);
            atmStrikeMap.set(indexName, atmStrike);
            subscriptionManager.isFirstIndexMessage.set(indexName, false);
            subscriptionManager.subscribeToAtmOptions(client, indexName, atmStrike);
            console.log(`Calculated ATM for ${indexName}: ${atmStrike}`);
          }
        }
      } else if (topic.startsWith("NSE_FO|")) {
        // Option topic, retrieve metadata
        const metadata = subscriptionManager.topicMetadataMap.get(topic);
        if (metadata) {
          indexName = metadata.indexName;
          type = metadata.type;
          strike = metadata.strike;
        }
      }

      // 5. Save data to database
      db.saveToDatabase(topic, ltp, indexName, type, strike);
      console.log("data saved to db");
    }
  } catch (error) {
    console.error("Error processing message:", error);
  }
}