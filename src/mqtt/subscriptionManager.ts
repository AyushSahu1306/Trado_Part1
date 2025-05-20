import mqtt from "mqtt";
import { config, INDICES, EXPIRY_DATES, STRIKE_RANGE } from "../config";
import * as utils from "../utils";


// Set of active subscriptions to avoid duplicates
export const activeSubscriptions = new Set<string>();

// Track if we've received the first message for each index
export const isFirstIndexMessage = new Map<string, boolean>();

// Store topic metadata for option topics
interface TopicMetadata {
  indexName: string;
  type: "ce" | "pe";
  strike: number;
}
export const topicMetadataMap = new Map<string, TopicMetadata>();

// Subscribe to all index topics
export function subscribeToAllIndices(client: mqtt.MqttClient) {
  INDICES.forEach((indexName) => {
    const topic = `${config.app.indexPrefix}/${indexName}`;
    console.log(`Subscribing to index: ${topic}`);
    client.subscribe(topic);
    activeSubscriptions.add(topic);
  });
}

// Initialize first message tracking
export function initializeFirstMessageTracking() {
  INDICES.forEach((indexName) => {
    isFirstIndexMessage.set(indexName, true);
  });
}

// Subscribe to options around ATM strike
export async function subscribeToAtmOptions(
  client: mqtt.MqttClient,
  indexName: string,
  atmStrike: number
) {
  console.log(`Subscribing to ${indexName} options around ATM ${atmStrike}`);

  const strikeDiff = utils.getStrikeDiff(indexName);
  const strikes = [];

  // 1. Calculate strike prices around ATM
  for (let i = -STRIKE_RANGE; i <= STRIKE_RANGE; i++) {
    strikes.push(atmStrike + i * strikeDiff);
  }

  // 2. For each strike, get option tokens for CE and PE
  for (const strike of strikes) {
    for (const optionType of ["ce", "pe"] as const) {
      const token = await getOptionToken(indexName, strike, optionType);
      if (token) {
        const topic = utils.getOptionTopic(indexName, token);
        if (!activeSubscriptions.has(topic)) {
          console.log(`Subscribing to option: ${topic}`);
          client.subscribe(topic);
          activeSubscriptions.add(topic);

          // Store metadata for this topic
          topicMetadataMap.set(topic, {
            indexName,
            type: optionType,
            strike,
          });
        }
      }
    }
  }
}

// Fetch option token from API
export async function getOptionToken(
  indexName: string,
  strikePrice: number,
  optionType: "ce" | "pe"
): Promise<string | null> {
  try {
    const expiryDate = EXPIRY_DATES[indexName as keyof typeof EXPIRY_DATES];
    const url = `https://api.trado.trade/token?index=${indexName}&expiryDate=${expiryDate}&optionType=${optionType}&strikePrice=${strikePrice}`;

    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    if (data && data.token) {
      return data.token;
    }
    return null;
  } catch (error) {
    console.error(
      `Error fetching token for ${indexName} ${strikePrice} ${optionType}:`,
      error
    );
    return null;
  }
}