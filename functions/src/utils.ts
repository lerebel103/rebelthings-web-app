import * as functions from "firebase-functions";
import {firestore} from "firebase-admin";
import {PubSub} from "@google-cloud/pubsub";
import * as express from "express";

export const IOT_EVENTS_TOPIC = "iot-events";
export const REGION = "australia-southeast1";

const RETENTION_PERIOD_SEC = 7 * 24 * 60 * 60;

/**
 * Helper function that will delete entries older than RETENTION_PERIOD_SEC
 */
export const trimRecords = async (): Promise<void> => {
  const epochNowSec = Date.now().valueOf() / 1000;
  const oldestEpoch = epochNowSec - RETENTION_PERIOD_SEC;

  functions.logger.info(`Deleting records older than epoch ${oldestEpoch}`);
  const collections = await firestore().listCollections();

  for (const collection of collections) {
    const docs = await collection.listDocuments();

    // Batch all queries
    for (const doc of docs) {
      const db = doc.collection(IOT_EVENTS_TOPIC).doc("telemetry").collection("events");
      let querySize = 0;
      let totalCount = 0;
      do {
        const batch = firestore().batch();
        const query = await db.where("timestamp", "<=", oldestEpoch).limit(500).get();
        querySize = query.size;
        totalCount += querySize;
        query.forEach((entry) => {
          batch.delete(entry.ref);
        });

        await batch.commit();
      } while (querySize > 0);
      functions.logger.info(`Deleted ${totalCount} documents for ${doc.id}`);
    }
  }

  functions.logger.info("Trimming done.");
  return Promise.resolve();
};

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const generateTestTelemetyData = async (resp: express.Response): Promise<void> => {
  const pubsub = new PubSub();
  const now = new Date();


  // Generate a bunch of messages for 10 different devices
  const startID = 9999999;
  for (let i = 1; i < 4; i++) {
    const deviceID = `re-2.0-${startID - i}`;

    const attributes = {
      deviceId: deviceID,
      deviceNumId: i.toString(),
      deviceRegistryId: "RebelEspresso",
      deviceRegistryLocation: "asia-east1",
      projectId: "rebelthings",
      subFolder: "telemetry",
    };

    functions.logger.info(`Generating data for ${deviceID}`);

    // Just make up a message spanning 10 days also
    for (let day = 0; day < 10; day++) {
      const current = new Date().setDate(now.getDate() - day);

      for (let timeOffset = 0; timeOffset < 10; timeOffset++) {
        const data = Buffer.from(JSON.stringify({
          timestamp: current.valueOf() / 1000 - timeOffset,
          boiler_temp: 92,
          boiler_setpoint: 119.0,
          boiler_heat_duty: 5,
          brew_temp: 67.5,
        }));

        await pubsub.topic(IOT_EVENTS_TOPIC).publishMessage({data, attributes, json: true});
      }
    }
  }

  functions.logger.info("Done.");
  return Promise.resolve();
};
