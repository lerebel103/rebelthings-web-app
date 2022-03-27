import * as admin from "firebase-admin";
admin.initializeApp();

import * as functions from "firebase-functions";
import * as express from "express";
import {https} from "firebase-functions";
import {PubSub} from "@google-cloud/pubsub";
import {firestore} from "firebase-admin";

const IOT_EVENTS_TOPIC = "iot-events";
const REGION = "australia-southeast1";
const RETENTION_PERIOD_SEC = 7 * 24 * 60 * 60;


/**
 * Helper function that will delete entries older than RETENTION_PERIOD_SEC
 */
const trimRecords = async () : Promise<void> => {
  const epochNowSec = Date.now().valueOf() / 1000;
  const oldestEpoch = epochNowSec - RETENTION_PERIOD_SEC;
  functions.logger.info(`Deleting records older than epoch ${oldestEpoch}`);
  const collections = await firestore().listCollections();
  for (const collection of collections) {
    const docs = await collection.listDocuments();
    // Batch all queries
    const batch = firestore().batch();
    for (const doc of docs) {
      const db = doc.collection(IOT_EVENTS_TOPIC).doc("telemetry").collection("events");
      const query = await db.where("timestamp", "<=", oldestEpoch).get();
      query.forEach((entry) => {
        batch.delete(entry.ref);
      });

      await batch.commit();
      functions.logger.info(`Deleted ${query.size} documents for ${doc.id}`);
    }
  }
  return Promise.resolve();
};

/**
 * Test function to trim records
 */
exports.testTrimDocuments = functions.region(REGION)
    .https
    .onRequest((req: https.Request, resp: express.Response) : Promise<void> => {
      return trimRecords();
    });

/**
 * Periodic handler which will trim the firestore DB to make sure it doesn't grow out of hand
 */
exports.trimTelemetryDocuments = functions.region(REGION)
    .pubsub.schedule("every 24 hours").onRun((context) => {
      return trimRecords();
    });

/**
 * Handles inbound telemetry payloads, processes them and stores them as required
 */
exports.handleIotEventsPayload =
    functions.region(REGION)
        .pubsub.topic(IOT_EVENTS_TOPIC)
        .onPublish((message) : Promise<void> => {
          try {
            const payload = JSON.stringify(message.json);
            const attributes = JSON.stringify(message.attributes);
            functions.logger.debug(`PubSub message was: ${payload}`);
            functions.logger.debug(`Attributes are: ${attributes}`);

            const db = firestore()
                .collection(message.attributes.deviceRegistryId)
                .doc(message.attributes.deviceId)
                .collection(`${IOT_EVENTS_TOPIC}`)
                .doc(`${message.attributes.subFolder}`)
                .collection("events");

            // Add new entry
            db.doc(`${message.json.timestamp}`)
                .set(message.json)
                .then(() => {
                  return Promise.resolve();
                }).catch((reason) => {
                  return Promise.reject(reason);
                });
            // Expected attributes
            //  {
            //      deviceId: "re-2.0-0000002",
            //      deviceNumId: "3187732082407108",
            //      deviceRegistryId: "RebelEspresso",
            //      deviceRegistryLocation: "asia-east1",
            //      projectId: "rebelthings",
            //      subFolder: "telemetry"
            //  }
          } catch (e) {
            functions.logger.error(`Failed to write payload to Firestore, ${e}`);
          }

          return Promise.resolve();
        });

/**
 * Used for testing so we can generate valid payloads
 */
exports.generateTestTelemetryPayload = functions.region(REGION)
    .https
    .onRequest((req: https.Request, resp: express.Response) : Promise<void> => {
      const pubsub = new PubSub();

      const attributes = {
        deviceId: "re-2.0-0000002",
        deviceNumId: "3187732082407108",
        deviceRegistryId: "RebelEspresso",
        deviceRegistryLocation: "asia-east1",
        projectId: "rebelthings",
        subFolder: "telemetry",
      };
      // Just make up a message
      const data = Buffer.from(JSON.stringify( {
        timestamp: Date.now().valueOf() / 1000,
        boiler_temp: 92,
        boiler_setpoint: 119.0,
        boiler_heat_duty: 5,
        brew_temp: 67.5,
      }));

      pubsub.topic(IOT_EVENTS_TOPIC).publishMessage({data, attributes, json: true}).then((value) => {
        resp.sendStatus(200);
      }).catch((reason) => {
        functions.logger.error(`Failed to publish to topic ${IOT_EVENTS_TOPIC}, ${reason}`);
        resp.sendStatus(500);
      });

      return Promise.resolve();
    });


