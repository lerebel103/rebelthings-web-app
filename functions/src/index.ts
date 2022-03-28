import * as admin from "firebase-admin";
admin.initializeApp();

import * as functions from "firebase-functions";
import {firestore} from "firebase-admin";
import {generateTestTelemetyData, IOT_EVENTS_TOPIC, REGION, trimRecords} from "./utils";
import {https} from "firebase-functions";
import * as express from "express";


/**
 * Periodic handler which will trim the firestore DB to make sure it doesn't grow out of hand
 */
exports.trimTelemetryDocuments = functions.region(REGION)
    .runWith({timeoutSeconds: 300, memory: "512MB"})
    .pubsub.schedule("every 24 hours").onRun((context) => {
      return trimRecords();
    });

/**
 * Handles inbound telemetry payloads, processes them and stores them as required
 *
 * Expected attributes in payload are:
 *   {
 *     deviceId: "re-2.0-0000002",
 *     deviceNumId: "3187732082407108",
 *     deviceRegistryId: "RebelEspresso",
 *     deviceRegistryLocation: "asia-east1",
 *     projectId: "rebelthings",
 *     subFolder: "telemetry"
 *   }
 */
exports.handleIotEventsPayload =
    functions.region(REGION)
        .pubsub.topic(IOT_EVENTS_TOPIC)
        .onPublish((message) : Promise<void> => {
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

          return Promise.resolve();
        });

/**
 * Used for testing so we can generate valid payloads
 */
exports.generateTestTelemetry = functions.region(REGION)
    .https
    .onRequest((req: https.Request, resp: express.Response) : Promise<void> => {
      return generateTestTelemetyData(resp);
    });
