import * as admin from "firebase-admin";
admin.initializeApp();

import * as functions from "firebase-functions";
import * as express from "express";
import {https} from "firebase-functions";
import {PubSub} from "@google-cloud/pubsub";
import {firestore} from "firebase-admin";
import CollectionReference = firestore.CollectionReference;
import DocumentData = firestore.DocumentData;
import WriteResult = firestore.WriteResult;

const TELEMETRY_TOPIC = "telemetry";
const REGION = "australia-southeast1";
const RETENTION_PERIOD_SEC = 7 * 24 * 60 * 60;


/**
 * Deletes old Firestore entries
 *
 * @param {CollectionReference<DocumentData>} db This is the reference to the db path to delete from.
 */
async function deleteOldEntries(db: CollectionReference<DocumentData>): Promise<WriteResult[]> {
  // Now delete old entries
  const batch = firestore().batch();
  const epochNowSec = Date.now().valueOf() / 1000;
  const oldestEpoch = epochNowSec - RETENTION_PERIOD_SEC;
  const queryResult = await db.orderBy("timestamp").where("timestamp", "<=", oldestEpoch).get();
  queryResult.forEach((entry) =>{
    batch.delete(entry.ref);
    console.log(`Deleting ${entry.data().timestamp}`);
  });

  return await batch.commit();
}
/**
 * Handles inbound telemetry payloads, processes them and stores them as required
 */
exports.handleTelemetryPayload =
    functions.region(REGION)
        .pubsub.topic(TELEMETRY_TOPIC)
        .onPublish((message) : Promise<void> => {
          try {
            const payload = JSON.stringify(message.json);
            const attributes = JSON.stringify(message.attributes);
            functions.logger.debug(`PubSub message was: ${payload}`);
            functions.logger.debug(`Attributes are: ${attributes}`);

            const db = firestore()
                .collection(message.attributes.deviceRegistryId)
                .doc(message.attributes.deviceId)
                .collection(`${TELEMETRY_TOPIC}`)
                .doc(`${message.attributes.subFolder}`)
                .collection("messages");
            // Add new entry
            db.doc(`${message.json.timestamp}`)
                .set(message.json)
                .then(() => {
                  deleteOldEntries(db).then( () =>{
                    return Promise.resolve();
                  });
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

      pubsub.topic(TELEMETRY_TOPIC).publishMessage({data, attributes, json: true}).then((value) => {
        console.log(value); // Success!
        resp.sendStatus(200);
      }).catch((reason) => {
        functions.logger.error(`Failed to publish to topic ${TELEMETRY_TOPIC}, ${reason}`);
        resp.sendStatus(500);
      });

      return Promise.resolve();
    });


