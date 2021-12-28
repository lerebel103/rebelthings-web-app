import * as functions from "firebase-functions";
import * as express from "express";
import {https} from "firebase-functions";
import {PubSub} from "@google-cloud/pubsub";

const TELEMETRY_TOPIC = "telemetry";
const REGION = "australia-southeast1";

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
            functions.logger.error("PubSub message was not JSON, e");
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

      // Just make up a message
      const data = Buffer.from(JSON.stringify( {
        timestamp: 1640683082,
        boiler_temp: 92,
        boiler_setpoint: 119.0,
        boiler_heat_duty: 5,
        brew_temp: 67.5,
      }));

      pubsub.topic(TELEMETRY_TOPIC).publishMessage({data, json: true}).then((value) => {
        console.log(value); // Success!
        resp.sendStatus(200);
      }).catch((reason) => {
        functions.logger.error(`Failed to publish to topic ${TELEMETRY_TOPIC}`);
        resp.sendStatus(500);
      });

      return Promise.resolve();
    });
