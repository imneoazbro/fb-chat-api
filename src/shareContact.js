"use strict";

const utils = require("../utils");
const { MQTT } = require("./protocol");
const { nextTaskID, publishRequest } = require("./mqttRequest");

module.exports = function (defaultFuncs, api, ctx) {
	return function shareContact(text, senderID, threadID, callback) {
		if (typeof text !== "string") text = "";
		if (senderID == null || threadID == null) {
			const error = new Error("senderID and threadID are required.");
			if (typeof callback === "function") callback(error);
			return Promise.reject(error);
		}

		const request = {
			app_id: MQTT.lightspeedAppId,
			payload: JSON.stringify({
				tasks: [{
					label: "359",
					payload: JSON.stringify({
						contact_id: senderID,
						sync_group: 1,
						text,
						thread_id: threadID
					}),
					queue_name: "messenger_contact_sharing",
					task_id: nextTaskID(ctx),
					failure_count: null
				}],
				epoch_id: utils.generateOfflineThreadingID(),
				version_id: MQTT.contactShareVersion
			}),
			type: 3
		};

		const promise = publishRequest(ctx, request);
		if (typeof callback === "function") {
			promise.then(value => callback(null, value), error => callback(error));
		}
		return promise;
	};
};
