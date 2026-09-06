"use strict";

const utils = require("../utils");
const { MQTT } = require("./protocol");
const { nextTaskID, publishRequest } = require("./mqttRequest");

function extractEditResult(payload) {
	try {
		const entry = payload.step[1][2][2][1];
		if (Array.isArray(entry) && entry.length > 4) {
			return { messageID: String(entry[2]), body: String(entry[4]) };
		}
	}
	catch (_) { }
	return null;
}

module.exports = function (defaultFuncs, api, ctx) {
	return function editMessage(text, messageID, callback) {
		if (typeof text !== "string" || messageID == null) {
			const error = new Error("text and messageID are required.");
			if (typeof callback === "function") callback(error);
			return Promise.reject(error);
		}

		const request = {
			app_id: MQTT.lightspeedAppId,
			payload: JSON.stringify({
				data_trace_id: null,
				epoch_id: Number(utils.generateOfflineThreadingID()),
				tasks: [{
					failure_count: null,
					label: "742",
					payload: JSON.stringify({ message_id: messageID, text }),
					queue_name: "edit_message",
					task_id: nextTaskID(ctx)
				}],
				version_id: MQTT.editMessageVersion
			}),
			type: 3
		};

		const promise = publishRequest(ctx, request).then(ack => {
			const result = extractEditResult(ack.response);
			if (!result) {
				throw { error: "Facebook returned an unexpected edit response.", response: ack.response };
			}
			if (result.body !== text) {
				throw { error: "The message is too old or not from you!", response: result };
			}
			return result;
		});

		if (typeof callback === "function") {
			promise.then(value => callback(null, value), error => callback(error));
		}
		return promise;
	};
};
