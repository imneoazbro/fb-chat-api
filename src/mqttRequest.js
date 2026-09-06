"use strict";

function nextRequestID(ctx) {
	ctx.wsReqNumber = (ctx.wsReqNumber || 0) + 1;
	return ctx.wsReqNumber;
}

function nextTaskID(ctx) {
	ctx.wsTaskNumber = (ctx.wsTaskNumber || 0) + 1;
	return ctx.wsTaskNumber;
}

function publishRequest(ctx, content, callback, timeoutMs = 15000) {
	const userCallback = typeof callback === "function" ? callback : null;
	const client = ctx.mqttClient;
	if (!client || typeof client.on !== "function" || typeof client.publish !== "function") {
		const error = new Error("Not connected to MQTT");
		if (userCallback) userCallback(error);
		return Promise.reject(error);
	}

	const requestID = content.request_id || nextRequestID(ctx);
	content.request_id = requestID;

	return new Promise((resolve, reject) => {
		let settled = false;
		let timer;
		const cleanup = () => {
			if (timer) clearTimeout(timer);
			client.removeListener("message", onMessage);
		};
		const finish = (error, value) => {
			if (settled) return;
			settled = true;
			cleanup();
			if (userCallback) userCallback(error, value);
			if (error) reject(error);
			else resolve(value);
		};
		const onMessage = (topic, rawMessage) => {
			if (topic !== "/ls_resp") return;
			let response;
			try {
				response = JSON.parse(rawMessage.toString());
				if (typeof response.payload === "string") response.payload = JSON.parse(response.payload);
			}
			catch (_) {
				return;
			}
			if (String(response.request_id) !== String(requestID)) return;
			finish(null, { success: true, response: response.payload, raw: response });
		};

		client.on("message", onMessage);
		timer = setTimeout(() => finish({ error: "Timeout waiting for ACK" }), timeoutMs);
		try {
			client.publish("/ls_req", JSON.stringify(content), { qos: 1, retain: false }, error => {
				if (error) finish(error);
			});
		}
		catch (error) {
			finish(error);
		}
	});
}

module.exports = { nextRequestID, nextTaskID, publishRequest };
