var utils = require("../utils");
var log = require("npmlog");
var { MQTT } = require("./protocol");

module.exports = function (defaultFuncs, api, ctx) {
	function extractResponseIDs(payload) {
		let messageID = null;
		let threadID = null;
		function walk(value) {
			if (!Array.isArray(value)) return;
			if (value[0] === 5 && (value[1] === "replaceOptimsiticMessage" || value[1] === "replaceOptimisticMessage")) {
				messageID = value[3] == null ? messageID : String(value[3]);
			}
			if (value[0] === 5 && value[1] === "writeCTAIdToThreadsTable" && Array.isArray(value[2]) && value[2][0] === 19) {
				threadID = value[2][1] == null ? threadID : String(value[2][1]);
			}
			value.forEach(walk);
		}
		walk(payload && payload.step);
		return { messageID, threadID };
	}

	function uploadAttachment(attachments, callback) {
		callback = callback || function () { };
		var uploads = [];

		// create an array of promises
		for (var i = 0; i < attachments.length; i++) {
			if (!utils.isReadableStream(attachments[i])) {
				throw {
					error:
						"Attachment should be a readable stream and not " +
						utils.getType(attachments[i]) +
						"."
				};
			}

			var form = {
				upload_1024: attachments[i],
				voice_clip: "true"
			};

			uploads.push(
				defaultFuncs
					.postFormData(
						"https://upload.facebook.com/ajax/mercury/upload.php",
						ctx.jar,
						form,
						{}
					)
					.then(utils.parseAndCheckLogin(ctx, defaultFuncs))
					.then(function (resData) {
						if (resData.error) {
							throw resData;
						}

						// We have to return the data unformatted unless we want to change it
						// back in sendMessage.
						return resData.payload.metadata[0];
					})
			);
		}

		// resolve all promises
		Promise
			.all(uploads)
			.then(function (resData) {
				callback(null, resData);
			})
			.catch(function (err) {
				log.error("uploadAttachment", err);
				return callback(err);
			});
	}

	const emojiSizes = {
		small: 1,
		medium: 2,
		large: 3
	};

	function handleEmoji(msg, form, callback, cb) {
		if (msg.emojiSize != null && msg.emoji == null) {
			return callback({ error: "emoji property is empty" });
		}
		if (msg.emoji) {
			if (!msg.emojiSize) {
				msg.emojiSize = "small";
			}
			if (
				msg.emojiSize !== "small" &&
				msg.emojiSize !== "medium" &&
				msg.emojiSize !== "large" &&
				(isNaN(msg.emojiSize) || msg.emojiSize < 1 || msg.emojiSize > 3)
			) {
				return callback({ error: "emojiSize property is invalid" });
			}

			form.payload.tasks[0].payload.send_type = 1;
			form.payload.tasks[0].payload.text = msg.emoji;
			form.payload.tasks[0].payload.hot_emoji_size = !isNaN(msg.emojiSize) ? msg.emojiSize : emojiSizes[msg.emojiSize];
		}
		cb();
	}

	function handleSticker(msg, form, callback, cb) {
		if (msg.sticker) {
			form.payload.tasks[0].payload.send_type = 2;
			form.payload.tasks[0].payload.sticker_id = msg.sticker;
		}
		cb();
	}

	function handleAttachment(msg, form, callback, cb) {
		if (msg.attachment) {
			form.payload.tasks[0].payload.send_type = 3;
			form.payload.tasks[0].payload.attachment_fbids = [];
			if (form.payload.tasks[0].payload.text == "")
				form.payload.tasks[0].payload.text = null;
			if (utils.getType(msg.attachment) !== "Array") {
				msg.attachment = [msg.attachment];
			}

			uploadAttachment(msg.attachment, function (err, files) {
				if (err) {
					return callback(err);
				}

				files.forEach(function (file) {
					var key = Object.keys(file);
					var type = key[0]; // image_id, file_id, etc
					form.payload.tasks[0].payload.attachment_fbids.push(file[type]); // push the id
				});
				cb();
			});
		} else {
			cb();
		}
	}


	function handleMention(msg, form, callback, cb) {
		if (msg.mentions) {
			form.payload.tasks[0].payload.send_type = 1;

			const arrayIds = [];
			const arrayOffsets = [];
			const arrayLengths = [];
			const mention_types = [];

			for (let i = 0; i < msg.mentions.length; i++) {
				const mention = msg.mentions[i];

				const tag = mention.tag;
				if (typeof tag !== "string") {
					return callback({ error: "Mention tags must be strings." });
				}

				const offset = String(msg.body || "").indexOf(tag, mention.fromIndex || 0);

				if (offset < 0) {
					log.warn(
						"handleMention",
						'Mention for "' + tag + '" not found in message string.'
					);
				}

				if (mention.id == null) {
					log.warn("handleMention", "Mention id should be non-null.");
				}

				const id = mention.id || 0;
				arrayIds.push(id);
				arrayOffsets.push(offset);
				arrayLengths.push(tag.length);
				mention_types.push("p");
			}

			form.payload.tasks[0].payload.mention_data = {
				mention_ids: arrayIds.join(","),
				mention_offsets: arrayOffsets.join(","),
				mention_lengths: arrayLengths.join(","),
				mention_types: mention_types.join(",")
			};
		}
		cb();
	}

	function handleLocation(msg, form, callback, cb) {
		// this is not working yet
		if (msg.location) {
			if (msg.location.latitude == null || msg.location.longitude == null) {
				return callback({ error: "location property needs both latitude and longitude" });
			}

			form.payload.tasks[0].payload.send_type = 1;
			form.payload.tasks[0].payload.location_data = {
				coordinates: {
					latitude: msg.location.latitude,
					longitude: msg.location.longitude
				},
				is_current_location: !!msg.location.current,
				is_live_location: !!msg.location.live
			};
		}

		cb();
	}

	function send(form, threadID, callback, replyToMessage, msg) {
		if (replyToMessage) {
			form.payload.tasks[0].payload.reply_metadata = {
				reply_source_id: replyToMessage,
				reply_source_type: 1,
				reply_type: 0
			};
		}
		const mqttClient = ctx.mqttClient;
		form.payload.tasks.forEach((task) => {
			task.payload = JSON.stringify(task.payload);
		});
		form.payload = JSON.stringify(form.payload);
		if (!mqttClient || typeof mqttClient.on !== "function" || typeof mqttClient.publish !== "function") {
			return callback(new Error("MQTT client is not initialized"));
		}

		const requestID = (ctx.mqttRequestID = (ctx.mqttRequestID || 0) + 1);
		form.request_id = requestID;
		let settled = false;
		const cleanup = function () {
			if (settled) return;
			settled = true;
			mqttClient.removeListener("message", onMessage);
			clearTimeout(timeout);
		};
		const onMessage = function (topic, message) {
			if (topic !== "/ls_resp") return;
			let response;
			try {
				response = JSON.parse(message.toString());
				if (typeof response.payload === "string") response.payload = JSON.parse(response.payload);
			}
			catch (_) {
				return;
			}
			if (String(response.request_id) !== String(requestID)) return;
			cleanup();
			const ids = extractResponseIDs(response.payload);
			callback(null, {
				body: msg.body == null ? null : String(msg.body),
				messageID: ids.messageID,
				threadID: ids.threadID || String(threadID),
				response: response.payload
			});
		};
		const timeout = setTimeout(function () {
			cleanup();
			callback({ error: "Timeout waiting for ACK" });
		}, 15000);

		mqttClient.on("message", onMessage);
		try {
			return mqttClient.publish("/ls_req", JSON.stringify(form), { qos: 1, retain: false }, function (err) {
				if (err) {
					cleanup();
					callback(err);
				}
			});
		}
		catch (err) {
			cleanup();
			callback(err);
		}
	}

	return function sendMessageMqtt(msg, threadID, callback, replyToMessage) {
		if (
			!callback &&
			(utils.getType(threadID) === "Function" ||
				utils.getType(threadID) === "AsyncFunction")
		) {
			return threadID({ error: "Pass a threadID as a second argument." });
		}
		if (
			!replyToMessage &&
			utils.getType(callback) === "String"
		) {
			replyToMessage = callback;
			callback = undefined;
		}

		const userCallback = typeof callback === "function" ? callback : null;
		let resolveFunc;
		let rejectFunc;
		const returnPromise = new Promise((resolve, reject) => {
			resolveFunc = resolve;
			rejectFunc = reject;
		});
		callback = function (err, data) {
			if (userCallback) userCallback(err, data);
			if (err) rejectFunc(err);
			else resolveFunc(data);
		};
		if (threadID == null) {
			callback({ error: "threadID is required" });
			return returnPromise;
		}

		var msgType = utils.getType(msg);
		var threadIDType = utils.getType(threadID);
		var messageIDType = utils.getType(replyToMessage);

		if (msgType !== "String" && msgType !== "Object") {
			callback({
				error:
					"Message should be of type string or object and not " + msgType + "."
			});
			return returnPromise;
		}

		if (msgType === "String") {
			msg = { body: msg };
		}

		const timestamp = Date.now();
		// get full date time
		const epoch = (BigInt(timestamp) << 22n).toString();
		const otid = utils.generateOfflineThreadingID();

		const form = {
			app_id: MQTT.lightspeedAppId,
			payload: {
				tasks: [
					{
						label: "46",
						payload: {
							thread_id: threadID.toString(),
							otid: otid.toString(),
							source: 2097153,
							send_type: 1,
							sync_group: 1,
							mark_thread_read: 1,
							text: msg.body != null && msg.body != undefined && msg.body.toString() !== "" ? msg.body.toString() : null,
							initiating_source: 0,
							skip_url_preview_gen: 0
						},
						queue_name: threadID.toString(),
						task_id: 400,
						failure_count: null
					},
					{
						label: "21",
						payload: {
							thread_id: threadID.toString(),
							last_read_watermark_ts: Date.now(),
							sync_group: 1
						},
						queue_name: threadID.toString(),
						task_id: 401,
						failure_count: null
					}
				],
				epoch_id: epoch,
				version_id: MQTT.messageVersion,
				data_trace_id: "#" + Buffer.from(String(Math.random())).toString("base64").replace(/=+$/g, "")
			},
			request_id: 1,
			type: 3
		};

		handleEmoji(msg, form, callback, function () {
			handleLocation(msg, form, callback, function () {
				handleMention(msg, form, callback, function () {
					handleSticker(msg, form, callback, function () {
						handleAttachment(msg, form, callback, function () {
							send(form, threadID, callback, replyToMessage, msg);
						});
					});
				});
			});
		});
		return returnPromise;
	};
};
