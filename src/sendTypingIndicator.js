"use strict";

const utils = require("../utils");
const log = require("npmlog");

module.exports = function (defaultFuncs, api, ctx) {
	function makeTypingIndicator(typ, threadID, callback, isGroup) {
		const form = {
			typ: +typ,
			to: "",
			source: "mercury-chat",
			thread: threadID
		};

		// Check if thread is a single person chat or a group chat
		// More info on this is in api.sendMessage
		if (utils.getType(isGroup) == "Boolean") {
			if (!isGroup) {
				form.to = threadID;
			}
			defaultFuncs
				.post("https://www.facebook.com/ajax/messaging/typ.php", ctx.jar, form)
				.then(utils.parseAndCheckLogin(ctx, defaultFuncs))
				.then(function (resData) {
					if (resData.error) {
						throw resData;
					}

					return callback();
				})
				.catch(function (err) {
					log.error("sendTypingIndicator", err);
					if (utils.getType(err) == "Object" && err.error === "Not logged in") {
						ctx.loggedIn = false;
					}
					return callback(err);
				});
		} else {
			api.getUserInfo(threadID, function (err, res) {
				if (err) {
					return callback(err);
				}

				// If id is single person chat
				if (Object.keys(res).length > 0) {
					form.to = threadID;
				}

				defaultFuncs
					.post("https://www.facebook.com/ajax/messaging/typ.php", ctx.jar, form)
					.then(utils.parseAndCheckLogin(ctx, defaultFuncs))
					.then(function (resData) {
						if (resData.error) {
							throw resData;
						}

						return callback();
					})
					.catch(function (err) {
						log.error("sendTypingIndicator", err);
						if (utils.getType(err) == "Object" && err.error === "Not logged in.") {
							ctx.loggedIn = false;
						}
						return callback(err);
					});
			});
		}
	}

	return function sendTypingIndicator(threadID, callback, isGroup) {
		if (
			utils.getType(callback) !== "Function" &&
			utils.getType(callback) !== "AsyncFunction"
		) {
			if (callback) {
				log.warn(
					"sendTypingIndicator",
					"callback is not a function - ignoring."
				);
			}
			callback = () => { };
		}

		if (ctx.mqttClient && utils.getType(isGroup) === "Boolean") {
			const publishStatus = (typing, done) => {
				ctx.wsReqNumber = (ctx.wsReqNumber || 0) + 1;
				const payload = {
					app_id: "772021112871879",
					payload: JSON.stringify({
						label: "3",
						payload: JSON.stringify({
							thread_key: String(threadID),
							is_group_thread: isGroup ? 1 : 0,
							is_typing: typing ? 1 : 0,
							attribution: 0,
							sync_group: 1,
							thread_type: isGroup ? 2 : 1
						}),
						version: "8965252033599983"
					}),
					request_id: ctx.wsReqNumber,
					type: 4
				};
				try {
					ctx.mqttClient.publish("/ls_req", JSON.stringify(payload), { qos: 1, retain: false }, done);
				}
				catch (err) {
					done(err);
				}
			};
			publishStatus(true, function (err) {
				callback(err || null);
			});
			return function endTyping(nextCallback) {
				publishStatus(false, nextCallback || function () { });
			};
		}

		makeTypingIndicator(true, threadID, callback, isGroup);

		return function end(cb) {
			if (
				utils.getType(cb) !== "Function" &&
				utils.getType(cb) !== "AsyncFunction"
			) {
				if (cb) {
					log.warn(
						"sendTypingIndicator",
						"callback is not a function - ignoring."
					);
				}
				cb = () => { };
			}

			makeTypingIndicator(false, threadID, cb, isGroup);
		};
	};
};
