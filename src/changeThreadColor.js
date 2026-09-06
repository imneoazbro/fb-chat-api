"use strict";

const utils = require("../utils");
const log = require("npmlog");
const { GRAPHQL_DOCS, MQTT } = require("./protocol");
const { publishRequest, nextTaskID } = require("./mqttRequest");

module.exports = function (defaultFuncs, api, ctx) {
	return function changeThreadColor(color, threadID, callback) {
		if (ctx.mqttClient) {
			return publishRequest(ctx, {
				app_id: MQTT.lightspeedAppId,
				payload: JSON.stringify({
					data_trace_id: null,
					epoch_id: utils.generateOfflineThreadingID(),
					tasks: [{
						failure_count: null,
						label: "43",
						payload: JSON.stringify({ thread_key: threadID, theme_fbid: String(color).toLowerCase(), source: null, sync_group: 1, payload: null }),
						queue_name: "thread_theme",
						task_id: nextTaskID(ctx)
					}],
					version_id: MQTT.threadMutationVersion
				}),
				type: 3
			}, callback);
		}
		let resolveFunc = function () { };
		let rejectFunc = function () { };
		const returnPromise = new Promise(function (resolve, reject) {
			resolveFunc = resolve;
			rejectFunc = reject;
		});

		if (!callback) {
			callback = function (err) {
				if (err) {
					return rejectFunc(err);
				}
				resolveFunc(err);
			};
		}

		if (!isNaN(color)) {
			color = color.toString();
		}
		const validatedColor = color !== null ? color.toLowerCase() : color; // API only accepts lowercase letters in hex string

		const form = {
			dpr: 1,
			queries: JSON.stringify({
				o0: {
					doc_id: GRAPHQL_DOCS.changeThreadColor,
					query_params: {
						data: {
							actor_id: ctx.i_userID || ctx.userID,
							client_mutation_id: "0",
							source: "SETTINGS",
							theme_id: validatedColor,
							thread_id: threadID
						}
					}
				}
			})
		};

		defaultFuncs
			.post("https://www.facebook.com/api/graphqlbatch/", ctx.jar, form)
			.then(utils.parseAndCheckLogin(ctx, defaultFuncs))
			.then(function (resData) {
				if (resData[resData.length - 1].error_results > 0) {
					throw new utils.CustomError(resData[0].o0.errors);
				}

				return callback();
			})
			.catch(function (err) {
				log.error("changeThreadColor", err);
				return callback(err);
			});

		return returnPromise;
	};
};
