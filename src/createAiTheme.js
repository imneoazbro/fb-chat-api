"use strict";

const log = require("npmlog");
const utils = require("../utils");
const { GRAPHQL_DOCS } = require("./protocol");

module.exports = function (defaultFuncs, api, ctx) {
	return function createAiTheme(prompt, callback) {
		let resolveFunc;
		let rejectFunc;
		const returnPromise = new Promise((resolve, reject) => {
			resolveFunc = resolve;
			rejectFunc = reject;
		});
		const userCallback = typeof callback === "function" ? callback : null;
		const done = (error, value) => {
			if (userCallback) userCallback(error, value);
			if (error) rejectFunc(error);
			else resolveFunc(value);
		};

		if (typeof prompt !== "string") {
			done({ error: "Invalid prompt. Please provide a string." });
			return returnPromise;
		}
		prompt = prompt.trim();
		if (!prompt) {
			done({ error: "Prompt cannot be empty." });
			return returnPromise;
		}

		const form = {
			av: ctx.globalOptions.pageID || ctx.userID,
			fb_api_caller_class: "RelayModern",
			fb_api_req_friendly_name: "useGenerateAIThemeMutation",
			doc_id: GRAPHQL_DOCS.aiTheme,
			variables: JSON.stringify({
				input: {
					client_mutation_id: Math.round(Math.random() * 19).toString(),
					actor_id: ctx.globalOptions.pageID || ctx.userID,
					bypass_cache: true,
					caller: "MESSENGER",
					num_themes: 1,
					prompt
				}
			}),
			server_timestamps: true
		};

		defaultFuncs
			.post("https://www.facebook.com/api/graphql/", ctx.jar, form)
			.then(utils.parseAndCheckLogin(ctx, defaultFuncs))
			.then(resData => {
				if (resData.errors) throw resData;
				const themes = resData.data && resData.data.xfb_generate_ai_themes_from_prompt &&
					resData.data.xfb_generate_ai_themes_from_prompt.themes;
				if (!Array.isArray(themes) || themes.length === 0) {
					throw { error: "No themes generated", res: resData };
				}
				const theme = themes[0];
				if (!theme || !theme.id || !theme.background_asset) {
					throw { error: "Invalid theme data", res: resData };
				}
				done(null, {
					id: theme.id,
					accessibility_label: theme.accessibility_label || null,
					background_asset: {
						id: theme.background_asset.id,
						image: { url: theme.background_asset.image && theme.background_asset.image.uri || null }
					}
				});
			})
			.catch(error => {
				log.error("createAiTheme", error);
				done(error);
			});

		return returnPromise;
	};
};
