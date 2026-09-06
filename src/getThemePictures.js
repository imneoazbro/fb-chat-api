"use strict";

const log = require("npmlog");
const utils = require("../utils");
const { GRAPHQL_DOCS } = require("./protocol");

module.exports = function (defaultFuncs, api, ctx) {
	return function getThemePictures(themeID, callback) {
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

		if (typeof themeID !== "string" || !themeID.trim()) {
			done({ error: "A theme ID is required." });
			return returnPromise;
		}

		const form = {
			av: ctx.globalOptions.pageID || ctx.userID,
			fb_api_caller_class: "RelayModern",
			fb_api_req_friendly_name: "MWPThreadThemeProviderQuery",
			doc_id: GRAPHQL_DOCS.themePictures,
			server_timestamps: true,
			variables: JSON.stringify({ id: themeID.trim() })
		};

		defaultFuncs
			.post("https://www.facebook.com/api/graphql/", ctx.jar, form)
			.then(utils.parseAndCheckLogin(ctx, defaultFuncs))
			.then(resData => {
				if (resData.errors) throw resData;
				done(null, resData);
			})
			.catch(error => {
				log.error("getThemePictures", error);
				done(error);
			});

		return returnPromise;
	};
};
